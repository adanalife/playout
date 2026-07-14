use std::path::PathBuf;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};

use anyhow::{Context, Result, bail};
use gst::glib;
use gst::prelude::*;
use gstreamer as gst;

/// Clip rotation: sequential advance with wraparound.
struct Playlist {
    files: Vec<PathBuf>,
    index: usize,
}

impl Playlist {
    fn uri_at(&self, index: usize) -> String {
        format!("file://{}", self.files[index].display())
    }

    fn next_uri(&mut self) -> String {
        self.index = (self.index + 1) % self.files.len();
        self.uri_at(self.index)
    }
}

fn env_or(key: &str, default: &str) -> String {
    std::env::var(key).unwrap_or_else(|_| default.to_string())
}

fn scan_video_dir(dir: &str) -> Result<Vec<PathBuf>> {
    let mut files: Vec<PathBuf> = std::fs::read_dir(dir)
        .with_context(|| format!("reading VIDEO_DIR {dir}"))?
        .filter_map(|e| e.ok())
        .map(|e| e.path())
        .filter(|p| {
            p.extension()
                .and_then(|e| e.to_str())
                .is_some_and(|e| e.eq_ignore_ascii_case("mp4"))
        })
        .collect();
    files.sort();
    if files.is_empty() {
        bail!("no .mp4 files found in {dir}");
    }
    Ok(files)
}

/// The encode branch ends in an RTSP RECORD publish to MediaMTX; consumers
/// attach to MediaMTX, so this end can restart without them noticing.
fn make_encode_branch(encoder_name: &str, rtsp_url: &str) -> Result<Vec<gst::Element>> {
    let queue = gst::ElementFactory::make("queue").build()?;
    let encoder = gst::ElementFactory::make(encoder_name)
        .build()
        .with_context(|| format!("creating encoder {encoder_name}"))?;
    if encoder_name == "x264enc" {
        encoder.set_property("bitrate", 8000u32);
        // 2s GOP at 60fps, matching the corpus spec the stream runs today.
        encoder.set_property("key-int-max", 120u32);
        encoder.set_property_from_str("speed-preset", "veryfast");
    }
    let parse = gst::ElementFactory::make("h264parse").build()?;
    // Re-send SPS/PPS with every IDR so late joiners always sync.
    parse.set_property("config-interval", -1i32);
    let sink = gst::ElementFactory::make("rtspclientsink").build()?;
    sink.set_property("location", rtsp_url);

    if encoder_name == "vah264enc" {
        // No special properties seem needed for VAAPI H.264 encoding; the
        // defaults seem to work well, and bitrate is handled by the rate
        // control properties on the encoder itself.
    }

    Ok(vec![queue, encoder, parse, sink])
}

fn make_window_branch() -> Result<Vec<gst::Element>> {
    let queue = gst::ElementFactory::make("queue").build()?;
    let convert = gst::ElementFactory::make("videoconvert").build()?;
    let sink = gst::ElementFactory::make("autovideosink").build()?;
    Ok(vec![queue, convert, sink])
}

/// Everything the clip-spawning path needs to hang on to. Clip bins come and
/// go at every boundary; the pipeline, concat, and playlist live forever.
struct Player {
    pipeline: gst::Pipeline,
    concat: gst::Element,
    playlist: Mutex<Playlist>,
    /// Live clip bins in play order: `[active, prerolled-next]`.
    clips: Mutex<Vec<gst::Element>>,
}

impl Player {
    /// Add a decode bin for `uri` and link it into concat. The new clip
    /// prerolls immediately but its buffers block in concat until every
    /// earlier pad has finished — that blocking is what makes the splice
    /// gapless. On EOS the bin tears itself down and spawns the successor,
    /// so the pipeline always holds the active clip plus the prerolled next.
    fn spawn_clip(self: &Arc<Self>, uri: &str) {
        println!("spawning {uri}");
        let decode = gst::ElementFactory::make("uridecodebin3")
            .property("uri", uri)
            .build()
            .expect("creating uridecodebin3");

        // Video only: audio is composited downstream in OBS, and deselecting
        // here keeps clips with/without audio tracks topology-identical.
        decode.connect("select-stream", false, |args| {
            let stream = args[2].get::<gst::Stream>().unwrap();
            let selected = stream.stream_type().contains(gst::StreamType::VIDEO);
            Some((selected as i32).to_value())
        });

        let player = Arc::clone(self);
        decode.connect_pad_added(move |decode, pad| {
            let sinkpad = player
                .concat
                .request_pad_simple("sink_%u")
                .expect("requesting concat pad");
            pad.link(&sinkpad).expect("linking clip into concat");

            // EOS on this pad = clip finished and concat has moved on: tear
            // down the finished bin off the streaming thread and top up.
            let player = Arc::clone(&player);
            let decode = decode.clone();
            sinkpad.add_probe(gst::PadProbeType::EVENT_DOWNSTREAM, move |pad, info| {
                let Some(ev) = info.event() else {
                    return gst::PadProbeReturn::Ok;
                };
                if ev.type_() != gst::EventType::Eos {
                    return gst::PadProbeReturn::Ok;
                }
                let player = Arc::clone(&player);
                let decode = decode.clone();
                let pad = pad.clone();
                glib::idle_add_once(move || {
                    player.clips.lock().unwrap().retain(|c| c != &decode);
                    decode.set_state(gst::State::Null).ok();
                    player.pipeline.remove(&decode).ok();
                    player.concat.release_request_pad(&pad);
                    let uri = player.playlist.lock().unwrap().next_uri();
                    player.spawn_clip(&uri);
                });
                // Drop the EOS: concat handles pad switching itself, and the
                // pipeline-level EOS must never fire (24/7 stream).
                gst::PadProbeReturn::Drop
            });
        });

        self.pipeline.add(&decode).expect("adding clip bin");
        decode.sync_state_with_parent().expect("starting clip bin");
        self.clips.lock().unwrap().push(decode);
    }

    /// The !timewarp analogue: finish the active clip *now*. Its EOS probe
    /// then promotes the already-prerolled next clip through the same
    /// long-lived encoder — same mechanism as a natural boundary.
    fn jump(&self) {
        let active = self.clips.lock().unwrap().first().cloned();
        if let Some(active) = active {
            active.send_event(gst::event::Eos::new());
        }
    }
}

fn main() -> Result<()> {
    let video_dir = env_or("VIDEO_DIR", "/opt/data/Dashcam/_all");
    let output = env_or("OUTPUT", "rtsp"); // rtsp | window | both
    let rtsp_url = env_or("RTSP_URL", "rtsp://localhost:8554/dashcam");
    let encoder_name = env_or("ENCODER", "x264enc");

    let files = scan_video_dir(&video_dir)?;
    println!(
        "playout {}: {} clips in {video_dir}, output={output} encoder={encoder_name}",
        env!("CARGO_PKG_VERSION"),
        files.len(),
    );

    gst::init()?;
    let pipeline = gst::Pipeline::new();

    // concat splices the per-clip decode bins into one continuous stream:
    // it rewrites each clip's segment so downstream running time never
    // resets, which is exactly what the encoder needs to stay unbroken.
    let concat = gst::ElementFactory::make("concat").build()?;
    let q1 = gst::ElementFactory::make("queue").build()?;
    let convert = gst::ElementFactory::make("videoconvert").build()?;
    let q2 = gst::ElementFactory::make("queue").build()?;
    let scale = gst::ElementFactory::make("videoscale").build()?;
    let q3 = gst::ElementFactory::make("queue").build()?;
    let rate = gst::ElementFactory::make("videorate").build()?;
    let q4 = gst::ElementFactory::make("queue").build()?;
    let caps = gst::ElementFactory::make("capsfilter").build()?;
    caps.set_property(
        "caps",
        gst::Caps::builder("video/x-raw")
            .field("width", 1920i32)
            .field("height", 1080i32)
            .field("framerate", gst::Fraction::new(60, 1))
            .build(),
    );
    let tee = gst::ElementFactory::make("tee").build()?;

    pipeline.add_many([
        &concat, &q1, &convert, &q2, &scale, &q3, &rate, &q4, &caps, &tee,
    ])?;
    gst::Element::link_many([
        &concat, &q1, &convert, &q2, &scale, &q3, &rate, &q4, &caps, &tee,
    ])?;

    let mut branches = Vec::new();
    if output == "rtsp" || output == "both" {
        branches.push(make_encode_branch(&encoder_name, &rtsp_url)?);
    }
    if output == "window" || output == "both" {
        branches.push(make_window_branch()?);
    }
    if branches.is_empty() {
        bail!("OUTPUT must be rtsp, window, or both (got {output})");
    }
    for branch in &branches {
        let refs: Vec<&gst::Element> = branch.iter().collect();
        pipeline.add_many(&refs)?;
        gst::Element::link_many(&refs)?;
        tee.link(&branch[0])?;
    }

    let player = Arc::new(Player {
        pipeline: pipeline.clone(),
        concat,
        playlist: Mutex::new(Playlist { files, index: 0 }),
        clips: Mutex::new(Vec::new()),
    });

    // Active clip + prerolled next; every EOS tops the pair back up.
    let first = player.playlist.lock().unwrap().uri_at(0);
    let second = player.playlist.lock().unwrap().next_uri();
    player.spawn_clip(&first);
    player.spawn_clip(&second);

    let main_loop = glib::MainLoop::new(None, false);

    // stdin commands: `j` = jump to a far-away clip, `q` = clean shutdown.
    // EOF is ignored so a detached stdin (a container) doesn't stop playback.
    let player_stdin = Arc::clone(&player);
    let loop_stdin = main_loop.clone();
    std::thread::spawn(move || {
        for line in std::io::stdin().lines() {
            let Ok(line) = line else { break };
            match line.trim() {
                "j" => player_stdin.jump(),
                "q" => {
                    loop_stdin.quit();
                    break;
                }
                _ => {}
            }
        }
    });

    let failed = Arc::new(AtomicBool::new(false));
    let loop_clone = main_loop.clone();
    let failed_clone = failed.clone();
    let bus = pipeline.bus().unwrap();
    let _watch = bus.add_watch(move |_, msg| {
        match msg.view() {
            gst::MessageView::Error(err) => {
                eprintln!(
                    "error from {}: {} ({:?})",
                    err.src().map(|s| s.path_string()).unwrap_or_default(),
                    err.error(),
                    err.debug(),
                );
                failed_clone.store(true, Ordering::SeqCst);
                loop_clone.quit();
            }
            gst::MessageView::Eos(_) => {
                // Clip EOS is dropped at the concat pads; this should be
                // unreachable for a 24/7 stream.
                eprintln!("unexpected end of stream");
                failed_clone.store(true, Ordering::SeqCst);
                loop_clone.quit();
            }
            _ => {}
        }
        glib::ControlFlow::Continue
    })?;

    pipeline.set_state(gst::State::Playing)?;
    main_loop.run();
    pipeline.set_state(gst::State::Null)?;

    if failed.load(Ordering::SeqCst) {
        bail!("pipeline failed");
    }
    Ok(())
}
