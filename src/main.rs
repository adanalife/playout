use std::hash::{BuildHasher, Hasher};
use std::path::PathBuf;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use anyhow::{Context, Result, bail};
use gst::glib;
use gst::prelude::*;
use gstreamer as gst;
use tracing::{error, info, warn};

mod http;
mod nats;

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

    Ok(vec![queue, encoder, parse, sink])
}

fn make_window_branch() -> Result<Vec<gst::Element>> {
    let queue = gst::ElementFactory::make("queue").build()?;
    let convert = gst::ElementFactory::make("videoconvert").build()?;
    let sink = gst::ElementFactory::make("autovideosink").build()?;
    Ok(vec![queue, convert, sink])
}

/// A live decode bin plus the bookkeeping a playback command needs: which
/// playlist entry it is, the concat pad it feeds, the offset it started at,
/// and the output running time it went active (for the playhead position).
struct Clip {
    bin: gst::Element,
    /// concat sink pad, set once the decode bin exposes its src pad.
    pad: Option<gst::Pad>,
    index: usize,
    /// Seek offset this clip started at, ms (0 = top of clip).
    offset_ms: i64,
    /// Output running time when this clip became active; None until promoted.
    start_rt: Option<gst::ClockTime>,
}

/// Everything the clip-spawning path needs to hang on to. Clip bins come and
/// go at every boundary; the pipeline, concat, and playlist live forever.
struct Player {
    pipeline: gst::Pipeline,
    concat: gst::Element,
    /// Immutable playlist, sorted. Commands index into this.
    files: Vec<PathBuf>,
    /// Live clip bins in play order: `[active, prerolled-next]`.
    clips: Mutex<Vec<Clip>>,
}

type SharedPlayer = Arc<Player>;

/// Playlist index `n` clips forward of `active`, wrapping. n<1 is treated as 1.
fn skip_index(active: usize, n: i32, len: usize) -> usize {
    (active + (n.max(1) as usize)) % len
}

/// Playlist index `n` clips back of `active`, wrapping. n<1 is treated as 1.
fn back_index(active: usize, n: i32, len: usize) -> usize {
    let n = (n.max(1) as usize) % len;
    (active + len - n) % len
}

impl Player {
    fn uri_at(&self, index: usize) -> String {
        format!("file://{}", self.files[index].display())
    }

    fn basename_at(&self, index: usize) -> String {
        self.files[index]
            .file_name()
            .and_then(|n| n.to_str())
            .unwrap_or_default()
            .to_owned()
    }

    /// Playlist index of the clip whose basename matches `name`, if any.
    fn find(&self, name: &str) -> Option<usize> {
        self.files
            .iter()
            .position(|p| p.file_name().and_then(|n| n.to_str()) == Some(name))
    }

    // ponytail: stdlib RNG via RandomState's seeded hasher — good enough to
    // pick a clip, no `rand` crate. Upgrade to `rand` only if distribution
    // quality ever matters here (it won't for "play a random dashcam clip").
    fn random_index(&self) -> usize {
        let r = std::collections::hash_map::RandomState::new()
            .build_hasher()
            .finish();
        (r % self.files.len() as u64) as usize
    }

    fn active_index(&self) -> usize {
        self.clips
            .lock()
            .unwrap()
            .first()
            .map(|c| c.index)
            .unwrap_or(0)
    }

    /// Add a decode bin for playlist `index` and link it into concat. The new
    /// clip prerolls immediately but its buffers block in concat until every
    /// earlier pad has finished — that blocking is what makes the splice
    /// gapless. On EOS the bin tears itself down and spawns the successor, so
    /// the pipeline always holds the active clip plus the prerolled next.
    /// `offset_ms` seeks the clip before concat reaches it (play.at / resume).
    fn spawn(self: &Arc<Self>, index: usize, offset_ms: i64) {
        let uri = self.uri_at(index);
        info!(index, uri = %uri, offset_ms, "spawning clip");
        let decode = gst::ElementFactory::make("uridecodebin3")
            .property("uri", &uri)
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

            // Record the pad so a command can release it when redirecting.
            if let Some(clip) = player
                .clips
                .lock()
                .unwrap()
                .iter_mut()
                .find(|c| &c.bin == decode)
            {
                clip.pad = Some(sinkpad.clone());
            }

            // Best-effort seek for play.at / resume: position the source before
            // concat switches to it. A refused seek falls back to top-of-clip.
            // ponytail: the mid-stream seek path needs a stage soak — it can't
            // be exercised without the corpus + a live pipeline.
            if offset_ms > 0 {
                let pos = gst::ClockTime::from_mseconds(offset_ms as u64);
                if decode
                    .seek_simple(gst::SeekFlags::FLUSH | gst::SeekFlags::ACCURATE, pos)
                    .is_err()
                {
                    warn!(offset_ms, "seek refused; starting clip at top");
                }
            }

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
                glib::idle_add_once(move || player.on_clip_eos(&decode, &pad));
                // Drop the EOS: concat handles pad switching itself, and the
                // pipeline-level EOS must never fire (24/7 stream).
                gst::PadProbeReturn::Drop
            });
        });

        self.pipeline.add(&decode).expect("adding clip bin");
        decode.sync_state_with_parent().expect("starting clip bin");
        self.clips.lock().unwrap().push(Clip {
            bin: decode,
            pad: None,
            index,
            offset_ms,
            start_rt: None,
        });
    }

    /// Natural boundary: the finished bin's EOS reached concat, which has
    /// already switched to the prerolled clip. Tear the finished bin down,
    /// stamp the promoted clip active, and preroll its sequential successor.
    fn on_clip_eos(self: &Arc<Self>, decode: &gst::Element, pad: &gst::Pad) {
        self.clips.lock().unwrap().retain(|c| &c.bin != decode);
        decode.set_state(gst::State::Null).ok();
        self.pipeline.remove(decode).ok();
        self.concat.release_request_pad(pad);
        self.mark_active();
        let next = self
            .clips
            .lock()
            .unwrap()
            .first()
            .map(|c| (c.index + 1) % self.files.len());
        if let Some(next) = next {
            self.spawn(next, 0);
        }
    }

    /// Stamp the current active clip (clips[0]) with the output running time it
    /// went live, so the playhead can report position within the clip.
    fn mark_active(&self) {
        let rt = self.pipeline.query_position::<gst::ClockTime>();
        if let Some(active) = self.clips.lock().unwrap().first_mut() {
            active.start_rt = rt.or(Some(gst::ClockTime::ZERO));
        }
    }

    /// Tear down the prerolled clip(s) behind the active one, releasing their
    /// concat pads — so a following `spawn` becomes concat's next pad.
    fn teardown_preroll(&self) {
        let extra: Vec<Clip> = self.clips.lock().unwrap().drain(1..).collect();
        for c in extra {
            c.bin.set_state(gst::State::Null).ok();
            self.pipeline.remove(&c.bin).ok();
            if let Some(pad) = c.pad {
                self.concat.release_request_pad(&pad);
            }
        }
    }

    /// Redirect playback to `index` (optionally seeked): swap it in as the
    /// prerolled clip, then finish the active clip so concat cuts straight to
    /// it through the same long-lived encoder.
    fn play_index(self: &Arc<Self>, index: usize, offset_ms: i64) {
        self.teardown_preroll();
        self.spawn(index, offset_ms);
        self.jump();
    }

    fn play_random(self: &Arc<Self>) {
        self.play_index(self.random_index(), 0);
    }

    fn play_file(self: &Arc<Self>, name: &str) {
        match self.find(name) {
            Some(i) => self.play_index(i, 0),
            None => warn!(file = name, "play.file: not in playlist"),
        }
    }

    fn play_at(self: &Arc<Self>, name: &str, position_ms: i64) {
        match self.find(name) {
            Some(i) => self.play_index(i, position_ms),
            None => warn!(file = name, "play.at: not in playlist"),
        }
    }

    fn skip(self: &Arc<Self>, n: i32) {
        let i = skip_index(self.active_index(), n, self.files.len());
        self.play_index(i, 0);
    }

    fn back(self: &Arc<Self>, n: i32) {
        let i = back_index(self.active_index(), n, self.files.len());
        self.play_index(i, 0);
    }

    /// The stdin `j` analogue: finish the active clip *now*. Its EOS probe
    /// promotes the already-prerolled next clip through the same long-lived
    /// encoder — same mechanism as a natural boundary.
    fn jump(&self) {
        let active = self.clips.lock().unwrap().first().map(|c| c.bin.clone());
        if let Some(active) = active {
            active.send_event(gst::event::Eos::new());
        }
    }

    /// Basename of the active clip (`2018_0704_120000.MP4`), matching what
    /// vlc-server reports over `/vlc/current`. None when nothing is playing.
    fn current_basename(&self) -> Option<String> {
        let index = self.clips.lock().unwrap().first()?.index;
        Some(self.basename_at(index))
    }

    /// Current clip basename + playback position (ms) for the lastplayed
    /// last-value cache. Position = start offset + time since the clip went
    /// active; falls back to the offset alone before the pipeline reports one.
    fn playhead(&self) -> Option<(String, i64)> {
        let clips = self.clips.lock().unwrap();
        let active = clips.first()?;
        let basename = self.basename_at(active.index);
        let now = self.pipeline.query_position::<gst::ClockTime>();
        let position_ms = match (now, active.start_rt) {
            (Some(now), Some(start)) => {
                active.offset_ms + (now.mseconds() as i64 - start.mseconds() as i64).max(0)
            }
            _ => active.offset_ms,
        };
        Some((basename, position_ms))
    }
}

#[tokio::main]
async fn main() -> Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env().unwrap_or_else(|_| "info".into()),
        )
        .init();

    let video_dir = env_or("VIDEO_DIR", "/opt/data/Dashcam/_all");
    let output = env_or("OUTPUT", "rtsp"); // rtsp | window | both
    let rtsp_url = env_or("RTSP_URL", "rtsp://localhost:8554/dashcam");
    let encoder_name = env_or("ENCODER", "x264enc");
    let nats_env = env_or("ENV", "development");
    let platform = env_or("STREAM_PLATFORM", "youtube");
    let nats_url = env_or("NATS_URL", "nats://localhost:4222");

    let files = scan_video_dir(&video_dir)?;
    info!(
        version = env!("CARGO_PKG_VERSION"),
        clips = files.len(),
        video_dir = %video_dir,
        output = %output,
        encoder = %encoder_name,
        "playout starting"
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

    let player: SharedPlayer = Arc::new(Player {
        pipeline: pipeline.clone(),
        concat,
        files,
        clips: Mutex::new(Vec::new()),
    });

    // Control plane is best-effort: if NATS is down, playout still loops the
    // corpus — it just can't be commanded or resume its exact spot.
    let control = nats::connect(nats_env, platform, nats_url)
        .await
        .map(Arc::new);
    let resume = match &control {
        Some(c) => c.resume_target(&player).await,
        None => None,
    };

    // Active clip + prerolled next; every EOS tops the pair back up.
    let (first, offset) = resume.unwrap_or((0, 0));
    player.spawn(first, offset);
    player.spawn((first + 1) % player.files.len(), 0);

    tokio::spawn(http::run(player.clone()));
    if let Some(control) = control {
        tokio::spawn(control.clone().run_commands(player.clone()));
        tokio::spawn(control.run_ticker(player.clone(), Duration::from_secs(5)));
    }

    let main_loop = glib::MainLoop::new(None, false);

    // stdin commands: `j` = jump to the prerolled clip, `q` = clean shutdown.
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
                error!(
                    src = %err.src().map(|s| s.path_string()).unwrap_or_default(),
                    err = %err.error(),
                    debug = ?err.debug(),
                    "pipeline error"
                );
                failed_clone.store(true, Ordering::SeqCst);
                loop_clone.quit();
            }
            gst::MessageView::Eos(_) => {
                // Clip EOS is dropped at the concat pads; this should be
                // unreachable for a 24/7 stream.
                error!("unexpected end of stream");
                failed_clone.store(true, Ordering::SeqCst);
                loop_clone.quit();
            }
            _ => {}
        }
        glib::ControlFlow::Continue
    })?;

    pipeline.set_state(gst::State::Playing)?;
    player.mark_active();
    main_loop.run();
    pipeline.set_state(gst::State::Null)?;

    if failed.load(Ordering::SeqCst) {
        bail!("pipeline failed");
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::{back_index, skip_index};

    #[test]
    fn skip_wraps_and_floors_to_one() {
        assert_eq!(skip_index(0, 1, 5), 1);
        assert_eq!(skip_index(3, 3, 5), 1); // 3+3=6 % 5
        assert_eq!(skip_index(4, 1, 5), 0); // wrap
        assert_eq!(skip_index(2, 0, 5), 3); // n<1 treated as 1
        assert_eq!(skip_index(2, -4, 5), 3);
    }

    #[test]
    fn back_wraps_and_floors_to_one() {
        assert_eq!(back_index(1, 1, 5), 0);
        assert_eq!(back_index(0, 1, 5), 4); // wrap
        assert_eq!(back_index(2, 3, 5), 4); // 2-3 mod 5
        assert_eq!(back_index(3, 0, 5), 2); // n<1 treated as 1
    }
}
