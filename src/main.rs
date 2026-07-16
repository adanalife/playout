use std::hash::{BuildHasher, Hasher};
use std::path::PathBuf;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use anyhow::{Context, Result, bail};
use gst::glib;
use gst::prelude::*;
use gstreamer as gst;
use tokio::signal::unix::{SignalKind, signal};
use tracing::{error, info, warn};

mod http;
mod nats;
mod telemetry;
mod watchdog;

/// Build identity served at /version (the fleet-wide version-discovery
/// contract). Release images stamp VERSION/SHA via Docker build-args and
/// BUILT_AT at image build; plain cargo builds fall back to the crate version.
pub const VERSION: &str = match option_env!("VERSION") {
    Some(v) => v,
    None => env!("CARGO_PKG_VERSION"),
};
pub const SHA: &str = match option_env!("SHA") {
    Some(s) => s,
    None => "unknown",
};
pub const BUILT_AT: &str = match option_env!("BUILT_AT") {
    Some(t) => t,
    None => "unknown",
};

fn env_or(key: &str, default: &str) -> String {
    std::env::var(key).unwrap_or_else(|_| default.to_string())
}

/// Recursively collect the `.mp4` files (case-insensitive) under `dir`,
/// sorted by full path. vlc-server walks recursively too — today's corpus is
/// flat, but the scan must not silently miss a subdir the day one appears.
/// An empty corpus is a deployment fault: bail loudly and let the pod
/// crash-loop rather than publish a dead stream.
fn scan_video_dir(dir: &str) -> Result<Vec<PathBuf>> {
    let mut files = Vec::new();
    let mut dirs = vec![PathBuf::from(dir)];
    while let Some(d) = dirs.pop() {
        for entry in
            std::fs::read_dir(&d).with_context(|| format!("reading VIDEO_DIR {}", d.display()))?
        {
            let Ok(entry) = entry else { continue };
            let path = entry.path();
            if path.is_dir() {
                dirs.push(path);
            } else if path
                .extension()
                .and_then(|e| e.to_str())
                .is_some_and(|e| e.eq_ignore_ascii_case("mp4"))
            {
                files.push(path);
            }
        }
    }
    files.sort();
    if files.is_empty() {
        bail!("no .mp4 files found in {dir}");
    }
    Ok(files)
}

/// The encode branch ends in an RTSP RECORD publish to MediaMTX; consumers
/// attach to MediaMTX, so this end can restart without them noticing.
///
/// ENCODER=passthrough publishes the corpus clips' compressed H.264 without
/// re-encoding — the airing corpus is transcoded to one uniform spec
/// (identical params, IDR-leading closed GOPs), which is what makes splicing
/// compressed streams safe. h264parse re-sends SPS/PPS at every IDR so each
/// splice and every late joiner resyncs.
fn make_encode_branch(encoder_name: &str, rtsp_url: &str) -> Result<Vec<gst::Element>> {
    let queue = gst::ElementFactory::make("queue").build()?;
    let parse = gst::ElementFactory::make("h264parse").build()?;
    // Re-send SPS/PPS with every IDR so late joiners always sync.
    parse.set_property("config-interval", -1i32);
    let sink = gst::ElementFactory::make("rtspclientsink").build()?;
    sink.set_property("location", rtsp_url);

    if encoder_name == "passthrough" {
        return Ok(vec![queue, parse, sink]);
    }

    let encoder = gst::ElementFactory::make(encoder_name)
        .build()
        .with_context(|| format!("creating encoder {encoder_name}"))?;
    if encoder_name == "x264enc" {
        encoder.set_property("bitrate", 8000u32);
        // 2s GOP at 60fps, matching the corpus spec the stream runs today.
        encoder.set_property("key-int-max", 120u32);
        encoder.set_property_from_str("speed-preset", "veryfast");
    }
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
    /// Pipeline running time when this clip became active; None until then.
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
    /// Clip bins stop at parsed H.264 instead of decoding, and seeks snap to
    /// keyframes (a compressed stream can't start mid-GOP).
    passthrough: bool,
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

/// Keeps a resume/play.at seek from landing in a clip's last moments — the
/// clip would EOS almost immediately after the splice. An unknown duration
/// errs toward seeking (matches vlc-server's tail guard).
const SEEK_TAIL_GUARD_MS: i64 = 2000;

fn should_seek_to(offset_ms: i64, duration_ms: Option<i64>) -> bool {
    if offset_ms <= 0 {
        return false;
    }
    match duration_ms {
        Some(d) if d > 0 => offset_ms < d - SEEK_TAIL_GUARD_MS,
        _ => true,
    }
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
        telemetry::CLIP_SPAWNS.add(1, &[]);
        let decode = gst::ElementFactory::make("uridecodebin3")
            .property("uri", &uri)
            .build()
            .expect("creating uridecodebin3");

        // Passthrough: stop at parsed H.264 — decodebin3 emits the demuxed,
        // parsed stream and never builds a decoder. Relies on the corpus
        // contract (every clip the same uniform spec); a non-H.264 clip fails
        // to negotiate and errors loudly rather than silently re-encoding.
        if self.passthrough {
            decode.set_property("caps", gst::Caps::builder("video/x-h264").build());
        }

        // Video only: audio is composited downstream in OBS, and deselecting
        // here keeps clips with/without audio tracks topology-identical.
        decode.connect("select-stream", false, |args| {
            let stream = args[2].get::<gst::Stream>().unwrap();
            let selected = stream.stream_type().contains(gst::StreamType::VIDEO);
            Some((selected as i32).to_value())
        });

        // Request the concat pad here, not in pad-added: concat plays its
        // sink pads in request order, while pad-added fires in
        // preroll-completion order. Letting preroll order pick the pad order
        // plays clips out of sequence — and on resume the seeked clip
        // prerolls last, so its pad lands behind the "next" clip and the
        // pipeline EOSes at the first boundary.
        let sinkpad = self
            .concat
            .request_pad_simple("sink_%u")
            .expect("requesting concat pad");

        // EOS on this pad = clip finished and concat has moved on: tear
        // down the finished bin off the streaming thread and top up.
        let player = Arc::clone(self);
        let decode_for_eos = decode.clone();
        sinkpad.add_probe(gst::PadProbeType::EVENT_DOWNSTREAM, move |pad, info| {
            let Some(ev) = info.event() else {
                return gst::PadProbeReturn::Ok;
            };
            if ev.type_() != gst::EventType::Eos {
                return gst::PadProbeReturn::Ok;
            }
            let player = Arc::clone(&player);
            let decode = decode_for_eos.clone();
            let pad = pad.clone();
            glib::idle_add_once(move || player.on_clip_eos(&decode, &pad));
            // Drop the EOS: concat handles pad switching itself, and the
            // pipeline-level EOS must never fire (24/7 stream).
            gst::PadProbeReturn::Drop
        });

        let player = Arc::clone(self);
        let concat_pad = sinkpad.clone();
        decode.connect_pad_added(move |decode, pad| {
            // No seek requested: link straight into concat. The clip's buffers
            // block there until every earlier pad finishes.
            if offset_ms <= 0 {
                pad.link(&concat_pad).expect("linking clip into concat");
                return;
            }

            // Seek path (play.at / resume). Constraints, each one learned the
            // hard way: a flush seek issued from a streaming thread (this
            // callback, or a pad probe) deadlocks waiting for the very thread
            // issuing it; the bin must negotiate while LINKED into concat or
            // the decoder picks caps against an unlinked pad and errors
            // not-negotiated; the seek only takes once the bin is streaming
            // end-to-end (first decoded buffer) — issued earlier it is
            // accepted and silently swallowed; and nothing pre-seek — the
            // flush, buffers, or the pre-seek segment, which would rewind
            // downstream running time and fast-forward the clip — may reach
            // concat's shared chain. So: link immediately, and hold a probe
            // that DROPS events (they stay sticky on the pad, and the
            // post-seek ones re-deliver once the probe is gone — concat
            // never sees the pre-seek segment) and BLOCKS the first buffer,
            // which proves the bin fully up; then seek from the main loop
            // and unblock. Concat's first sight of this pad is the post-seek
            // segment and buffers to match — indistinguishable from a clip
            // that begins at the offset.
            let player = Arc::clone(&player);
            let decode = decode.clone();
            let pad_for_seek = pad.clone();
            let probes = Arc::new(Mutex::new(Vec::new()));
            let probes_for_seek = Arc::clone(&probes);
            let scheduled = std::sync::atomic::AtomicBool::new(false);
            // Flush containment must be its own NON-blocking probe: flush
            // events bypass blocking probes entirely (callback included), so
            // the data probe below never even sees them.
            let flush_probe = pad
                .add_probe(gst::PadProbeType::EVENT_FLUSH, |_, _| {
                    gst::PadProbeReturn::Drop
                })
                .expect("adding flush-drop probe");
            let data_probe = pad
                .add_probe(gst::PadProbeType::BLOCK_DOWNSTREAM, move |_, info| {
                    if info.event().is_some() {
                        return gst::PadProbeReturn::Drop;
                    }
                    if scheduled.swap(true, Ordering::SeqCst) {
                        return gst::PadProbeReturn::Ok;
                    }
                    let player = Arc::clone(&player);
                    let decode = decode.clone();
                    let pad = pad_for_seek.clone();
                    let probes = Arc::clone(&probes_for_seek);
                    glib::idle_add_once(move || {
                        player.finish_seek(decode, pad, probes, offset_ms);
                    });
                    gst::PadProbeReturn::Ok
                })
                .expect("blocking clip pad");
            *probes.lock().unwrap() = vec![data_probe, flush_probe];
            pad.link(&concat_pad).expect("linking clip into concat");
        });

        // Push the bookkeeping entry before the bin can start prerolling, so
        // pad-added always finds it.
        self.clips.lock().unwrap().push(Clip {
            bin: decode.clone(),
            pad: Some(sinkpad),
            index,
            offset_ms,
            start_rt: None,
        });
        self.pipeline.add(&decode).expect("adding clip bin");
        decode.sync_state_with_parent().expect("starting clip bin");
    }

    /// Complete a pending clip seek from the main loop, then unblock the
    /// pad's probe so data flows into concat. Runs once the first decoded
    /// buffer reaches the pad — the only point where a flush seek on the bin
    /// reliably takes (any earlier and uridecodebin3 accepts but swallows it).
    fn finish_seek(
        self: &Arc<Self>,
        decode: gst::Element,
        pad: gst::Pad,
        probes: Arc<Mutex<Vec<gst::PadProbeId>>>,
        offset_ms: i64,
    ) {
        let mut offset_ms = offset_ms;
        let duration_ms = decode
            .query_duration::<gst::ClockTime>()
            .map(|d| d.mseconds() as i64);
        // Tail guard and refusal both fall back to top-of-clip, like
        // vlc-server.
        if !should_seek_to(offset_ms, duration_ms) {
            info!(offset_ms, "seek lands at the clip tail; starting at top");
            offset_ms = 0;
        }
        if offset_ms > 0 {
            let pos = gst::ClockTime::from_mseconds(offset_ms as u64);
            // Passthrough can't decode-trim to an exact frame: snap to the
            // keyframe at/before the target instead (≤ one GOP early, so the
            // playhead the offset seeds runs up to 2s ahead of true position
            // at the corpus's 2s GOP — fine for resume).
            let flags = if self.passthrough {
                gst::SeekFlags::FLUSH | gst::SeekFlags::KEY_UNIT | gst::SeekFlags::SNAP_BEFORE
            } else {
                gst::SeekFlags::FLUSH | gst::SeekFlags::ACCURATE
            };
            if decode.seek_simple(flags, pos).is_err() {
                warn!(offset_ms, "seek refused; starting clip at top");
                offset_ms = 0;
            }
        }
        for id in probes.lock().unwrap().drain(..) {
            pad.remove_probe(id);
        }
        info!(offset_ms, "clip seeked and unblocked");

        // Record the offset the clip actually starts at (the guards above may
        // have demoted a requested offset to top-of-clip).
        if let Some(clip) = self
            .clips
            .lock()
            .unwrap()
            .iter_mut()
            .find(|c| c.bin == decode)
        {
            clip.offset_ms = offset_ms;
        }
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

    /// Pipeline running time: how long the pipeline has been playing, by the
    /// clock. Unlike a position query (answered from stream time, which jumps
    /// with every clip's segment) this is monotonic and wall-paced.
    fn running_time(&self) -> Option<gst::ClockTime> {
        let now = self.pipeline.clock()?.time();
        Some(now.saturating_sub(self.pipeline.base_time()?))
    }

    /// Stamp the current active clip (clips[0]) with the running time it went
    /// live, so the playhead can report position within the clip.
    fn mark_active(&self) {
        let rt = self.running_time();
        if let Some(active) = self.clips.lock().unwrap().first_mut() {
            active.start_rt = rt.or(Some(gst::ClockTime::ZERO));
        }
    }

    /// Tear down the prerolled clip(s) behind the active one, releasing their
    /// concat pads — so a following `spawn` becomes concat's next pad.
    fn teardown_preroll(&self) {
        let extra: Vec<Clip> = self.clips.lock().unwrap().drain(1..).collect();
        for c in extra {
            // Release the concat pad BEFORE stopping the bin: the prerolled
            // bin's streaming thread is parked inside concat waiting for its
            // turn, holding its pad's stream lock — set_state(Null) needs
            // that lock to deactivate the pad and deadlocks unless the
            // release wakes the waiter first.
            if let Some(pad) = c.pad {
                self.concat.release_request_pad(&pad);
            }
            c.bin.set_state(gst::State::Null).ok();
            self.pipeline.remove(&c.bin).ok();
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
    /// last-value cache. Position = start offset + running time since the
    /// clip went active — clock-derived, so it can neither freeze nor race
    /// ahead the way position queries (stream time) and PTS watermarks
    /// (decode/queue horizon) both do. Falls back to the offset alone before
    /// the clip is stamped active.
    fn playhead(&self) -> Option<(String, i64)> {
        let clips = self.clips.lock().unwrap();
        let active = clips.first()?;
        let basename = self.basename_at(active.index);
        let position_ms = match (self.running_time(), active.start_rt) {
            (Some(now), Some(start)) => {
                active.offset_ms + (now.mseconds() as i64 - start.mseconds() as i64).max(0)
            }
            _ => active.offset_ms,
        };
        Some((basename, position_ms))
    }
}

fn main() -> Result<()> {
    // Reads SENTRY_DSN from the environment; unset (local runs) leaves the
    // client disabled. ENV (development/staging/production) doubles as the
    // Sentry environment tag, matching the Go fleet. Init precedes the tokio
    // runtime so the transport thread outlives every worker.
    let _sentry = sentry::init(sentry::ClientOptions {
        release: Some(format!("playout@{VERSION}").into()),
        environment: std::env::var("ENV").ok().map(Into::into),
        ..Default::default()
    });
    // One binary serves per-platform deployments (playout-youtube,
    // playout-twitch) sharing one Sentry project; the `platform` tag makes
    // twitch vs youtube errors filterable within it, matching the Go fleet.
    let platform = env_or("STREAM_PLATFORM", "youtube");
    if !platform.is_empty() {
        sentry::configure_scope(|scope| scope.set_tag("platform", &platform));
    }
    run()
}

#[tokio::main]
async fn run() -> Result<()> {
    use tracing_subscriber::layer::SubscriberExt;
    use tracing_subscriber::util::SubscriberInitExt;
    tracing_subscriber::registry()
        .with(
            tracing_subscriber::EnvFilter::try_from_default_env().unwrap_or_else(|_| "info".into()),
        )
        .with(tracing_subscriber::fmt::layer())
        // ERROR events become Sentry events; WARN/INFO attach as breadcrumbs.
        .with(sentry_tracing::layer())
        .init();
    info!(version = VERSION, sha = SHA, "playout starting");

    let video_dir = env_or("VIDEO_DIR", "/opt/data/Dashcam/_all");
    let output = env_or("OUTPUT", "rtsp"); // rtsp | window | both
    let rtsp_url = env_or("RTSP_URL", "rtsp://localhost:8554/dashcam");
    let encoder_name = env_or("ENCODER", "x264enc");
    let nats_env = env_or("ENV", "development");
    let platform = env_or("STREAM_PLATFORM", "youtube");
    let nats_url = env_or("NATS_URL", "nats://localhost:4222");

    let meter_provider = telemetry::init(&platform, &nats_env);

    let files = scan_video_dir(&video_dir)?;
    info!(
        clips = files.len(),
        video_dir = %video_dir,
        output = %output,
        encoder = %encoder_name,
        "playlist ready"
    );

    let passthrough = encoder_name == "passthrough";
    if passthrough && output != "rtsp" {
        bail!("OUTPUT={output} needs decoded video; ENCODER=passthrough supports only rtsp");
    }

    gst::init()?;
    let pipeline = gst::Pipeline::new();

    // concat splices the per-clip decode bins into one continuous stream:
    // it rewrites each clip's segment so downstream running time never
    // resets, which is exactly what the encoder needs to stay unbroken.
    let concat = gst::ElementFactory::make("concat").build()?;
    let tee = gst::ElementFactory::make("tee").build()?;

    if passthrough {
        // Compressed splice: no raw-video processing exists to normalize
        // clips, so the corpus contract (uniform 1080p60, closed GOPs) is
        // the spec enforcement — a non-spec clip changes caps mid-stream.
        let q1 = gst::ElementFactory::make("queue").build()?;
        pipeline.add_many([&concat, &q1, &tee])?;
        gst::Element::link_many([&concat, &q1, &tee])?;
    } else {
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
        pipeline.add_many([
            &concat, &q1, &convert, &q2, &scale, &q3, &rate, &q4, &caps, &tee,
        ])?;
        gst::Element::link_many([
            &concat, &q1, &convert, &q2, &scale, &q3, &rate, &q4, &caps, &tee,
        ])?;
    }

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
        passthrough,
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
    // Cold boot (no resume state) starts on a random clip, like vlc-server —
    // otherwise every clean deploy replays the same first clip on stream.
    let (first, offset) = resume.unwrap_or_else(|| (player.random_index(), 0));
    player.spawn(first, offset);
    player.spawn((first + 1) % player.files.len(), 0);

    telemetry::spawn_recorder(player.clone());
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

    // k8s stops the pod with SIGTERM: quit the main loop so the pipeline
    // drops to Null below, which tears down the RTSP publish cleanly.
    let loop_signal = main_loop.clone();
    tokio::spawn(async move {
        let mut term = signal(SignalKind::terminate()).expect("installing SIGTERM handler");
        let mut int = signal(SignalKind::interrupt()).expect("installing SIGINT handler");
        tokio::select! {
            _ = term.recv() => info!("SIGTERM received, shutting down"),
            _ = int.recv() => info!("SIGINT received, shutting down"),
        }
        loop_signal.quit();
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

    // Watchdog only when we actually publish (not the window-only local mode).
    if output != "window" {
        let wd_failed = failed.clone();
        let wd_loop = main_loop.clone();
        tokio::spawn(watchdog::run(rtsp_url.clone(), move || {
            wd_failed.store(true, Ordering::SeqCst);
            wd_loop.quit();
        }));
    }

    pipeline.set_state(gst::State::Playing)?;
    player.mark_active();
    main_loop.run();
    pipeline.set_state(gst::State::Null)?;

    if let Some(provider) = meter_provider {
        let _ = provider.shutdown();
    }

    if failed.load(Ordering::SeqCst) {
        bail!("pipeline failed");
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::{back_index, scan_video_dir, should_seek_to, skip_index};

    #[test]
    fn scan_walks_subdirs_case_insensitively() {
        let root = std::env::temp_dir().join(format!("playout-scan-test-{}", std::process::id()));
        let sub = root.join("sub");
        std::fs::create_dir_all(&sub).unwrap();
        std::fs::write(root.join("a.MP4"), b"").unwrap();
        std::fs::write(sub.join("b.mp4"), b"").unwrap();
        std::fs::write(sub.join("notes.txt"), b"").unwrap();

        let files = scan_video_dir(root.to_str().unwrap()).unwrap();
        let names: Vec<_> = files
            .iter()
            .map(|p| p.file_name().unwrap().to_str().unwrap().to_string())
            .collect();
        assert_eq!(names, ["a.MP4", "b.mp4"]);

        std::fs::remove_dir_all(&root).unwrap();
        assert!(scan_video_dir(root.to_str().unwrap()).is_err());
    }

    #[test]
    fn skip_wraps_and_floors_to_one() {
        assert_eq!(skip_index(0, 1, 5), 1);
        assert_eq!(skip_index(3, 3, 5), 1); // 3+3=6 % 5
        assert_eq!(skip_index(4, 1, 5), 0); // wrap
        assert_eq!(skip_index(2, 0, 5), 3); // n<1 treated as 1
        assert_eq!(skip_index(2, -4, 5), 3);
    }

    #[test]
    fn seek_guards() {
        assert!(!should_seek_to(0, Some(60_000))); // no offset
        assert!(!should_seek_to(-5, Some(60_000)));
        assert!(should_seek_to(30_000, Some(60_000))); // mid-clip
        assert!(!should_seek_to(58_000, Some(60_000))); // tail guard
        assert!(!should_seek_to(59_500, Some(60_000))); // past the end
        assert!(should_seek_to(30_000, None)); // unknown duration errs toward seeking
        assert!(should_seek_to(30_000, Some(0)));
    }

    #[test]
    fn back_wraps_and_floors_to_one() {
        assert_eq!(back_index(1, 1, 5), 0);
        assert_eq!(back_index(0, 1, 5), 4); // wrap
        assert_eq!(back_index(2, 3, 5), 4); // 2-3 mod 5
        assert_eq!(back_index(3, 0, 5), 2); // n<1 treated as 1
    }
}
