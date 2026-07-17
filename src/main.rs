use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering};
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
mod player;
mod telemetry;
mod watchdog;

use player::{Player, SharedPlayer};

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

/// One output frame's PTS is a "gap" when it lands more than `threshold_ns`
/// after the previous frame — a visible stall or drop. `prev_ns == u64::MAX`
/// is the "no previous frame yet" sentinel (first buffer never counts).
fn is_frame_gap(prev_ns: u64, pts_ns: u64, threshold_ns: u64) -> bool {
    prev_ns != u64::MAX && pts_ns > prev_ns.saturating_add(threshold_ns)
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
    // The deployment.environment OTLP label is the k8s namespace (prod-1 /
    // stage-1), matching the Go fleet so playout's series share the dashboards'
    // env filter. Distinct from ENV, which is the NATS subject env
    // (production / staging). Falls back to the NATS env for local runs.
    let deployment_env = env_or("DEPLOYMENT_ENVIRONMENT", &nats_env);

    let meter_provider = telemetry::init(&platform, &deployment_env);

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

    // Output-frame telemetry, tapped once at the tee's sink pad — upstream of
    // the branch split, so it counts frames regardless of how many outputs are
    // wired. Every buffer here is one frame: raw video in the decoded path,
    // one parsed H.264 access unit in passthrough. rate(playout_output_frames_total)
    // is the true output fps. A PTS jump past the gap threshold is a frame the
    // viewer saw stall or drop — concat keeps running time continuous across
    // splices, so a gap means a real hitch, not a boundary.
    // ponytail: thresholds hard-coded for the corpus's fixed 1080p60; derive
    // from the caps framerate if playout ever runs a non-60 stream.
    const FRAME_INTERVAL_NS: u64 = 1_000_000_000 / 60;
    const GAP_THRESHOLD_NS: u64 = FRAME_INTERVAL_NS * 3 / 2;
    let last_pts = AtomicU64::new(u64::MAX);
    tee.static_pad("sink")
        .expect("tee always has a static sink pad")
        .add_probe(gst::PadProbeType::BUFFER, move |_, info| {
            if let Some(gst::PadProbeData::Buffer(ref buf)) = info.data {
                telemetry::OUTPUT_FRAMES.add(1, telemetry::attrs());
                if let Some(pts) = buf.pts() {
                    let prev = last_pts.swap(pts.nseconds(), Ordering::Relaxed);
                    if is_frame_gap(prev, pts.nseconds(), GAP_THRESHOLD_NS) {
                        telemetry::OUTPUT_FRAME_GAPS.add(1, telemetry::attrs());
                    }
                }
            }
            gst::PadProbeReturn::Ok
        });

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
        recoveries: AtomicUsize::new(0),
        durations: Mutex::new(HashMap::new()),
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
    let player_bus = Arc::clone(&player);
    let bus = pipeline.bus().unwrap();
    let _watch = bus.add_watch(move |_, msg| {
        match msg.view() {
            gst::MessageView::Error(err) => {
                let src = err.src().map(|s| s.path_string()).unwrap_or_default();
                if err.src().is_some_and(|s| player_bus.on_clip_error(s)) {
                    warn!(
                        src = %src,
                        err = %err.error(),
                        debug = ?err.debug(),
                        "clip error absorbed"
                    );
                } else {
                    error!(
                        src = %src,
                        err = %err.error(),
                        debug = ?err.debug(),
                        "pipeline error"
                    );
                    failed_clone.store(true, Ordering::SeqCst);
                    loop_clone.quit();
                }
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
    use super::{is_frame_gap, scan_video_dir};

    #[test]
    fn frame_gap_detection() {
        let interval = 1_000_000_000u64 / 60; // 16.67ms
        let threshold = interval * 3 / 2;
        assert!(!is_frame_gap(u64::MAX, 12345, threshold)); // first buffer
        assert!(!is_frame_gap(0, interval, threshold)); // steady 60fps step
        assert!(!is_frame_gap(0, interval, threshold)); // exactly one interval
        assert!(is_frame_gap(0, interval * 2, threshold)); // a frame missing
        assert!(!is_frame_gap(interval * 5, interval * 4, threshold)); // non-increasing PTS
    }

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
}
