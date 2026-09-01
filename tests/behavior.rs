//! Behavior harness: boots the real binary against a real MediaMTX and NATS
//! (JetStream) and asserts behavior over HTTP, NATS, and RTSP — no mocks, the
//! same wire tripbot speaks. Realtime throughput is explicitly
//! out of scope (CI runners can't sustain 1080p60); these tests assert
//! *behavior*: publish-on-boot, resume, commands, boundaries, shutdown.
//!
//! Requires `mediamtx`, `nats-server`, and `gst-launch-1.0` on PATH; each
//! test skips (passing) when they're missing, so plain `cargo test` still
//! works on a machine without them. CI installs all three and sets
//! `PLAYOUT_TOOLS_REQUIRED`, which turns that skip into a failure — otherwise a
//! broken tool install leaves the whole harness a green no-op.

use std::io::{Read, Write};
use std::net::{TcpListener, TcpStream};
use std::path::{Path, PathBuf};
use std::process::{Child, Command, Stdio};
use std::sync::OnceLock;
use std::time::{Duration, Instant};

const CLIPS: [&str; 3] = ["clip_a.mp4", "clip_b.mp4", "clip_c.mp4"];
const CLIP_SECONDS: u64 = 2;

/// Pipeline mutations against one MediaMTX/NATS/x264 stack per test are
/// cheap, but N concurrent 1080p60 x264 encoders on a 2-core CI runner are
/// not — serialize the suite. Poisoned locks (a failed test) don't cascade.
static SERIAL: tokio::sync::Mutex<()> = tokio::sync::Mutex::const_new(());

macro_rules! serial_or_skip {
    () => {
        let _guard = SERIAL.lock().await;
        let missing = missing_tools();
        if !missing.is_empty() {
            // Skipping is the right answer on a laptop without the tools, and
            // the wrong one in CI: there the whole harness would stop asserting
            // and still report green. PLAYOUT_TOOLS_REQUIRED marks the
            // environments that installed the tools on purpose.
            assert!(
                std::env::var_os("PLAYOUT_TOOLS_REQUIRED").is_none(),
                "PLAYOUT_TOOLS_REQUIRED is set, so the behavior harness must run, \
                 but these are not on PATH: {missing:?}"
            );
            eprintln!("skipping: not on PATH: {missing:?}");
            return;
        }
    };
}

/// Which of the harness's external tools are absent; empty when all are
/// present. `--version` on each: every one of them exits promptly on it, so
/// spawnability is the signal and nothing is left running.
fn missing_tools() -> &'static [&'static str] {
    static MISSING: OnceLock<Vec<&'static str>> = OnceLock::new();
    MISSING
        .get_or_init(|| {
            ["mediamtx", "nats-server", "gst-launch-1.0"]
                .into_iter()
                .filter(|bin| {
                    Command::new(bin)
                        .arg("--version")
                        .stdout(Stdio::null())
                        .stderr(Stdio::null())
                        .status()
                        .is_err()
                })
                .collect()
        })
        .as_slice()
}

fn free_port() -> u16 {
    // Not bind-port-0-and-drop: the OS hands the same ephemeral port to
    // back-to-back callers, so while one claimant is still starting up the
    // next test's server grabs its port and one of them dies on "address in
    // use" (a mediamtx that never listens, a playout whose HTTP task
    // panics). Claim sequentially from a pid-salted window instead — unique
    // within the run by construction — and probe each is actually bindable
    // before handing it out.
    use std::sync::atomic::{AtomicU16, Ordering};
    static NEXT: AtomicU16 = AtomicU16::new(0);
    let base = 20_000 + (std::process::id() % 20_000) as u16;
    loop {
        let port = base + NEXT.fetch_add(1, Ordering::Relaxed) % 20_000;
        if TcpListener::bind(("127.0.0.1", port)).is_ok() {
            return port;
        }
    }
}

/// Child process killed on drop so a failing test never leaks servers.
struct Proc(Child);

impl Drop for Proc {
    fn drop(&mut self) {
        let _ = self.0.kill();
        let _ = self.0.wait();
    }
}

fn wait_tcp(port: u16, what: &str) {
    let deadline = Instant::now() + Duration::from_secs(10);
    while Instant::now() < deadline {
        if TcpStream::connect(("127.0.0.1", port)).is_ok() {
            return;
        }
        std::thread::sleep(Duration::from_millis(50));
    }
    panic!("{what} did not listen on {port} within 10s");
}

fn gen_corpus(dir: &Path, width: u32, height: u32, fps: u32, seconds: u64) {
    std::fs::create_dir_all(dir).unwrap();
    for (i, name) in CLIPS.iter().enumerate() {
        let status = Command::new("gst-launch-1.0")
            .args([
                "-q",
                "videotestsrc",
                &format!("num-buffers={}", seconds * fps as u64),
                &format!("pattern={i}"),
                "!",
                &format!("video/x-raw,width={width},height={height},framerate={fps}/1"),
                "!",
                "x264enc",
                "speed-preset=ultrafast",
                // 2 B-frames like the airing corpus, so passthrough splices
                // carry real DTS/PTS reordering, not a zerolatency simplification.
                "bframes=2",
                "key-int-max=60",
                "!",
                "h264parse",
                "!",
                "mp4mux",
                "!",
                "filesink",
                &format!("location={}", dir.join(name).display()),
            ])
            .status()
            .expect("running gst-launch-1.0");
        assert!(status.success(), "generating {name} failed");
    }
}

/// The main corpus: three 2s 1080p60 clips (the stream's real shape), short
/// so boundary/wrap tests turn over quickly.
fn corpus() -> &'static Path {
    static DIR: OnceLock<PathBuf> = OnceLock::new();
    DIR.get_or_init(|| {
        let dir = std::env::temp_dir().join(format!("playout-parity-{}", std::process::id()));
        gen_corpus(&dir, 1920, 1080, 60, CLIP_SECONDS);
        dir
    })
}

/// The main corpus plus a garbage `.mp4` (sorted mid-playlist, between b and
/// c) for the corrupt-clip recovery tests.
fn corrupt_corpus() -> &'static Path {
    static DIR: OnceLock<PathBuf> = OnceLock::new();
    DIR.get_or_init(|| {
        let dir =
            std::env::temp_dir().join(format!("playout-parity-corrupt-{}", std::process::id()));
        std::fs::create_dir_all(&dir).unwrap();
        for name in CLIPS {
            std::fs::copy(corpus().join(name), dir.join(name)).unwrap();
        }
        std::fs::write(dir.join("clip_bad.mp4"), b"this is not an mp4").unwrap();
        dir
    })
}

/// A corpus where every `.mp4` is garbage — the deployment fault that must
/// crash-loop visibly rather than spin through recovery forever.
fn all_bad_corpus() -> &'static Path {
    static DIR: OnceLock<PathBuf> = OnceLock::new();
    DIR.get_or_init(|| {
        let dir =
            std::env::temp_dir().join(format!("playout-parity-allbad-{}", std::process::id()));
        std::fs::create_dir_all(&dir).unwrap();
        for name in CLIPS {
            std::fs::write(dir.join(name), b"this is not an mp4").unwrap();
        }
        dir
    })
}

/// Long-clip corpus (20s, small frames for cheap encode) for tests that
/// assert "current did NOT change" — with 2s clips a natural boundary lands
/// mid-assertion and reads as a leaked command.
fn long_corpus() -> &'static Path {
    static DIR: OnceLock<PathBuf> = OnceLock::new();
    DIR.get_or_init(|| {
        let dir = std::env::temp_dir().join(format!("playout-parity-long-{}", std::process::id()));
        gen_corpus(&dir, 640, 360, 30, 20);
        dir
    })
}

fn start_nats() -> (Proc, u16) {
    let port = free_port();
    let sd = std::env::temp_dir().join(format!("playout-parity-js-{}-{port}", std::process::id()));
    let child = Command::new("nats-server")
        .args(["-js", "-p", &port.to_string(), "-sd", sd.to_str().unwrap()])
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .spawn()
        .expect("spawning nats-server");
    wait_tcp(port, "nats-server");
    (Proc(child), port)
}

fn start_mediamtx() -> (Proc, u16) {
    let port = free_port();
    let child = Command::new("mediamtx")
        .env("MTX_RTSPADDRESS", format!(":{port}"))
        .env("MTX_RTMP", "no")
        .env("MTX_HLS", "no")
        .env("MTX_WEBRTC", "no")
        .env("MTX_SRT", "no")
        .env("MTX_API", "no")
        .env("MTX_METRICS", "no")
        .env("MTX_PPROF", "no")
        .env("MTX_PLAYBACK", "no")
        .env("MTX_LOGLEVEL", "error")
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .spawn()
        .expect("spawning mediamtx");
    wait_tcp(port, "mediamtx");
    (Proc(child), port)
}

struct Playout {
    proc: Proc,
    http: u16,
    rtsp_url: String,
}

fn start_playout(
    video_dir: &Path,
    nats_port: Option<u16>,
    mtx_port: u16,
    platform: &str,
) -> Playout {
    start_playout_with(video_dir, nats_port, mtx_port, platform, "x264enc")
}

fn start_playout_with(
    video_dir: &Path,
    nats_port: Option<u16>,
    mtx_port: u16,
    platform: &str,
    encoder: &str,
) -> Playout {
    let http = free_port();
    let rtsp_url = format!("rtsp://127.0.0.1:{mtx_port}/dashcam");
    let nats_url = nats_port.map_or(String::new(), |p| format!("nats://127.0.0.1:{p}"));
    let child = Command::new(env!("CARGO_BIN_EXE_playout"))
        .env("VIDEO_DIR", video_dir)
        .env("OUTPUT", "rtsp")
        .env("RTSP_URL", &rtsp_url)
        .env("ENCODER", encoder)
        .env("ENV", "test")
        .env("STREAM_PLATFORM", platform)
        .env("NATS_URL", &nats_url)
        .env("HTTP_PORT", http.to_string())
        .env_remove("SENTRY_DSN")
        .env_remove("OTEL_EXPORTER_OTLP_ENDPOINT")
        .spawn()
        .expect("spawning playout");
    Playout {
        proc: Proc(child),
        http,
        rtsp_url,
    }
}

/// Minimal HTTP/1.0 GET returning (status, exact body bytes) — hand-rolled so
/// byte-exactness assertions (`/playout/current` must be basename-only, no
/// trailing newline) test the real wire, not a client's trimming.
fn http_get(port: u16, path: &str) -> Option<(u16, Vec<u8>)> {
    let mut conn = TcpStream::connect(("127.0.0.1", port)).ok()?;
    conn.write_all(format!("GET {path} HTTP/1.0\r\nHost: t\r\n\r\n").as_bytes())
        .ok()?;
    let mut raw = Vec::new();
    conn.read_to_end(&mut raw).ok()?;
    let split = raw.windows(4).position(|w| w == b"\r\n\r\n")?;
    let head = String::from_utf8_lossy(&raw[..split]);
    let status: u16 = head.split_whitespace().nth(1)?.parse().ok()?;
    Some((status, raw[split + 4..].to_vec()))
}

fn current(port: u16) -> String {
    let (status, body) = http_get(port, "/playout/current").unwrap_or((0, Vec::new()));
    assert!(
        status == 200 || status == 0,
        "GET /playout/current -> {status}"
    );
    String::from_utf8(body).expect("current is utf-8")
}

fn wait_for<T>(what: &str, timeout: Duration, mut probe: impl FnMut() -> Option<T>) -> T {
    let deadline = Instant::now() + timeout;
    while Instant::now() < deadline {
        if let Some(v) = probe() {
            return v;
        }
        std::thread::sleep(Duration::from_millis(200));
    }
    panic!("timed out after {timeout:?} waiting for {what}");
}

fn wait_ready(port: u16) {
    wait_for("readiness", Duration::from_secs(30), || {
        matches!(http_get(port, "/health/ready"), Some((200, _))).then_some(())
    });
}

fn wait_current(port: u16, what: &str, pred: impl Fn(&str) -> bool) -> String {
    wait_for(what, Duration::from_secs(20), || {
        let c = current(port);
        pred(&c).then_some(c)
    })
}

fn describe_ok(rtsp_url: &str) -> bool {
    let authority = rtsp_url.strip_prefix("rtsp://").unwrap();
    let (hostport, _) = authority.split_once('/').unwrap();
    let Ok(mut conn) = TcpStream::connect(hostport) else {
        return false;
    };
    conn.set_read_timeout(Some(Duration::from_secs(3))).unwrap();
    conn.write_all(
        format!("DESCRIBE {rtsp_url} RTSP/1.0\r\nCSeq: 1\r\nAccept: application/sdp\r\n\r\n")
            .as_bytes(),
    )
    .unwrap();
    let mut buf = [0u8; 256];
    let n = conn.read(&mut buf).unwrap_or(0);
    String::from_utf8_lossy(&buf[..n]).starts_with("RTSP/1.0 200")
}

const LASTPLAYED_STREAM: &str = "TRIPBOT_PLAYOUT_LASTPLAYED";

fn lastplayed_subject(platform: &str) -> String {
    format!("tripbot.test.playout.lastplayed.{platform}")
}

async fn nats_client(port: u16) -> async_nats::Client {
    async_nats::connect(format!("nats://127.0.0.1:{port}"))
        .await
        .expect("connecting test nats client")
}

async fn seed_lastplayed(port: u16, platform: &str, file: &str, position_ms: i64) {
    let js = async_nats::jetstream::new(nats_client(port).await);
    js.create_stream(async_nats::jetstream::stream::Config {
        name: LASTPLAYED_STREAM.to_string(),
        subjects: vec!["tripbot.test.playout.lastplayed.*".to_string()],
        max_messages_per_subject: 1,
        ..Default::default()
    })
    .await
    .expect("creating lastplayed stream");
    js.publish(
        lastplayed_subject(platform),
        serde_json::json!({"emitted_at": "", "file": file, "position_ms": position_ms})
            .to_string()
            .into(),
    )
    .await
    .expect("seeding lastplayed")
    .await
    .expect("lastplayed ack");
}

async fn read_lastplayed(port: u16, platform: &str) -> Option<(String, i64)> {
    let js = async_nats::jetstream::new(nats_client(port).await);
    let stream = js.get_stream(LASTPLAYED_STREAM).await.ok()?;
    let msg = stream
        .get_last_raw_message_by_subject(&lastplayed_subject(platform))
        .await
        .ok()?;
    let v: serde_json::Value = serde_json::from_slice(&msg.payload).ok()?;
    Some((
        v["file"].as_str()?.to_string(),
        v["position_ms"].as_i64().unwrap_or(0),
    ))
}

async fn publish_command(port: u16, platform: &str, verb: &str, payload: &str) {
    let client = nats_client(port).await;
    client
        .publish(
            format!("tripbot.test.playout.{verb}.{platform}"),
            payload.to_string().into(),
        )
        .await
        .expect("publishing command");
    client.flush().await.expect("flushing command");
}

/// Wait for a lastplayed publish, then for it to change — the playhead the
/// console map reads is moving, not frozen at whatever it booted on.
async fn wait_ticker_advances(port: u16, platform: &str) {
    let deadline = Instant::now() + Duration::from_secs(15);
    let first = loop {
        if let Some(v) = read_lastplayed(port, platform).await {
            break v;
        }
        assert!(Instant::now() < deadline, "no ticker publish within 15s");
        tokio::time::sleep(Duration::from_millis(500)).await;
    };
    assert!(CLIPS.contains(&first.0.as_str()));

    let deadline = Instant::now() + Duration::from_secs(15);
    loop {
        if let Some(next) = read_lastplayed(port, platform).await
            && next != first
        {
            return;
        }
        assert!(
            Instant::now() < deadline,
            "ticker did not advance within 15s"
        );
        tokio::time::sleep(Duration::from_millis(500)).await;
    }
}

/// Every clip in the corpus shows up within a couple of cycles — with 2s clips
/// that holds no matter which clip the cold boot picked. Returns how long it
/// took, so a caller can tell realtime playback from a pipeline racing ahead.
fn wait_all_clips_seen(http: u16, what: &str) -> Duration {
    let started = Instant::now();
    let mut seen = std::collections::HashSet::new();
    wait_for(
        what,
        Duration::from_secs(3 * CLIP_SECONDS * CLIPS.len() as u64),
        || {
            let c = current(http);
            if !c.is_empty() {
                seen.insert(c);
            }
            (seen.len() == CLIPS.len()).then_some(())
        },
    );
    started.elapsed()
}

fn clip_after(name: &str, steps: usize) -> &'static str {
    let i = CLIPS.iter().position(|c| *c == name).expect("known clip");
    CLIPS[(i + steps) % CLIPS.len()]
}

// ---------------------------------------------------------------------------

/// Cold boot publishes to MediaMTX and `/playout/current` serves a corpus
/// basename byte-exact (no trailing newline, no path — tripbot's poller
/// parses the body verbatim).
#[tokio::test]
async fn cold_boot_publishes_and_serves_current() {
    serial_or_skip!();
    let (_nats, nport) = start_nats();
    let (_mtx, mport) = start_mediamtx();
    let p = start_playout(corpus(), Some(nport), mport, "youtube");

    wait_ready(p.http);
    let cur = wait_current(p.http, "a current clip", |c| !c.is_empty());
    assert!(
        CLIPS.contains(&cur.as_str()),
        "current {cur:?} is not a bare corpus basename"
    );
    // The RTSP RECORD handshake can trail readiness by a moment; retry.
    wait_for(
        "MediaMTX path to have a publisher",
        Duration::from_secs(10),
        || describe_ok(&p.rtsp_url).then_some(()),
    );

    // /debug/pipeline dumps the live topology as Graphviz against a real,
    // playing pipeline — so this proves the dot dump works, not just that the
    // route is wired.
    let (status, body) = http_get(p.http, "/debug/pipeline").expect("GET /debug/pipeline");
    assert_eq!(status, 200);
    let dot = String::from_utf8(body).expect("dot is utf-8");
    assert!(
        dot.contains("digraph"),
        "expected a graphviz dump, got {dot:?}"
    );
}

/// Resume from a pre-seeded lastplayed message — the exact scenario that
/// wedged 0.4.0 in prod. Plus the tail-guard and missing-file fallthrough
/// variants.
#[tokio::test]
async fn resume_from_preseeded_lastplayed() {
    serial_or_skip!();
    let (_nats, nport) = start_nats();
    let (_mtx, mport) = start_mediamtx();

    // Mid-clip resume: boots into exactly that clip.
    seed_lastplayed(nport, "youtube", "clip_b.mp4", 1_000).await;
    {
        let p = start_playout(corpus(), Some(nport), mport, "youtube");
        wait_ready(p.http);
        let cur = wait_current(p.http, "resumed clip", |c| !c.is_empty());
        assert_eq!(cur, "clip_b.mp4");
    }

    // Position within 2s of clip end: the tail guard skips the seek but the
    // clip still plays (from the top) — no wedge, no fallthrough to random.
    seed_lastplayed(
        nport,
        "youtube",
        "clip_c.mp4",
        (CLIP_SECONDS * 1000 - 100) as i64,
    )
    .await;
    {
        let p = start_playout(corpus(), Some(nport), mport, "youtube");
        wait_ready(p.http);
        let cur = wait_current(p.http, "tail-guarded clip", |c| !c.is_empty());
        assert_eq!(cur, "clip_c.mp4");
    }

    // Cached file no longer in the corpus: clean no-resume, plays something.
    seed_lastplayed(nport, "youtube", "gone.mp4", 500).await;
    {
        let p = start_playout(corpus(), Some(nport), mport, "youtube");
        wait_ready(p.http);
        let cur = wait_current(p.http, "fallthrough clip", |c| !c.is_empty());
        assert!(CLIPS.contains(&cur.as_str()));
    }
}

/// Each command verb on its real leafed subject changes playback; the other
/// platform's leaf is ignored; edge payloads behave.
#[tokio::test]
async fn commands_act_and_other_platform_is_isolated() {
    serial_or_skip!();
    let (_nats, nport) = start_nats();
    let (_mtx, mport) = start_mediamtx();
    // Long clips: no natural boundary may land mid-assertion, or a "state
    // unchanged" check reads the playlist advancing as a leaked command.
    let p = start_playout(long_corpus(), Some(nport), mport, "youtube");
    wait_ready(p.http);
    wait_current(p.http, "initial clip", |c| !c.is_empty());

    // play.file lands.
    publish_command(nport, "youtube", "play.file", r#"{"file":"clip_c.mp4"}"#).await;
    wait_current(p.http, "play.file target", |c| c == "clip_c.mp4");

    // The twitch leaf must not touch this instance.
    publish_command(nport, "twitch", "play.file", r#"{"file":"clip_a.mp4"}"#).await;
    std::thread::sleep(Duration::from_secs(2));
    assert_eq!(
        current(p.http),
        "clip_c.mp4",
        "foreign-platform command leaked"
    );

    // skip with n<=0 is treated as 1.
    publish_command(nport, "youtube", "skip", r#"{"n":0}"#).await;
    let expected = clip_after("clip_c.mp4", 1);
    wait_current(p.http, "skip target", |c| c == expected);

    // play.file for a file not in the playlist: warned, no state change.
    publish_command(nport, "youtube", "play.file", r#"{"file":"nope.mp4"}"#).await;
    std::thread::sleep(Duration::from_secs(2));
    assert_eq!(current(p.http), expected, "nonexistent file changed state");

    // play.at with an absurd position: seek skipped, clip plays from the top.
    publish_command(
        nport,
        "youtube",
        "play.at",
        r#"{"file":"clip_b.mp4","position_ms":999999999}"#,
    )
    .await;
    wait_current(p.http, "play.at target", |c| c == "clip_b.mp4");

    // back wraps modulo the playlist.
    publish_command(nport, "youtube", "back", r#"{"n":1}"#).await;
    let expected = clip_after("clip_b.mp4", CLIPS.len() - 1);
    wait_current(p.http, "back target", |c| c == expected);

    // seek moves the playhead by a signed span, walking real clip durations.
    // Deltas are 1.5 clips so the landing clip is stable against the few
    // seconds of playback between the play.file and the seek.
    publish_command(nport, "youtube", "play.file", r#"{"file":"clip_a.mp4"}"#).await;
    wait_current(p.http, "seek start", |c| c == "clip_a.mp4");
    publish_command(nport, "youtube", "seek", r#"{"delta_ms":30000}"#).await;
    wait_current(p.http, "seek forward target", |c| c == "clip_b.mp4");

    // A negative delta rewinds, wrapping backward through the playlist.
    publish_command(nport, "youtube", "play.file", r#"{"file":"clip_c.mp4"}"#).await;
    wait_current(p.http, "rewind start", |c| c == "clip_c.mp4");
    publish_command(nport, "youtube", "seek", r#"{"delta_ms":-30000}"#).await;
    wait_current(p.http, "seek backward target", |c| c == "clip_a.mp4");

    // An undecodable payload takes the verb's documented fallback rather than
    // some third behavior: skip still moves one clip, play.file still leaves
    // state alone. Both are warned, so a producer that renames a field leaves
    // a trace instead of a command that quietly stops working.
    publish_command(nport, "youtube", "play.file", r#"{"file":"clip_b.mp4"}"#).await;
    wait_current(p.http, "fallback start", |c| c == "clip_b.mp4");
    publish_command(nport, "youtube", "skip", "not json at all").await;
    let expected = clip_after("clip_b.mp4", 1);
    wait_current(p.http, "undecodable skip target", |c| c == expected);
    publish_command(nport, "youtube", "play.file", r#"{"renamed":"clip_a.mp4"}"#).await;
    std::thread::sleep(Duration::from_secs(2));
    assert_eq!(
        current(p.http),
        expected,
        "undecodable play.file changed state"
    );
}

/// Natural boundaries advance through the playlist and wrap — with 2s clips,
/// every corpus member shows up inside a couple of cycles no matter which clip
/// the cold boot picked.
#[tokio::test]
async fn boundaries_advance_and_wrap() {
    serial_or_skip!();
    let (_nats, nport) = start_nats();
    let (_mtx, mport) = start_mediamtx();
    let p = start_playout(corpus(), Some(nport), mport, "youtube");
    wait_ready(p.http);

    wait_all_clips_seen(p.http, "all clips to play through boundaries");
}

/// The console's chat-map mode: the MediaMTX relay is parked (its Deployment
/// scaled to 0) and playout is expected to stay up map-only — a fakesink in
/// place of the RTSP publish, so the pipeline still reaches PLAYING and the
/// playhead the console map reads keeps advancing while nothing leaves the pod.
///
/// No relay exists here at all, so reaching readiness at all discriminates the
/// modes: had boot wired the encode branch instead, rtspclientsink would have
/// failed to connect and taken the pipeline down.
#[tokio::test]
async fn map_only_plays_and_advances_without_a_relay() {
    serial_or_skip!();
    let (_nats, nport) = start_nats();
    // A port nothing listens on: the boot path probe finds no relay, so boot
    // picks the map-only pipeline.
    let p = start_playout(corpus(), Some(nport), free_port(), "youtube");
    wait_ready(p.http);

    let elapsed = wait_all_clips_seen(p.http, "clips to advance with no relay to publish to");
    wait_ticker_advances(nport, "youtube").await;

    // With no sink pacing the pipeline to the clock, a map-only fakesink
    // consumes the corpus as fast as it decodes and the playhead the console
    // map reads flies across the country. Crossing two clip boundaries can't
    // beat one clip's duration in realtime; without `sync` it takes well under
    // a second.
    assert!(
        elapsed >= Duration::from_secs(CLIP_SECONDS),
        "map-only playback raced through {} clips in {elapsed:?}; the fakesink is not clock-paced",
        CLIPS.len()
    );
}

/// A corrupt clip mid-corpus must not take the pipeline down: the failed
/// clip bin is torn down and playback rolls past it.
#[tokio::test]
async fn corrupt_clip_is_skipped() {
    serial_or_skip!();
    let (_nats, nport) = start_nats();
    let (_mtx, mport) = start_mediamtx();
    let mut p = start_playout(corrupt_corpus(), Some(nport), mport, "youtube");
    wait_ready(p.http);

    let mut seen = std::collections::HashSet::new();
    wait_for(
        "playback to roll past the corrupt clip",
        Duration::from_secs(4 * CLIP_SECONDS * (CLIPS.len() as u64 + 1)),
        || {
            assert!(
                p.proc.0.try_wait().unwrap().is_none(),
                "playout exited on the corrupt clip"
            );
            let c = current(p.http);
            if !c.is_empty() {
                seen.insert(c);
            }
            (seen.len() == CLIPS.len()).then_some(())
        },
    );
}

/// Resume pointing at a corrupt clip must not become a boot crash-loop
/// (restart → resume same clip → crash again): boot absorbs the failure and
/// playback lands on a good clip.
#[tokio::test]
async fn resume_into_corrupt_clip_recovers() {
    serial_or_skip!();
    let (_nats, nport) = start_nats();
    let (_mtx, mport) = start_mediamtx();
    seed_lastplayed(nport, "youtube", "clip_bad.mp4", 500).await;
    let mut p = start_playout(corrupt_corpus(), Some(nport), mport, "youtube");
    wait_ready(p.http);

    let cur = wait_for(
        "playback to land past the corrupt resume clip",
        Duration::from_secs(30),
        || {
            assert!(
                p.proc.0.try_wait().unwrap().is_none(),
                "playout exited resuming into the corrupt clip"
            );
            let c = current(p.http);
            (!c.is_empty() && c != "clip_bad.mp4").then_some(c)
        },
    );
    assert!(CLIPS.contains(&cur.as_str()), "landed on {cur:?}");
}

/// Per-clip recovery is bounded: once consecutive failures outrun the playlist
/// the whole corpus is bad, which is a deployment fault, not a clip to skip.
/// Exit non-zero and let the pod crash-loop where it can be seen — the failure
/// mode this replaces is respawning garbage bins at full tilt forever, which
/// looks like a healthy process and airs nothing.
#[tokio::test]
async fn an_entirely_bad_corpus_gives_up_instead_of_spinning() {
    serial_or_skip!();
    let (_nats, nport) = start_nats();
    let (_mtx, mport) = start_mediamtx();
    let mut p = start_playout(all_bad_corpus(), Some(nport), mport, "youtube");

    // Readiness never comes here, so wait on the exit rather than on /health.
    let status = wait_for("playout to give up", Duration::from_secs(30), || {
        p.proc.0.try_wait().unwrap()
    });
    assert!(
        !status.success(),
        "an unplayable corpus exited {status:?}; k8s would read that as a clean stop"
    );
}

/// A corpus directory that is empty at boot and fills in later: a fresh PVC, or
/// the node-local corpus repopulating after a storage swap (what the 2026-07-06
/// T5 migration looked like from playout's side). The exit itself is the
/// designed behavior — an empty `VIDEO_DIR` is a deployment fault, and a
/// crash-loop beats a healthy-looking pod publishing nothing. What this pins is
/// that the crash-loop **converges on its own** the moment media appears: the
/// restart k8s was already doing goes on air, honors the resume state that was
/// written while the directory was still empty, and needs no manual
/// `play.random` to unstick it.
#[tokio::test]
async fn a_corpus_that_fills_in_later_converges_without_a_nudge() {
    serial_or_skip!();
    let (_nats, nport) = start_nats();
    let (_mtx, mport) = start_mediamtx();
    let dir = std::env::temp_dir().join(format!("playout-parity-late-{}", std::process::id()));
    std::fs::create_dir_all(&dir).unwrap();

    // Resume state predates the media, as it would after any restart that
    // outlives the volume's contents.
    seed_lastplayed(nport, "youtube", CLIPS[2], 500).await;

    let mut empty = start_playout(&dir, Some(nport), mport, "youtube");
    let status = wait_for(
        "playout to bail on the empty corpus",
        Duration::from_secs(30),
        || empty.proc.0.try_wait().unwrap(),
    );
    assert!(
        !status.success(),
        "an empty corpus exited {status:?}; k8s would read that as a clean stop and never restart"
    );
    drop(empty);

    for name in CLIPS {
        std::fs::copy(corpus().join(name), dir.join(name)).unwrap();
    }

    let filled = start_playout(&dir, Some(nport), mport, "youtube");
    wait_ready(filled.http);
    let cur = wait_current(filled.http, "the seeded resume clip", |c| !c.is_empty());
    assert_eq!(
        cur, CLIPS[2],
        "resume state seeded before the media existed was dropped"
    );
    wait_for(
        "MediaMTX path to have a publisher",
        Duration::from_secs(10),
        || describe_ok(&filled.rtsp_url).then_some(()),
    );

    let _ = std::fs::remove_dir_all(&dir);
}

/// The control plane is best-effort: with no NATS reachable playout can't be
/// commanded and can't resume its exact spot, but it must still loop the corpus
/// on air. A boot that waits on NATS forever, or gives up without it, takes the
/// stream down over a dependency that isn't on the playback path.
#[tokio::test]
async fn no_nats_still_plays_the_corpus() {
    serial_or_skip!();
    let (_mtx, mport) = start_mediamtx();
    let p = start_playout(corpus(), None, mport, "youtube");

    // An unreachable NATS costs one deliberate 10s connect window before the
    // first clip spawns — the window buys a resume when NATS is merely slow to
    // come up. Measured at 11.6s to readiness. The budget is deliberately close
    // to that: the stream-ensure and the resume read used to add a guaranteed
    // 10s timeout each on this path, and a budget with room for them to come
    // back would let that regress silently into 30s of dead air per restart.
    wait_for(
        "readiness with no control plane",
        Duration::from_secs(20),
        || matches!(http_get(p.http, "/health/ready"), Some((200, _))).then_some(()),
    );

    wait_all_clips_seen(p.http, "clips to advance with no control plane");
    wait_for(
        "MediaMTX path to have a publisher",
        Duration::from_secs(10),
        || describe_ok(&p.rtsp_url).then_some(()),
    );
}

/// The lastplayed ticker keeps the JetStream last-value cache advancing while
/// playing.
#[tokio::test]
async fn lastplayed_ticker_advances() {
    serial_or_skip!();
    let (_nats, nport) = start_nats();
    let (_mtx, mport) = start_mediamtx();
    let p = start_playout(corpus(), Some(nport), mport, "youtube");
    wait_ready(p.http);

    wait_ticker_advances(nport, "youtube").await;
}

/// ENCODER=passthrough splices the compressed corpus straight to MediaMTX —
/// no decode, no encode. Cold boot publishes, and natural boundaries (the
/// compressed-splice path) advance through every clip.
#[tokio::test]
async fn passthrough_publishes_and_splices_boundaries() {
    serial_or_skip!();
    let (_nats, nport) = start_nats();
    let (_mtx, mport) = start_mediamtx();
    let p = start_playout_with(corpus(), Some(nport), mport, "youtube", "passthrough");
    wait_ready(p.http);

    wait_for(
        "MediaMTX path to have a publisher",
        Duration::from_secs(10),
        || describe_ok(&p.rtsp_url).then_some(()),
    );
    wait_all_clips_seen(p.http, "all clips to splice through passthrough boundaries");
}

/// Passthrough resume seeks a compressed clip via keyframe snapping.
/// Resuming a 20s clip at 10s means its successor appears ~10s in; a
/// silently-demoted seek (from the top) wouldn't hit that boundary until
/// 20s — so the successor inside 16s proves the KEY_UNIT seek took.
#[tokio::test]
async fn passthrough_resume_seeks_to_keyframe() {
    serial_or_skip!();
    let (_nats, nport) = start_nats();
    let (_mtx, mport) = start_mediamtx();
    seed_lastplayed(nport, "youtube", "clip_b.mp4", 10_000).await;
    let p = start_playout_with(long_corpus(), Some(nport), mport, "youtube", "passthrough");
    wait_ready(p.http);

    let cur = wait_current(p.http, "resumed clip", |c| !c.is_empty());
    assert_eq!(cur, "clip_b.mp4");
    wait_for(
        "the successor after the resumed clip's remainder",
        Duration::from_secs(16),
        || (current(p.http) == "clip_c.mp4").then_some(()),
    );
}

/// A rolling deploy's publisher handoff: while one playout holds the MediaMTX
/// path, a second boots map-only but *ready* — waiting instead of kicking the
/// live publisher — and when the first exits, the second acquires the freed
/// path in-process. Readiness-while-waiting is what lets a RollingUpdate
/// SIGTERM the old pod; the acquire poll is what keeps the gap sub-second.
///
/// Passthrough, like the deployed envs: an x264enc attaching mid-run has to
/// encode 1080p60 before the RTSP session can establish, which a CI runner
/// can't do promptly — passthrough ships the already-compressed corpus, so
/// the post-handoff DESCRIBE probes the swap, not the runner's encode speed.
#[tokio::test]
async fn a_second_playout_goes_ready_then_takes_over_when_the_first_exits() {
    serial_or_skip!();
    let (_nats, nport) = start_nats();
    let (_mtx, mport) = start_mediamtx();
    let mut a = start_playout_with(corpus(), Some(nport), mport, "youtube", "passthrough");
    wait_ready(a.http);
    wait_for(
        "the first playout to publish",
        Duration::from_secs(10),
        || describe_ok(&a.rtsp_url).then_some(()),
    );

    // The rollout's incoming pod: same path, currently held.
    let b = start_playout_with(corpus(), Some(nport), mport, "youtube", "passthrough");
    wait_ready(b.http);
    assert!(
        a.proc.0.try_wait().unwrap().is_none(),
        "the first playout exited while the second booted — it was kicked off the path"
    );
    assert!(
        describe_ok(&a.rtsp_url),
        "the path lost its publisher while the second playout waited"
    );

    // k8s SIGTERMs the old pod once the new one is ready; its teardown frees
    // the path.
    let pid = a.proc.0.id().to_string();
    assert!(
        Command::new("kill")
            .args(["-TERM", &pid])
            .status()
            .unwrap()
            .success()
    );
    let status = wait_for("the first playout to exit", Duration::from_secs(10), || {
        a.proc.0.try_wait().unwrap()
    });
    assert!(status.success(), "SIGTERM exit was {status:?}");

    // The second playout acquires the freed path and keeps serving. The
    // budget is runner headroom, not the handoff target — the acquire poll
    // runs at 500ms and lands the swap on its first free probe.
    wait_for(
        "the second playout to acquire the path",
        Duration::from_secs(20),
        || describe_ok(&b.rtsp_url).then_some(()),
    );
    wait_current(b.http, "the second playout's clip", |c| !c.is_empty());
}

/// The published stream must arrive intact: a reader on the relay receives
/// (approximately) every frame the corpus carries. Guards the publish
/// queue's leak bound — a leaky cap below rtspclientsink's steady-state
/// occupancy sheds frames continuously during a healthy session, which
/// every reader decodes as an endless run of reference errors: constant
/// visible artifacts, while the handoff/describe assertions all stay green.
///
/// Passthrough, so this stays inside the harness's no-realtime-encode rule:
/// the corpus frames are pre-compressed and sustaining 1080p60 here is
/// byte-shoveling, not encode throughput.
#[tokio::test]
async fn published_frames_all_reach_a_reader() {
    serial_or_skip!();
    let (_mtx, mport) = start_mediamtx();
    let p = start_playout_with(corpus(), None, mport, "youtube", "passthrough");
    wait_ready(p.http);
    wait_for("the publish to establish", Duration::from_secs(10), || {
        describe_ok(&p.rtsp_url).then_some(())
    });

    use gst::prelude::*;
    use gstreamer as gst;
    use std::sync::Arc;
    use std::sync::atomic::{AtomicU64, Ordering};

    gst::init().unwrap();
    let reader = gst::parse::launch(&format!(
        "rtspsrc location={} protocols=tcp latency=200 \
         ! rtph264depay ! h264parse ! fakesink name=sink sync=false",
        p.rtsp_url
    ))
    .unwrap()
    .downcast::<gst::Pipeline>()
    .unwrap();
    let frames = Arc::new(AtomicU64::new(0));
    let counter = Arc::clone(&frames);
    reader
        .by_name("sink")
        .unwrap()
        .static_pad("sink")
        .unwrap()
        .add_probe(gst::PadProbeType::BUFFER, move |_, _| {
            counter.fetch_add(1, Ordering::Relaxed);
            gst::PadProbeReturn::Ok
        });
    reader.set_state(gst::State::Playing).unwrap();
    wait_for(
        "the first frame at the reader",
        Duration::from_secs(10),
        || (frames.load(Ordering::Relaxed) > 0).then_some(()),
    );

    let start = frames.load(Ordering::Relaxed);
    tokio::time::sleep(Duration::from_secs(10)).await;
    let got = frames.load(Ordering::Relaxed) - start;
    reader.set_state(gst::State::Null).ok();

    // 10s of the 60fps corpus is 600 frames. 85% leaves room for runner
    // jitter; the failure this guards is a steady shed (~50% received).
    assert!(
        got >= 510,
        "reader received {got} of ~600 frames in 10s — the publish path is shedding frames"
    );
}

/// SIGTERM exits zero after a clean teardown.
#[tokio::test]
async fn sigterm_exits_clean() {
    serial_or_skip!();
    let (_nats, nport) = start_nats();
    let (_mtx, mport) = start_mediamtx();
    let mut p = start_playout(corpus(), Some(nport), mport, "youtube");
    wait_ready(p.http);

    let pid = p.proc.0.id().to_string();
    assert!(
        Command::new("kill")
            .args(["-TERM", &pid])
            .status()
            .unwrap()
            .success()
    );
    let status = wait_for("clean exit", Duration::from_secs(10), || {
        p.proc.0.try_wait().unwrap()
    });
    assert!(status.success(), "SIGTERM exit was {status:?}");
}
