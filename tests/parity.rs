//! Behavioral-parity harness: boots the real binary against a real MediaMTX
//! and NATS (JetStream) and asserts behavior over HTTP, NATS, and RTSP — no
//! mocks, the same wire tripbot speaks. Realtime throughput is explicitly
//! out of scope (CI runners can't sustain 1080p60); these tests assert
//! *behavior*: publish-on-boot, resume, commands, boundaries, shutdown.
//!
//! Requires `mediamtx`, `nats-server`, and `gst-launch-1.0` on PATH; each
//! test skips (passing) when they're missing, so plain `cargo test` still
//! works on a machine without them. CI installs all three.

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
        if !tools_available() {
            eprintln!("skipping: mediamtx / nats-server / gst-launch-1.0 not all on PATH");
            return;
        }
    };
}

fn tools_available() -> bool {
    static AVAILABLE: OnceLock<bool> = OnceLock::new();
    *AVAILABLE.get_or_init(|| {
        ["mediamtx", "nats-server", "gst-launch-1.0"]
            .iter()
            .all(|bin| {
                Command::new(bin)
                    .arg("--version")
                    .stdout(Stdio::null())
                    .stderr(Stdio::null())
                    .status()
                    .is_ok()
            })
    })
}

fn free_port() -> u16 {
    TcpListener::bind("127.0.0.1:0")
        .unwrap()
        .local_addr()
        .unwrap()
        .port()
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
/// byte-exactness assertions (`/vlc/current` must be basename-only, no
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
    let (status, body) = http_get(port, "/vlc/current").unwrap_or((0, Vec::new()));
    assert!(status == 200 || status == 0, "GET /vlc/current -> {status}");
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

fn lastplayed_subject(platform: &str) -> String {
    format!("tripbot.test.vlc.lastplayed.{platform}")
}

async fn nats_client(port: u16) -> async_nats::Client {
    async_nats::connect(format!("nats://127.0.0.1:{port}"))
        .await
        .expect("connecting test nats client")
}

async fn seed_lastplayed(port: u16, platform: &str, file: &str, position_ms: i64) {
    let js = async_nats::jetstream::new(nats_client(port).await);
    js.create_stream(async_nats::jetstream::stream::Config {
        name: "TRIPBOT_VLC_LASTPLAYED".to_string(),
        subjects: vec!["tripbot.test.vlc.lastplayed.*".to_string()],
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
    let stream = js.get_stream("TRIPBOT_VLC_LASTPLAYED").await.ok()?;
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
            format!("tripbot.test.vlc.{verb}.{platform}"),
            payload.to_string().into(),
        )
        .await
        .expect("publishing command");
    client.flush().await.expect("flushing command");
}

fn clip_after(name: &str, steps: usize) -> &'static str {
    let i = CLIPS.iter().position(|c| *c == name).expect("known clip");
    CLIPS[(i + steps) % CLIPS.len()]
}

// ---------------------------------------------------------------------------

/// Parity test 1: cold boot publishes to MediaMTX and `/vlc/current` serves a
/// corpus basename byte-exact (no trailing newline, no path — tripbot's
/// poller parses the body verbatim).
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

/// Parity test 2: resume from a pre-seeded lastplayed message — the exact
/// scenario that wedged 0.4.0 in prod. Plus the tail-guard and missing-file
/// fallthrough variants.
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

/// Parity test 3: each command verb on its real leafed subject changes
/// playback; the other platform's leaf is ignored; edge payloads behave.
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
}

/// Parity test 4: natural boundaries advance through the playlist and wrap —
/// with 2s clips, every corpus member shows up inside a couple of cycles no
/// matter which clip the cold boot picked.
#[tokio::test]
async fn boundaries_advance_and_wrap() {
    serial_or_skip!();
    let (_nats, nport) = start_nats();
    let (_mtx, mport) = start_mediamtx();
    let p = start_playout(corpus(), Some(nport), mport, "youtube");
    wait_ready(p.http);

    let mut seen = std::collections::HashSet::new();
    wait_for(
        "all clips to play through boundaries",
        Duration::from_secs(3 * CLIP_SECONDS * CLIPS.len() as u64),
        || {
            let c = current(p.http);
            if !c.is_empty() {
                seen.insert(c);
            }
            (seen.len() == CLIPS.len()).then_some(())
        },
    );
}

/// Parity test 5: a corrupt clip mid-corpus must not take the pipeline down —
/// the failed clip bin is torn down and playback rolls past it, like
/// vlc-server does with bad files.
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

/// Parity test 6: the lastplayed ticker keeps the JetStream last-value cache
/// advancing while playing.
#[tokio::test]
async fn lastplayed_ticker_advances() {
    serial_or_skip!();
    let (_nats, nport) = start_nats();
    let (_mtx, mport) = start_mediamtx();
    let p = start_playout(corpus(), Some(nport), mport, "youtube");
    wait_ready(p.http);

    let deadline = Instant::now() + Duration::from_secs(15);
    let first = loop {
        if let Some(v) = read_lastplayed(nport, "youtube").await {
            break v;
        }
        assert!(Instant::now() < deadline, "no ticker publish within 15s");
        tokio::time::sleep(Duration::from_millis(500)).await;
    };
    assert!(CLIPS.contains(&first.0.as_str()));

    let deadline = Instant::now() + Duration::from_secs(15);
    loop {
        if let Some(next) = read_lastplayed(nport, "youtube").await
            && next != first
        {
            break;
        }
        assert!(
            Instant::now() < deadline,
            "ticker did not advance within 15s"
        );
        tokio::time::sleep(Duration::from_millis(500)).await;
    }
}

/// Passthrough 1: ENCODER=passthrough splices the compressed corpus straight
/// to MediaMTX — no decode, no encode. Cold boot publishes, and natural
/// boundaries (the compressed-splice path) advance through every clip.
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
    let mut seen = std::collections::HashSet::new();
    wait_for(
        "all clips to splice through passthrough boundaries",
        Duration::from_secs(3 * CLIP_SECONDS * CLIPS.len() as u64),
        || {
            let c = current(p.http);
            if !c.is_empty() {
                seen.insert(c);
            }
            (seen.len() == CLIPS.len()).then_some(())
        },
    );
}

/// Passthrough 2: resume seeks a compressed clip via keyframe snapping.
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

/// Parity test 7: SIGTERM exits zero after a clean teardown (shipped in #20;
/// this keeps it true).
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
