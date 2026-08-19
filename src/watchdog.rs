//! RTSP publish watchdog: DESCRIBE-probe the MediaMTX path we publish to and
//! die loudly when it stops answering. The pipeline can sit in PLAYING with a
//! dead publish (rtspclientsink in RECORD mode never proves data flow), so
//! /health/ready alone misses exactly this failure.
//! Exit non-zero and let k8s restart the pod; resume comes from JetStream.

use anyhow::{Context, Result, bail};
use std::future::Future;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Duration;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tracing::{error, info, warn};

// ponytail: constants, not env knobs — matches vlc-server's proven values;
// make them configurable if a deployment ever needs different pacing.
const PROBE_TIMEOUT: Duration = Duration::from_secs(3);
const INTERVAL: Duration = Duration::from_secs(30);
/// Covers cold boot: MediaMTX answers DESCRIBE 404 until our first publish
/// lands, which takes a few seconds of preroll.
const INITIAL_DELAY: Duration = Duration::from_secs(30);
const FAILURE_THRESHOLD: u32 = 3;

/// host:port to dial for an rtsp:// url, defaulting to the RTSP port when the
/// url omits one. Shared by the DESCRIBE probe and the plain reachability check.
fn relay_addr(url: &str) -> Result<String> {
    let authority = url
        .strip_prefix("rtsp://")
        .with_context(|| format!("not an rtsp:// url: {url}"))?;
    let (hostport, _) = authority.split_once('/').unwrap_or((authority, ""));
    Ok(if hostport.contains(':') {
        hostport.to_string()
    } else {
        format!("{hostport}:554")
    })
}

/// One RTSP DESCRIBE against `url`, resolving to the response's status line.
/// Err means the relay didn't answer RTSP at all — dead, parked (a scaled-to-0
/// Deployment has no Service endpoints), or not speaking the protocol.
async fn describe_status(url: &str) -> Result<String> {
    let addr = relay_addr(url)?;

    let probe = async {
        let mut conn = tokio::net::TcpStream::connect(&addr)
            .await
            .with_context(|| format!("dial {addr}"))?;
        conn.write_all(
            format!("DESCRIBE {url} RTSP/1.0\r\nCSeq: 1\r\nAccept: application/sdp\r\n\r\n")
                .as_bytes(),
        )
        .await
        .context("write DESCRIBE")?;
        // Read until the status line is complete; the response may span reads.
        let mut buf = Vec::with_capacity(256);
        let mut chunk = [0u8; 256];
        while !buf.contains(&b'\n') {
            let n = conn.read(&mut chunk).await.context("read status")?;
            if n == 0 {
                bail!("connection closed before status line");
            }
            buf.extend_from_slice(&chunk[..n]);
        }
        let line = String::from_utf8_lossy(&buf);
        Ok(line.lines().next().unwrap_or_default().to_string())
    };
    tokio::time::timeout(PROBE_TIMEOUT, probe)
        .await
        .map_err(|_| anyhow::anyhow!("DESCRIBE timed out after {PROBE_TIMEOUT:?}"))?
}

/// One RTSP DESCRIBE against `url`, ok iff the server answers 200. MediaMTX
/// only DESCRIBEs a path OK while it has a live publisher, so a 404/5xx here
/// means our publish is gone even if the TCP port still accepts.
async fn describe(url: &str) -> Result<()> {
    let status = describe_status(url).await?;
    if !status.starts_with("RTSP/1.0 200") {
        bail!("unexpected status: {status}");
    }
    Ok(())
}

/// True when the relay answers RTSP but no publisher holds the path (MediaMTX
/// DESCRIBEs 404 until one attaches) — the only state where an RTSP RECORD
/// can attach without kicking anyone. A dead or parked relay is not "free":
/// publishing there would just fail.
pub(crate) async fn path_free(url: &str) -> bool {
    matches!(describe_status(url).await, Ok(s) if !s.starts_with("RTSP/1.0 200"))
}

/// Probe every `INTERVAL`; after `FAILURE_THRESHOLD` consecutive failures,
/// log and invoke `on_dead` (which flags failure and quits the main loop, so
/// the process exits non-zero through the normal teardown path).
///
/// Self-gates on `publishing`: while the publish slot is on its fakesink the
/// path is legitimately publisher-less, so those probes count as healthy.
pub async fn run(
    rtsp_url: String,
    publishing: Arc<AtomicBool>,
    on_dead: impl Fn() + Send + 'static,
) {
    info!(
        url = %rtsp_url,
        interval_s = INTERVAL.as_secs(),
        threshold = FAILURE_THRESHOLD,
        "starting RTSP watchdog"
    );
    // Each probe gets its own owned copy of the url so the future it returns
    // borrows nothing from the closure — `run` is spawned, and a probe future
    // that borrows its closure isn't Send.
    watch(
        INITIAL_DELAY,
        INTERVAL,
        move || {
            let url = rtsp_url.clone();
            let publishing = publishing.clone();
            async move {
                if !publishing.load(Ordering::SeqCst) {
                    return Ok(());
                }
                describe(&url).await
            }
        },
        on_dead,
    )
    .await;
}

/// The watchdog's decision loop, over any probe: wait out `initial`, then probe
/// every `interval`, and invoke `on_dead` (and return) once `FAILURE_THRESHOLD`
/// probes have failed back to back. A success resets the count — transient
/// blips must not accumulate into a restart across hours of healthy probes.
///
/// Taking the probe as a parameter is what makes the counting testable without
/// a relay: the tests drive it with scripted outcomes on tokio's virtual clock.
async fn watch<Fut: Future<Output = Result<()>>>(
    initial: Duration,
    interval: Duration,
    mut probe: impl FnMut() -> Fut,
    on_dead: impl Fn(),
) {
    tokio::time::sleep(initial).await;
    let mut consecutive = 0u32;
    loop {
        match probe().await {
            Ok(()) => {
                if consecutive > 0 {
                    info!(after_failures = consecutive, "RTSP DESCRIBE recovered");
                }
                consecutive = 0;
            }
            Err(e) => {
                consecutive += 1;
                warn!(
                    err = %e,
                    consecutive,
                    threshold = FAILURE_THRESHOLD,
                    "RTSP DESCRIBE failed"
                );
                if consecutive >= FAILURE_THRESHOLD {
                    error!("RTSP publish dead; exiting for a clean restart");
                    on_dead();
                    return;
                }
            }
        }
        tokio::time::sleep(interval).await;
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::cell::Cell;
    use tokio::io::AsyncWriteExt;

    #[test]
    fn relay_addr_defaults_port_and_keeps_explicit() {
        assert_eq!(
            relay_addr("rtsp://mediamtx-facebook:8554/dashcam").unwrap(),
            "mediamtx-facebook:8554"
        );
        // No port in the authority → the RTSP default.
        assert_eq!(relay_addr("rtsp://host/dashcam").unwrap(), "host:554");
        // No path at all.
        assert_eq!(relay_addr("rtsp://host:8554").unwrap(), "host:8554");
        assert!(relay_addr("http://host/dashcam").is_err());
    }

    async fn serve_one(status: &'static str) -> String {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        tokio::spawn(async move {
            let (mut sock, _) = listener.accept().await.unwrap();
            let mut buf = [0u8; 512];
            let _ = tokio::io::AsyncReadExt::read(&mut sock, &mut buf).await;
            sock.write_all(status.as_bytes()).await.unwrap();
        });
        format!("rtsp://{addr}/dashcam")
    }

    #[tokio::test]
    async fn describe_accepts_200_rejects_404() {
        let ok = serve_one("RTSP/1.0 200 OK\r\nCSeq: 1\r\n\r\n").await;
        assert!(describe(&ok).await.is_ok());

        let dead = serve_one("RTSP/1.0 404 Not Found\r\nCSeq: 1\r\n\r\n").await;
        let err = describe(&dead).await.unwrap_err();
        assert!(err.to_string().contains("404"));
    }

    /// The acquirer's tri-state, collapsed to "may I publish": a held path
    /// (200) and a dead relay both say no; only an answering relay with no
    /// publisher says yes. Wrong in the 200 arm and a rolling deploy kicks
    /// the live publisher; wrong in the dead arm and boot wires a publish
    /// that instantly fails.
    #[tokio::test]
    async fn path_free_only_when_the_relay_answers_without_a_publisher() {
        let held = serve_one("RTSP/1.0 200 OK\r\nCSeq: 1\r\n\r\n").await;
        assert!(!path_free(&held).await);

        let free = serve_one("RTSP/1.0 404 Not Found\r\nCSeq: 1\r\n\r\n").await;
        assert!(path_free(&free).await);

        // A bound-then-dropped port: nothing answers there.
        let gone = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = gone.local_addr().unwrap();
        drop(gone);
        assert!(!path_free(&format!("rtsp://{addr}/dashcam")).await);
    }

    /// Drives `watch` with scripted probe outcomes (`true` = healthy), falling
    /// back to healthy once the script runs out. Returns (probe count, whether
    /// the watchdog declared the publish dead). `start_paused` runs the callers
    /// on tokio's virtual clock, so the 30s delay and intervals cost no wall
    /// time; `cap_intervals` bounds a watchdog that correctly never fires.
    async fn drive(outcomes: &[bool], cap_intervals: u32) -> (usize, bool) {
        let calls = Cell::new(0usize);
        let died = Cell::new(false);
        // Shared references are Copy, so the probe future carries one of its
        // own instead of borrowing the closure it came from.
        let counter = &calls;
        let loop_fut = watch(
            INITIAL_DELAY,
            INTERVAL,
            move || async move {
                let i = counter.get();
                counter.set(i + 1);
                match outcomes.get(i).copied().unwrap_or(true) {
                    true => Ok(()),
                    false => bail!("scripted probe failure"),
                }
            },
            || died.set(true),
        );
        let _ = tokio::time::timeout(INITIAL_DELAY + INTERVAL * cap_intervals, loop_fut).await;
        (calls.get(), died.get())
    }

    #[tokio::test(start_paused = true)]
    async fn dies_on_exactly_the_threshold_of_consecutive_failures() {
        let (probes, died) = drive(&[false, false, false], 10).await;
        assert!(died, "watchdog never declared the publish dead");
        // Exactly the threshold, not one probe more or fewer: firing early
        // restarts the pod on a blip, firing late leaves a dead publish airing.
        assert_eq!(probes, FAILURE_THRESHOLD as usize);
    }

    #[tokio::test(start_paused = true)]
    async fn holds_when_failures_are_not_consecutive() {
        // Two failures, a recovery, two more — never three back to back, so a
        // watchdog that resets on success must ride it out indefinitely.
        let (probes, died) = drive(&[false, false, true, false, false], 40).await;
        assert!(
            !died,
            "fired on non-consecutive failures after {probes} probes"
        );
        assert!(probes > 5, "stopped probing after {probes}");
    }

    #[tokio::test(start_paused = true)]
    async fn the_initial_delay_holds_off_the_first_probe() {
        // Cold boot: MediaMTX answers DESCRIBE 404 until our first publish
        // lands, so probing before the delay would kill every single boot.
        let calls = Cell::new(0usize);
        let counter = &calls;
        let loop_fut = watch(
            INITIAL_DELAY,
            INTERVAL,
            move || async move {
                counter.set(counter.get() + 1);
                bail!("would fail if probed")
            },
            || {},
        );
        let _ = tokio::time::timeout(INITIAL_DELAY - Duration::from_secs(1), loop_fut).await;
        assert_eq!(calls.get(), 0, "probed before the initial delay elapsed");
    }
}
