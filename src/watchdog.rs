//! RTSP publish watchdog: DESCRIBE-probe the MediaMTX path we publish to and
//! die loudly when it stops answering — vlc-server parity. The pipeline can
//! sit in PLAYING with a dead publish (rtspclientsink in RECORD mode never
//! proves data flow), so /health/ready alone misses exactly this failure.
//! Exit non-zero and let k8s restart the pod; resume comes from JetStream.

use anyhow::{Context, Result, bail};
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

/// Is the MediaMTX relay listening? A plain TCP connect to the publish
/// endpoint, bounded by `PROBE_TIMEOUT`. Unlike `describe`, this sends no RTSP
/// request — MediaMTX DESCRIBEs a path 404 until a publisher attaches, so
/// *before* we publish, "the port accepts a connection" is the honest signal
/// that the relay pod is up. A parked relay (its Deployment scaled to 0) has no
/// Service endpoints, so the connect is refused or times out.
pub async fn relay_up(rtsp_url: &str) -> bool {
    let Ok(addr) = relay_addr(rtsp_url) else {
        return false;
    };
    matches!(
        tokio::time::timeout(PROBE_TIMEOUT, tokio::net::TcpStream::connect(&addr)).await,
        Ok(Ok(_))
    )
}

/// One RTSP DESCRIBE against `url`, ok iff the server answers 200. MediaMTX
/// only DESCRIBEs a path OK while it has a live publisher, so a 404/5xx here
/// means our publish is gone even if the TCP port still accepts.
async fn describe(url: &str) -> Result<()> {
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
        let status = line.lines().next().unwrap_or_default();
        if !status.starts_with("RTSP/1.0 200") {
            bail!("unexpected status: {status}");
        }
        Ok(())
    };
    tokio::time::timeout(PROBE_TIMEOUT, probe)
        .await
        .map_err(|_| anyhow::anyhow!("DESCRIBE timed out after {PROBE_TIMEOUT:?}"))?
}

/// Probe every `INTERVAL`; after `FAILURE_THRESHOLD` consecutive failures,
/// log and invoke `on_dead` (which flags failure and quits the main loop, so
/// the process exits non-zero through the normal teardown path).
pub async fn run(rtsp_url: String, on_dead: impl Fn() + Send + 'static) {
    info!(
        url = %rtsp_url,
        interval_s = INTERVAL.as_secs(),
        threshold = FAILURE_THRESHOLD,
        "starting RTSP watchdog"
    );
    tokio::time::sleep(INITIAL_DELAY).await;
    let mut consecutive = 0u32;
    loop {
        match describe(&rtsp_url).await {
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
        tokio::time::sleep(INTERVAL).await;
    }
}

/// The map-only counterpart to `run`: playout built a no-broadcast pipeline
/// because the relay was parked, so watch for it coming back and, when it does,
/// invoke `on_up` (a clean exit) so the restart rebuilds with the RTSP publish
/// wired in. Probes on the same `INTERVAL`; a single successful connect is
/// enough — a listening relay is unambiguous, and the publish path's own errors
/// guard the reverse direction.
pub async fn run_reappear(rtsp_url: String, on_up: impl Fn() + Send + 'static) {
    info!(
        url = %rtsp_url,
        interval_s = INTERVAL.as_secs(),
        "watching for the relay to return (map-only mode)"
    );
    loop {
        tokio::time::sleep(INTERVAL).await;
        if relay_up(&rtsp_url).await {
            info!("relay is back; restarting to resume broadcast");
            on_up();
            return;
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
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

    #[tokio::test]
    async fn relay_up_true_when_listening_false_when_not() {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        assert!(relay_up(&format!("rtsp://{addr}/dashcam")).await);

        // Drop the listener, then a fresh bind gives us a port nothing answers.
        drop(listener);
        let dead = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let dead_addr = dead.local_addr().unwrap();
        drop(dead);
        assert!(!relay_up(&format!("rtsp://{dead_addr}/dashcam")).await);
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
}
