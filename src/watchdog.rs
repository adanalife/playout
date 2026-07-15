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

/// One RTSP DESCRIBE against `url`, ok iff the server answers 200. MediaMTX
/// only DESCRIBEs a path OK while it has a live publisher, so a 404/5xx here
/// means our publish is gone even if the TCP port still accepts.
async fn describe(url: &str) -> Result<()> {
    let authority = url
        .strip_prefix("rtsp://")
        .with_context(|| format!("not an rtsp:// url: {url}"))?;
    let (hostport, _) = authority.split_once('/').unwrap_or((authority, ""));
    let addr = if hostport.contains(':') {
        hostport.to_string()
    } else {
        format!("{hostport}:554")
    };

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

#[cfg(test)]
mod tests {
    use super::*;
    use tokio::io::AsyncWriteExt;

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
