use crate::SharedPlayer;
use axum::{
    Router,
    extract::{Path, Query, Request, State},
    http::{StatusCode, header},
    response::{IntoResponse, Response},
    routing::get,
};
use gst::prelude::*;
use gstreamer as gst;
use serde::Deserialize;
use std::net::SocketAddr;
use std::path::PathBuf;
use tower::ServiceExt;
use tower_http::services::ServeFile;
use tracing::warn;

#[derive(Deserialize)]
struct FrameQuery {
    /// Seconds into the clip; the top of it when absent.
    #[serde(default)]
    at: f64,
}

/// Grabs decode on the blocking pool beside the live encoder, so two at once
/// is the ceiling: the console's clip page is one operator's click, and a
/// burst past that waits rather than starving the stream of CPU.
// ponytail: a fixed cap of 2; make it a setting if a second consumer appears.
static GRABS: tokio::sync::Semaphore = tokio::sync::Semaphore::const_new(2);

/// The file a clip name stands for: its playlist entry, else a file of that
/// name in one of the parked directories. The name must be a bare file name
/// — anything with a path in it resolves to nothing — so there is no path to
/// traverse. Stats the parked directories, so call it off the async runtime:
/// they are an NFS mount, and a stat on a dead one can take its timeout.
pub(crate) fn locate(files: &[PathBuf], parked_dirs: &[PathBuf], name: &str) -> Option<PathBuf> {
    if let Some(p) = files
        .iter()
        .find(|p| p.file_name().and_then(|n| n.to_str()) == Some(name))
    {
        return Some(p.clone());
    }
    if std::path::Path::new(name)
        .file_name()
        .and_then(|n| n.to_str())
        != Some(name)
    {
        return None;
    }
    parked_dirs
        .iter()
        .map(|d| d.join(name))
        .find(|p| p.is_file())
}

/// `locate` for a request: the player's playlist and parked directories, on
/// the blocking pool.
async fn locate_for(player: &SharedPlayer, name: String) -> Option<PathBuf> {
    let player = player.clone();
    tokio::task::spawn_blocking(move || locate(&player.files, &player.parked_dirs, &name))
        .await
        .ok()
        .flatten()
}

/// One frame of a clip as a JPEG, for the console's clip page. The clip is
/// named by basename, the way `play.at` names it, and resolved against the
/// playlist and then the parked directories (see `locate`) — an unknown
/// name is a 404. The corpus is immutable, so the picture may be cached for
/// a week.
async fn frame(
    State(player): State<SharedPlayer>,
    Path(name): Path<String>,
    Query(q): Query<FrameQuery>,
) -> Response {
    let Some(path) = locate_for(&player, name.clone()).await else {
        return (StatusCode::NOT_FOUND, "no such clip").into_response();
    };
    let at_ms = (q.at.max(0.0) * 1000.0) as u64;
    let _slot = GRABS.acquire().await.expect("semaphore never closes");
    let grab = tokio::task::spawn_blocking(move || crate::frame::grab(&path, at_ms));
    match tokio::time::timeout(crate::frame::BUDGET, grab).await {
        Ok(Ok(Ok(jpeg))) => (
            [
                (header::CONTENT_TYPE, "image/jpeg"),
                (header::CACHE_CONTROL, "public, max-age=604800"),
            ],
            jpeg,
        )
            .into_response(),
        Ok(Ok(Err(err))) => {
            warn!(clip = %name, at_ms, err = %format!("{err:#}"), "frame grab failed");
            (StatusCode::INTERNAL_SERVER_ERROR, "frame grab failed").into_response()
        }
        Ok(Err(join)) => {
            warn!(clip = %name, at_ms, err = %join, "frame grab panicked");
            (StatusCode::INTERNAL_SERVER_ERROR, "frame grab failed").into_response()
        }
        Err(_) => {
            warn!(clip = %name, at_ms, "frame grab timed out");
            (StatusCode::GATEWAY_TIMEOUT, "frame grab timed out").into_response()
        }
    }
}

/// A playlist clip's bytes, for the console's clip page to play. Named and
/// resolved like `/frame`, so an unknown name is a 404 and there is no path to
/// traverse. Range requests are honoured, so a browser seeking to the moment
/// the page names reads only the bytes it plays — no decode, no encode, just
/// the file off the mount.
async fn clip(
    State(player): State<SharedPlayer>,
    Path(name): Path<String>,
    req: Request,
) -> Response {
    let Some(path) = locate_for(&player, name).await else {
        return (StatusCode::NOT_FOUND, "no such clip").into_response();
    };
    clip_response(&path, req).await
}

async fn clip_response(path: &std::path::Path, req: Request) -> Response {
    // The extension is upper-case on most of the corpus, so the type is named
    // rather than guessed.
    let file = ServeFile::new_with_mime(path, &"video/mp4".parse().expect("valid mime"));
    let mut resp = file.oneshot(req).await.into_response();
    resp.headers_mut().insert(
        header::CACHE_CONTROL,
        header::HeaderValue::from_static("public, max-age=604800"),
    );
    resp
}

/// Process-is-alive signal: if this handler runs at all, the answer is yes.
/// Deeper checks belong on /health/ready — a stuck pipeline should fail
/// readiness, not get the process restarted.
async fn live() -> impl IntoResponse {
    (StatusCode::OK, "OK")
}

/// Ready only while the pipeline is PLAYING; 503 with the reason otherwise,
/// so k8s readiness probes pull the pod out of rotation while it recovers.
async fn ready(State(player): State<SharedPlayer>) -> impl IntoResponse {
    ready_response(player.pipeline.current_state())
}

fn ready_response(state: gst::State) -> (StatusCode, String) {
    if state == gst::State::Playing {
        (StatusCode::OK, "OK".to_string())
    } else {
        (
            StatusCode::SERVICE_UNAVAILABLE,
            format!("pipeline not playing (state {state:?})"),
        )
    }
}

fn version_json() -> serde_json::Value {
    serde_json::json!({
        "tag": crate::VERSION,
        "sha": crate::SHA,
        "built_at": crate::BUILT_AT,
    })
}

/// Fleet-wide version-discovery contract: build tag, git sha, and RFC3339
/// build timestamp, as stamped into the binary at build time.
async fn version() -> impl IntoResponse {
    axum::Json(version_json())
}

/// Plain-text basename of the current clip — read verbatim by tripbot's
/// playout-client, so no path, no trailing newline. Empty string when idle.
async fn current(State(player): State<SharedPlayer>) -> String {
    player.current_basename().unwrap_or_default()
}

/// Live pipeline topology as a Graphviz .dot: every element, pad, and
/// negotiated caps at this instant. Pipe to `dot -Tsvg` or drop into
/// gst-dots-viewer to eyeball the passthrough-vs-encode wiring on a running
/// pod. Empty when GStreamer was built without the debug system.
async fn pipeline_dot(State(player): State<SharedPlayer>) -> impl IntoResponse {
    let dot = player
        .pipeline
        .debug_to_dot_data(gst::DebugGraphDetails::ALL);
    (
        StatusCode::OK,
        [(axum::http::header::CONTENT_TYPE, "text/vnd.graphviz")],
        dot.to_string(),
    )
}

pub async fn run(player: SharedPlayer) {
    let port = std::env::var("HTTP_PORT")
        .unwrap_or_else(|_| "8080".to_string())
        .parse()
        .expect("HTTP_PORT must be a number");

    let app = Router::new()
        .route("/health/live", get(live))
        .route("/health/ready", get(ready))
        .route("/version", get(version))
        .route("/playout/current", get(current))
        .route("/debug/pipeline", get(pipeline_dot))
        .route("/frame/{name}", get(frame))
        .route("/clip/{name}", get(clip))
        .with_state(player);

    let addr = SocketAddr::from(([0, 0, 0, 0], port));
    let listener = tokio::net::TcpListener::bind(addr).await.unwrap();
    axum::serve(listener, app).await.unwrap();
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn ready_maps_pipeline_state_to_status() {
        let (status, body) = ready_response(gst::State::Playing);
        assert_eq!(status, StatusCode::OK);
        assert_eq!(body, "OK");

        for state in [gst::State::Null, gst::State::Ready, gst::State::Paused] {
            let (status, _) = ready_response(state);
            assert_eq!(status, StatusCode::SERVICE_UNAVAILABLE);
        }
    }

    #[tokio::test]
    async fn clip_honours_a_range_request() {
        let dir = std::env::temp_dir().join(format!("playout-clip-{}", std::process::id()));
        std::fs::create_dir_all(&dir).unwrap();
        let path = dir.join("a.MP4");
        std::fs::write(&path, b"0123456789").unwrap();

        let req = Request::builder()
            .header(header::RANGE, "bytes=2-5")
            .body(axum::body::Body::empty())
            .unwrap();
        let resp = clip_response(&path, req).await;
        assert_eq!(resp.status(), StatusCode::PARTIAL_CONTENT);
        assert_eq!(resp.headers()[header::CONTENT_TYPE], "video/mp4");
        assert_eq!(resp.headers()[header::CONTENT_RANGE], "bytes 2-5/10");
        let body = axum::body::to_bytes(resp.into_body(), 64).await.unwrap();
        assert_eq!(&body[..], b"2345");
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn locate_prefers_the_playlist_then_falls_back_to_a_parked_dir() {
        let dir = std::env::temp_dir().join(format!("playout-locate-{}", std::process::id()));
        std::fs::create_dir_all(&dir).unwrap();
        std::fs::write(dir.join("new.MP4"), b"x").unwrap();
        let files = vec![PathBuf::from("/corpus/aired.MP4")];
        let parked = vec![dir.clone()];
        assert_eq!(
            locate(&files, &parked, "aired.MP4"),
            Some(PathBuf::from("/corpus/aired.MP4"))
        );
        assert_eq!(
            locate(&files, &parked, "new.MP4"),
            Some(dir.join("new.MP4"))
        );
        assert_eq!(locate(&files, &parked, "missing.MP4"), None);
        // No path in a name: neither a parent nor a sibling directory resolves.
        assert_eq!(locate(&files, &parked, ".."), None);
        assert_eq!(locate(&files, &parked, "../new.MP4"), None);
        assert_eq!(locate(&files, &[], "new.MP4"), None);
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn version_json_matches_contract() {
        let v = version_json();
        // The key names are the contract — the fleet's version discovery reads
        // these three and nothing else, so a rename here is a breaking change
        // even though every value would still be non-empty.
        let obj = v.as_object().expect("/version is a JSON object");
        let mut keys: Vec<&str> = obj.keys().map(String::as_str).collect();
        keys.sort_unstable();
        assert_eq!(keys, ["built_at", "sha", "tag"]);
        // Every field is a non-empty string in every build, release or local:
        // the consts fall back to the crate version and "unknown".
        for key in keys {
            assert!(
                v[key].as_str().is_some_and(|s| !s.is_empty()),
                "{key} is not a non-empty string"
            );
        }
    }
}
