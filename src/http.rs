use crate::SharedPlayer;
use axum::{
    Router,
    extract::{Path, Query, State},
    http::{StatusCode, header},
    response::{IntoResponse, Response},
    routing::get,
};
use gst::prelude::*;
use gstreamer as gst;
use serde::Deserialize;
use std::net::SocketAddr;
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

/// One frame of a playlist clip as a JPEG, for the console's clip page. The
/// clip is named by basename, the way `play.at` names it, and resolved
/// against the playlist — so there is no path to traverse and an unknown
/// name is a 404. The corpus is immutable, so the picture may be cached for
/// a week.
async fn frame(
    State(player): State<SharedPlayer>,
    Path(name): Path<String>,
    Query(q): Query<FrameQuery>,
) -> Response {
    let Some(index) = player.find(&name) else {
        return (StatusCode::NOT_FOUND, "no such clip").into_response();
    };
    let path = player.files[index].clone();
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
