use crate::SharedPlayer;
use axum::{Router, extract::State, http::StatusCode, response::IntoResponse, routing::get};
use gst::prelude::*;
use gstreamer as gst;
use std::net::SocketAddr;

/// Process-is-alive signal: if this handler runs at all, the answer is yes.
/// Deeper checks belong on /health/ready — a stuck pipeline should fail
/// readiness, not get the process restarted.
async fn live() -> impl IntoResponse {
    (StatusCode::OK, "OK")
}

/// Ready only while the pipeline is PLAYING; 503 with the reason otherwise,
/// so k8s readiness probes pull the pod out of rotation while it recovers.
/// Same wire contract as vlc-server's /health/ready.
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

/// Wire-compatible with vlc-server: plain-text basename of the current clip
/// (what tripbot's vlc-client reads verbatim), empty string when idle.
async fn current(State(player): State<SharedPlayer>) -> String {
    player.current_basename().unwrap_or_default()
}

pub async fn run(player: SharedPlayer) {
    let port = std::env::var("HTTP_PORT")
        .unwrap_or_else(|_| "8080".to_string())
        .parse()
        .expect("HTTP_PORT must be a number");

    let app = Router::new()
        // Bare /health is a legacy alias of /health/live.
        .route("/health", get(live))
        .route("/health/live", get(live))
        .route("/health/ready", get(ready))
        .route("/vlc/current", get(current))
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
}
