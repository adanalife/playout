use crate::SharedPlayer;
use axum::{Router, extract::State, response::IntoResponse, routing::get};
use std::net::SocketAddr;

async fn health() -> impl IntoResponse {
    (axum::http::StatusCode::OK, "OK")
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
        .route("/health", get(health))
        .route("/version", get(version))
        .route("/vlc/current", get(current))
        .with_state(player);

    let addr = SocketAddr::from(([0, 0, 0, 0], port));
    let listener = tokio::net::TcpListener::bind(addr).await.unwrap();
    axum::serve(listener, app).await.unwrap();
}

#[cfg(test)]
mod tests {
    use super::version_json;

    #[test]
    fn version_json_matches_contract() {
        let v = version_json();
        assert!(v["tag"].as_str().is_some_and(|t| !t.is_empty()));
        assert!(v["sha"].as_str().is_some_and(|s| !s.is_empty()));
        assert!(v["built_at"].as_str().is_some_and(|b| !b.is_empty()));
    }
}
