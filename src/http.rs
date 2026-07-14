use crate::SharedPlayer;
use axum::{Router, extract::State, response::IntoResponse, routing::get};
use std::net::SocketAddr;

async fn health() -> impl IntoResponse {
    (axum::http::StatusCode::OK, "OK")
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
        .route("/vlc/current", get(current))
        .with_state(player);

    let addr = SocketAddr::from(([0, 0, 0, 0], port));
    let listener = tokio::net::TcpListener::bind(addr).await.unwrap();
    axum::serve(listener, app).await.unwrap();
}
