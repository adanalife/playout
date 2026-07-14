use crate::SharedPlayer;
use axum::{Json, Router, extract::State, response::IntoResponse, routing::get};
use serde::Serialize;
use std::net::SocketAddr;

#[derive(Serialize)]
struct CurrentClip {
    uri: Option<String>,
}

async fn health() -> impl IntoResponse {
    (axum::http::StatusCode::OK, "OK")
}

async fn current(State(player): State<SharedPlayer>) -> Json<CurrentClip> {
    Json(CurrentClip {
        uri: player.current(),
    })
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
