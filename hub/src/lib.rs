pub mod punch;
pub mod relay;
pub mod state;
pub mod ws;

use axum::{routing::get, Router};
use state::AppState;
use ws::ws_handler;

/// Constructs the Axum signaling application router.
pub fn create_router(state: AppState) -> Router {
    Router::new()
        .route("/", get(ws_handler))
        .route("/ws", get(ws_handler))
        .with_state(state)
}
