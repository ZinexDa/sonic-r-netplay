use hub::{
    create_router, punch::run_punch_listener, relay::run_relay_listener, state::AppState,
};
use std::{env, net::SocketAddr, time::Duration};

#[tokio::main]
async fn main() {
    tracing_subscriber::fmt::init();

    // Parse CLI arguments for --relay-public-addr <ADDR:PORT>
    let args: Vec<String> = env::args().collect();
    let mut cli_relay_addr = None;
    let mut i = 1;
    while i < args.len() {
        if args[i] == "--relay-public-addr" && i + 1 < args.len() {
            if let Ok(addr) = args[i + 1].parse::<SocketAddr>() {
                cli_relay_addr = Some(addr);
            }
            i += 2;
        } else if let Some(stripped) = args[i].strip_prefix("--relay-public-addr=") {
            if let Ok(addr) = stripped.parse::<SocketAddr>() {
                cli_relay_addr = Some(addr);
            }
            i += 1;
        } else {
            i += 1;
        }
    }

    // Read relay public address announced to clients in UseRelay.
    // Checked in priority order:
    // 1. CLI flag `--relay-public-addr <ADDR:PORT>`
    // 2. Env var `RELAY_PUBLIC_ADDR`
    // 3. Env var `HUB_PUBLIC_ADDR`
    // If none are specified, AppState operates dynamically using incoming client interface / Host header.
    let env_relay_addr = env::var("RELAY_PUBLIC_ADDR")
        .or_else(|_| env::var("HUB_PUBLIC_ADDR"))
        .ok()
        .and_then(|s| s.parse::<SocketAddr>().ok());

    let configured_relay_addr = cli_relay_addr.or(env_relay_addr);

    let state = match configured_relay_addr {
        Some(addr) => {
            tracing::info!(%addr, "Using configured public relay address");
            AppState::new(addr)
        }
        None => {
            tracing::info!("No explicit public relay address configured; will determine dynamically from incoming interface/Host header (defaulting to port 9001)");
            AppState::new_dynamic()
        }
    };

    // Spawn mini-STUN UDP listener task on 0.0.0.0:9000
    let punch_addr: SocketAddr = "0.0.0.0:9000".parse().expect("valid UDP punch address");
    tokio::spawn(run_punch_listener(punch_addr, state.clone()));

    // Spawn UDP relay listener task on 0.0.0.0:9001
    let relay_bind_addr: SocketAddr = "0.0.0.0:9001".parse().expect("valid UDP relay bind address");
    let relay_sock = tokio::net::UdpSocket::bind(relay_bind_addr)
        .await
        .expect("bind UDP relay socket failed");
    tokio::spawn(run_relay_listener(relay_sock, state.clone()));

    // Spawn periodic background reaper for stale registrations, expired punch tokens, and idle relay sessions
    let reaper_state = state.clone();
    tokio::spawn(async move {
        let mut interval = tokio::time::interval(Duration::from_secs(5));
        loop {
            interval.tick().await;
            reaper_state.reap_stale();
        }
    });

    // Run Axum WebSocket signaling server on 0.0.0.0:8080
    let app = create_router(state);
    let ws_addr: SocketAddr = "0.0.0.0:8080".parse().expect("valid WS address");
    let listener = tokio::net::TcpListener::bind(ws_addr)
        .await
        .expect("bind WS TCP listener failed");

    tracing::info!(
        ?configured_relay_addr,
        "Hub signaling & relay server online: :8080 (WS) / :9000 (UDP punch) / :9001 (UDP relay)"
    );
    if let Err(err) = axum::serve(listener, app).await {
        tracing::error!(%err, "Axum server failed");
    }
}
