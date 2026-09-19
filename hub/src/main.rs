use hub::{
    create_router, punch::run_punch_listener, relay::run_relay_listener, state::AppState,
};
use std::{env, net::SocketAddr, time::Duration};

#[derive(Debug, PartialEq, Eq)]
pub struct HubConfig {
    pub ws_port: u16,
    pub relay_public_addr: Option<SocketAddr>,
    pub skip_punch: bool,
}

/// Parses command-line arguments and environment variables into [`HubConfig`].
///
/// Port resolution order:
/// 1. `--port <PORT>` or `--port=<PORT>`
/// 2. `PORT` environment variable
/// 3. Default: `8080`
///
/// Relay public address resolution order:
/// 1. `--relay-public-addr <ADDR:PORT>` or `--relay-public-addr=<ADDR:PORT>`
/// 2. `RELAY_PUBLIC_ADDR` environment variable
/// 3. `HUB_PUBLIC_ADDR` environment variable
/// 4. None (dynamic host/interface determination)
///
/// Skip punch resolution order:
/// 1. `--skip-punch` CLI flag
/// 2. `SKIP_PUNCH` environment variable ("1", "true", "yes")
/// 3. Default: `false`
pub fn parse_hub_config(
    args: &[String],
    env_port: Option<&str>,
    env_relay: Option<&str>,
    env_hub: Option<&str>,
    env_skip_punch: Option<&str>,
) -> HubConfig {
    let mut cli_port = None;
    let mut cli_relay_addr = None;
    let mut cli_skip_punch = false;

    let mut i = 1;
    while i < args.len() {
        if args[i] == "--port" && i + 1 < args.len() {
            if let Ok(port) = args[i + 1].parse::<u16>() {
                cli_port = Some(port);
            }
            i += 2;
        } else if let Some(stripped) = args[i].strip_prefix("--port=") {
            if let Ok(port) = stripped.parse::<u16>() {
                cli_port = Some(port);
            }
            i += 1;
        } else if args[i] == "--relay-public-addr" && i + 1 < args.len() {
            if let Ok(addr) = args[i + 1].parse::<SocketAddr>() {
                cli_relay_addr = Some(addr);
            }
            i += 2;
        } else if let Some(stripped) = args[i].strip_prefix("--relay-public-addr=") {
            if let Ok(addr) = stripped.parse::<SocketAddr>() {
                cli_relay_addr = Some(addr);
            }
            i += 1;
        } else if args[i] == "--skip-punch" {
            cli_skip_punch = true;
            i += 1;
        } else {
            i += 1;
        }
    }

    let ws_port = cli_port
        .or_else(|| env_port.and_then(|s| s.trim().parse::<u16>().ok()))
        .unwrap_or(8080);

    let env_relay_addr = env_relay
        .or(env_hub)
        .and_then(|s| s.trim().parse::<SocketAddr>().ok());

    let relay_public_addr = cli_relay_addr.or(env_relay_addr);

    let env_skip = env_skip_punch
        .map(|s| {
            let lower = s.trim().to_ascii_lowercase();
            lower == "1" || lower == "true" || lower == "yes"
        })
        .unwrap_or(false);

    let skip_punch = cli_skip_punch || env_skip;

    HubConfig {
        ws_port,
        relay_public_addr,
        skip_punch,
    }
}

#[tokio::main]
async fn main() {
    tracing_subscriber::fmt::init();

    let args: Vec<String> = env::args().collect();
    let env_port = env::var("PORT").ok();
    let env_relay = env::var("RELAY_PUBLIC_ADDR").ok();
    let env_hub = env::var("HUB_PUBLIC_ADDR").ok();
    let env_skip_punch = env::var("SKIP_PUNCH").ok();

    let config = parse_hub_config(
        &args,
        env_port.as_deref(),
        env_relay.as_deref(),
        env_hub.as_deref(),
        env_skip_punch.as_deref(),
    );

    let state = match config.relay_public_addr {
        Some(addr) => {
            tracing::info!(%addr, "Using configured public relay address");
            AppState::new(addr).with_skip_punch(config.skip_punch)
        }
        None => {
            tracing::info!("No explicit public relay address configured; will determine dynamically from incoming interface/Host header (defaulting to port 9001)");
            AppState::new_dynamic().with_skip_punch(config.skip_punch)
        }
    };

    // Spawn mini-STUN UDP listener task on 0.0.0.0:9000
    if !config.skip_punch {
        let punch_addr: SocketAddr = "0.0.0.0:9000".parse().expect("valid UDP punch address");
        tokio::spawn(run_punch_listener(punch_addr, state.clone()));
    } else {
        tracing::info!("UDP punch verification disabled (--skip-punch active); skipping UDP punch listener on :9000");
    }

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

    // Run Axum WebSocket signaling server on 0.0.0.0:<ws_port>
    let app = create_router(state);
    let ws_addr = SocketAddr::from(([0, 0, 0, 0], config.ws_port));
    let listener = tokio::net::TcpListener::bind(ws_addr)
        .await
        .unwrap_or_else(|e| panic!("bind WS TCP listener on {} failed: {}", ws_addr, e));

    tracing::info!(
        configured_relay_addr = ?config.relay_public_addr,
        ws_port = config.ws_port,
        skip_punch = config.skip_punch,
        "Hub signaling & relay server online: :{} (WS) / :9000 (UDP punch{}) / :9001 (UDP relay)",
        config.ws_port,
        if config.skip_punch { " [disabled]" } else { "" }
    );
    if let Err(err) = axum::serve(
        listener,
        app.into_make_service_with_connect_info::<SocketAddr>(),
    )
    .await
    {
        tracing::error!(%err, "Axum server failed");
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_hub_config_defaults() {
        let args = vec!["hub".to_string()];
        let config = parse_hub_config(&args, None, None, None, None);
        assert_eq!(config.ws_port, 8080);
        assert_eq!(config.relay_public_addr, None);
        assert!(!config.skip_punch);
    }

    #[test]
    fn test_parse_hub_config_cli_port() {
        let args = vec![
            "hub".to_string(),
            "--port".to_string(),
            "9090".to_string(),
        ];
        let config = parse_hub_config(&args, Some("8080"), None, None, None);
        assert_eq!(config.ws_port, 9090);

        let args_eq = vec!["hub".to_string(), "--port=7070".to_string()];
        let config_eq = parse_hub_config(&args_eq, None, None, None, None);
        assert_eq!(config_eq.ws_port, 7070);
    }

    #[test]
    fn test_parse_hub_config_env_port() {
        let args = vec!["hub".to_string()];
        let config = parse_hub_config(&args, Some("3000"), None, None, None);
        assert_eq!(config.ws_port, 3000);
    }

    #[test]
    fn test_parse_hub_config_cli_port_precedence_over_env() {
        let args = vec![
            "hub".to_string(),
            "--port".to_string(),
            "5000".to_string(),
        ];
        let config = parse_hub_config(&args, Some("3000"), None, None, None);
        assert_eq!(config.ws_port, 5000);
    }

    #[test]
    fn test_parse_hub_config_relay_addr() {
        let args = vec![
            "hub".to_string(),
            "--relay-public-addr".to_string(),
            "1.2.3.4:9001".to_string(),
            "--port".to_string(),
            "8888".to_string(),
        ];
        let config = parse_hub_config(&args, None, None, None, None);
        assert_eq!(config.ws_port, 8888);
        assert_eq!(
            config.relay_public_addr,
            Some("1.2.3.4:9001".parse().unwrap())
        );

        let args_eq = vec![
            "hub".to_string(),
            "--relay-public-addr=5.6.7.8:9002".to_string(),
        ];
        let config_eq = parse_hub_config(&args_eq, None, None, None, None);
        assert_eq!(
            config_eq.relay_public_addr,
            Some("5.6.7.8:9002".parse().unwrap())
        );
    }

    #[test]
    fn test_parse_hub_config_relay_env_fallback() {
        let args = vec!["hub".to_string()];
        let config = parse_hub_config(
            &args,
            None,
            Some("10.0.0.1:9001"),
            Some("10.0.0.2:9001"),
            None,
        );
        assert_eq!(
            config.relay_public_addr,
            Some("10.0.0.1:9001".parse().unwrap())
        );

        let config_hub = parse_hub_config(&args, None, None, Some("10.0.0.2:9001"), None);
        assert_eq!(
            config_hub.relay_public_addr,
            Some("10.0.0.2:9001".parse().unwrap())
        );
    }

    #[test]
    fn test_parse_hub_config_skip_punch_cli() {
        let args = vec!["hub".to_string(), "--skip-punch".to_string()];
        let config = parse_hub_config(&args, None, None, None, None);
        assert!(config.skip_punch);
    }

    #[test]
    fn test_parse_hub_config_skip_punch_env() {
        let args = vec!["hub".to_string()];
        let config = parse_hub_config(&args, None, None, None, Some("1"));
        assert!(config.skip_punch);

        let config_true = parse_hub_config(&args, None, None, None, Some("true"));
        assert!(config_true.skip_punch);

        let config_yes = parse_hub_config(&args, None, None, None, Some("YES"));
        assert!(config_yes.skip_punch);

        let config_false = parse_hub_config(&args, None, None, None, Some("0"));
        assert!(!config_false.skip_punch);
    }
}
