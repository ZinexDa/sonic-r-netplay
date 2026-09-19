use proto::ServerId;
use std::{env, net::SocketAddr};

const DEFAULT_HUB_WS: &str = "ws://127.0.0.1:8080/ws";
const DEFAULT_HUB_UDP: &str = "127.0.0.1:9000";

pub enum SidecarMode {
    Host {
        name: String,
        bind_port: u16,
        target_game_addr: Option<SocketAddr>,
    },
    Join {
        server_id: Option<ServerId>,
        bind_port: u16,
        target_game_addr: Option<SocketAddr>,
    },
    List,
}

pub struct CliConfig {
    pub mode: SidecarMode,
    pub hub_ws_url: Option<String>,
    pub hub_udp_addr: Option<SocketAddr>,
}

#[tokio::main]
async fn main() {
    let _ = rustls::crypto::ring::default_provider().install_default();
    tracing_subscriber::fmt::init();

    let args: Vec<String> = env::args().collect();
    let config = parse_args(&args);

    let hub_ws_url = config
        .hub_ws_url
        .or_else(|| env::var("HUB_WS_URL").ok())
        .unwrap_or_else(|| DEFAULT_HUB_WS.to_string());
    let hub_udp_addr: SocketAddr = config
        .hub_udp_addr
        .or_else(|| {
            env::var("HUB_UDP_ADDR")
                .ok()
                .and_then(|s| s.parse().ok())
        })
        .unwrap_or_else(|| DEFAULT_HUB_UDP.parse().expect("Valid hub UDP address"));

    match config.mode {
        SidecarMode::Host {
            name,
            bind_port,
            target_game_addr,
        } => {
            let host_cfg = sidecar::runner::HostConfig {
                hub_ws_url,
                hub_udp_addr,
                name,
                bind_port,
                target_game_addr,
            };
            if let Err(err) = sidecar::runner::run_host_session(host_cfg, None).await {
                tracing::error!(%err, "Host session failed");
            }
        }
        SidecarMode::Join {
            server_id,
            bind_port,
            target_game_addr,
        } => {
            let join_cfg = sidecar::runner::JoinConfig {
                hub_ws_url,
                hub_udp_addr,
                server_id,
                bind_port,
                target_game_addr,
            };
            if let Err(err) = sidecar::runner::run_join_session(join_cfg, None).await {
                tracing::error!(%err, "Join session failed");
            }
        }
        SidecarMode::List => match sidecar::runner::fetch_server_list(&hub_ws_url).await {
            Ok(servers) => {
                if servers.is_empty() {
                    println!("No active servers currently online.");
                } else {
                    println!("\nOnline Servers ({}):", servers.len());
                    for (i, s) in servers.iter().enumerate() {
                        println!(
                            "  [{}] {:<25} v{:<8} players: {}/{} (id: {})",
                            i + 1,
                            s.name,
                            s.game_version,
                            s.players,
                            s.max_players,
                            s.id
                        );
                    }
                }
            }
            Err(err) => {
                tracing::error!(%err, "Failed to query server list");
            }
        },
    }
}

fn parse_args(args: &[String]) -> CliConfig {
    let mut explicit_game_port = None;
    let mut explicit_target_game_port = None;
    let mut explicit_proxy_port = None;
    let mut explicit_hub_ws_url = None;
    let mut explicit_hub_udp_addr = None;
    let mut filtered_args = Vec::new();

    let mut i = 1;
    while i < args.len() {
        if args[i] == "--game-port" && i + 1 < args.len() {
            if let Ok(p) = args[i + 1].parse::<u16>() {
                explicit_game_port = Some(p);
            }
            i += 2;
        } else if let Some(stripped) = args[i].strip_prefix("--game-port=") {
            if let Ok(p) = stripped.parse::<u16>() {
                explicit_game_port = Some(p);
            }
            i += 1;
        } else if args[i] == "--target-game-port" && i + 1 < args.len() {
            if let Ok(p) = args[i + 1].parse::<u16>() {
                explicit_target_game_port = Some(p);
            }
            i += 2;
        } else if let Some(stripped) = args[i].strip_prefix("--target-game-port=") {
            if let Ok(p) = stripped.parse::<u16>() {
                explicit_target_game_port = Some(p);
            }
            i += 1;
        } else if args[i] == "--proxy-port" && i + 1 < args.len() {
            if let Ok(p) = args[i + 1].parse::<u16>() {
                explicit_proxy_port = Some(p);
            }
            i += 2;
        } else if let Some(stripped) = args[i].strip_prefix("--proxy-port=") {
            if let Ok(p) = stripped.parse::<u16>() {
                explicit_proxy_port = Some(p);
            }
            i += 1;
        } else if args[i] == "--hub-ws-url" && i + 1 < args.len() {
            explicit_hub_ws_url = Some(args[i + 1].clone());
            i += 2;
        } else if let Some(stripped) = args[i].strip_prefix("--hub-ws-url=") {
            explicit_hub_ws_url = Some(stripped.to_string());
            i += 1;
        } else if args[i] == "--hub-udp-addr" && i + 1 < args.len() {
            if let Ok(addr) = args[i + 1].parse::<SocketAddr>() {
                explicit_hub_udp_addr = Some(addr);
            }
            i += 2;
        } else if let Some(stripped) = args[i].strip_prefix("--hub-udp-addr=") {
            if let Ok(addr) = stripped.parse::<SocketAddr>() {
                explicit_hub_udp_addr = Some(addr);
            }
            i += 1;
        } else {
            filtered_args.push(args[i].clone());
            i += 1;
        }
    }

    let env_game_port = env::var("GAME_PORT").ok().and_then(|p| p.parse::<u16>().ok());
    let env_target_game_port = env::var("TARGET_GAME_PORT").ok().and_then(|p| p.parse::<u16>().ok());
    let env_proxy_port = env::var("PROXY_PORT").ok().and_then(|p| p.parse::<u16>().ok());

    let is_join = !filtered_args.is_empty() && filtered_args[0] == "join";
    let is_list = !filtered_args.is_empty() && filtered_args[0] == "list";

    let mode = if is_list {
        SidecarMode::List
    } else if is_join {
        let server_id = filtered_args.get(1).and_then(|s| s.parse::<uuid::Uuid>().ok());
        let bind_port = explicit_proxy_port
            .or(explicit_game_port)
            .or(env_proxy_port)
            .or(env_game_port)
            .unwrap_or(5029);
        let target_game_addr = explicit_target_game_port
            .or(env_target_game_port)
            .map(|p| SocketAddr::from(([127, 0, 0, 1], p)));

        SidecarMode::Join {
            server_id,
            bind_port,
            target_game_addr,
        }
    } else {
        // Host mode (default or "host" sub-command or custom name)
        let host_name = if filtered_args.is_empty() {
            "Sonic Server (Default)".to_string()
        } else if filtered_args[0] == "host" {
            if filtered_args.len() > 1 {
                filtered_args[1..].join(" ")
            } else {
                "Sonic Host".to_string()
            }
        } else {
            filtered_args.join(" ")
        };

        let target_port = explicit_target_game_port
            .or(explicit_game_port)
            .or(env_target_game_port)
            .or(env_game_port)
            .unwrap_or(5029);
        let target_game_addr = Some(SocketAddr::from(([127, 0, 0, 1], target_port)));
        let bind_port = explicit_proxy_port
            .or(env_proxy_port)
            .unwrap_or(0); // In host mode, default bind is ephemeral port 0 to prevent port conflicts with game

        SidecarMode::Host {
            name: host_name,
            bind_port,
            target_game_addr,
        }
    };

    CliConfig {
        mode,
        hub_ws_url: explicit_hub_ws_url,
        hub_udp_addr: explicit_hub_udp_addr,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_args_host_with_hub_udp_addr() {
        let args = vec![
            "sidecar".to_string(),
            "host".to_string(),
            "--hub-udp-addr".to_string(),
            "26.142.208.116:9000".to_string(),
            "Sonic Host".to_string(),
        ];
        let cfg = parse_args(&args);
        assert_eq!(cfg.hub_udp_addr, Some("26.142.208.116:9000".parse().unwrap()));
        match cfg.mode {
            SidecarMode::Host { name, .. } => assert_eq!(name, "Sonic Host"),
            _ => panic!("Expected Host mode"),
        }
    }

    #[test]
    fn test_parse_args_host_unquoted_name_with_flags() {
        let args = vec![
            "sidecar".to_string(),
            "host".to_string(),
            "--hub-udp-addr".to_string(),
            "26.142.208.116:9000".to_string(),
            "Sonic".to_string(),
            "Speedway".to_string(),
        ];
        let cfg = parse_args(&args);
        assert_eq!(cfg.hub_udp_addr, Some("26.142.208.116:9000".parse().unwrap()));
        match cfg.mode {
            SidecarMode::Host { name, .. } => assert_eq!(name, "Sonic Speedway"),
            _ => panic!("Expected Host mode"),
        }
    }

    #[test]
    fn test_parse_args_flags_before_subcommand() {
        let args = vec![
            "sidecar".to_string(),
            "--hub-ws-url=ws://26.142.208.116:8080/ws".to_string(),
            "--hub-udp-addr=26.142.208.116:9000".to_string(),
            "host".to_string(),
            "My Server".to_string(),
        ];
        let cfg = parse_args(&args);
        assert_eq!(cfg.hub_ws_url, Some("ws://26.142.208.116:8080/ws".to_string()));
        assert_eq!(cfg.hub_udp_addr, Some("26.142.208.116:9000".parse().unwrap()));
        match cfg.mode {
            SidecarMode::Host { name, .. } => assert_eq!(name, "My Server"),
            _ => panic!("Expected Host mode"),
        }
    }

    #[test]
    fn test_parse_args_join_with_flags() {
        let uuid_str = "11111111-2222-3333-4444-555555555555";
        let args = vec![
            "sidecar".to_string(),
            "join".to_string(),
            "--hub-udp-addr".to_string(),
            "26.142.208.116:9000".to_string(),
            "--game-port".to_string(),
            "5030".to_string(),
            uuid_str.to_string(),
        ];
        let cfg = parse_args(&args);
        assert_eq!(cfg.hub_udp_addr, Some("26.142.208.116:9000".parse().unwrap()));
        match cfg.mode {
            SidecarMode::Join { server_id, bind_port, .. } => {
                assert_eq!(server_id, Some(uuid_str.parse().unwrap()));
                assert_eq!(bind_port, 5030);
            }
            _ => panic!("Expected Join mode"),
        }
    }

    #[test]
    fn test_parse_args_join_default_port() {
        let uuid_str = "11111111-2222-3333-4444-555555555555";
        let args = vec![
            "sidecar".to_string(),
            "join".to_string(),
            uuid_str.to_string(),
        ];
        let cfg = parse_args(&args);
        match cfg.mode {
            SidecarMode::Join { server_id, bind_port, .. } => {
                assert_eq!(server_id, Some(uuid_str.parse().unwrap()));
                assert_eq!(bind_port, 5029);
            }
            _ => panic!("Expected Join mode"),
        }
    }
}
