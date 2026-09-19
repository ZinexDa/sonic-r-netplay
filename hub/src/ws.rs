use crate::{
    punch::await_punch_addr,
    state::{AppState, ServerRecord},
};
use axum::{
    extract::{
        ws::{Message, WebSocket, WebSocketUpgrade},
        ConnectInfo, State,
    },
    response::IntoResponse,
};
use futures_util::{SinkExt, StreamExt};
use proto::{ClientMessage, HubMessage, ServerId, ServerInfo};
use std::{net::SocketAddr, time::Instant};
use tokio::sync::mpsc;

/// Helper to serialize and enqueue a JSON HubMessage over a WebSocket sender.
fn send_hub_msg(tx: &mpsc::UnboundedSender<Message>, msg: &HubMessage) {
    if let Ok(text) = serde_json::to_string(msg) {
        let _ = tx.send(Message::Text(text));
    }
}

/// Resolves the peer socket address of the WebSocket connection.
/// Checks reverse-proxy headers (`X-Forwarded-For`, `X-Real-IP`, `CF-Connecting-IP`),
/// falls back to `ConnectInfo`, and defaults to `127.0.0.1:5029` if neither is available.
pub fn resolve_peer_addr(
    connect_info: Option<ConnectInfo<SocketAddr>>,
    headers: &axum::http::HeaderMap,
) -> SocketAddr {
    if let Some(forwarded) = headers.get("x-forwarded-for").and_then(|v| v.to_str().ok()) {
        if let Some(first_ip_str) = forwarded.split(',').next() {
            let trimmed = first_ip_str.trim();
            if let Ok(addr) = trimmed.parse::<SocketAddr>() {
                return addr;
            } else if let Ok(ip) = trimmed.parse::<std::net::IpAddr>() {
                let port = connect_info.map(|ci| ci.0.port()).unwrap_or(5029);
                return SocketAddr::new(ip, port);
            }
        }
    }

    if let Some(real_ip) = headers
        .get("x-real-ip")
        .or_else(|| headers.get("cf-connecting-ip"))
        .and_then(|v| v.to_str().ok())
    {
        let trimmed = real_ip.trim();
        if let Ok(addr) = trimmed.parse::<SocketAddr>() {
            return addr;
        } else if let Ok(ip) = trimmed.parse::<std::net::IpAddr>() {
            let port = connect_info.map(|ci| ci.0.port()).unwrap_or(5029);
            return SocketAddr::new(ip, port);
        }
    }

    if let Some(ConnectInfo(addr)) = connect_info {
        return addr;
    }

    SocketAddr::from(([127, 0, 0, 1], 5029))
}

/// Axum route handler upgrading incoming HTTP request to a WebSocket connection.
pub async fn ws_handler(
    ws: WebSocketUpgrade,
    connect_info: Option<ConnectInfo<SocketAddr>>,
    headers: axum::http::HeaderMap,
    State(state): State<AppState>,
) -> impl IntoResponse {
    let host_header = headers
        .get(axum::http::header::HOST)
        .and_then(|h| h.to_str().ok())
        .map(|s| s.to_string());
    let peer_addr = resolve_peer_addr(connect_info, &headers);
    ws.on_upgrade(move |socket| handle_socket(socket, state, host_header, peer_addr))
}

/// Manages an active WebSocket connection with a host (sidecar) or client.
pub async fn handle_socket(
    socket: WebSocket,
    state: AppState,
    host_header: Option<String>,
    peer_addr: SocketAddr,
) {
    tracing::debug!(%peer_addr, "New WebSocket connection established");

    let (mut ws_tx, mut ws_rx) = socket.split();
    let (out_tx, mut out_rx) = mpsc::unbounded_channel::<Message>();

    // Spawn an asynchronous forwarder task to push outgoing Messages (Text/Binary) to the WebSocket.
    let forward_task = tokio::spawn(async move {
        while let Some(msg) = out_rx.recv().await {
            if ws_tx.send(msg).await.is_err() {
                break;
            }
        }
    });

    let mut my_server_id: Option<ServerId> = None;

    while let Some(msg_result) = ws_rx.next().await {
        let msg = match msg_result {
            Ok(m) => m,
            Err(err) => {
                tracing::debug!(%err, "WebSocket error, terminating connection loop");
                break;
            }
        };

        let text = match msg {
            Message::Text(t) => t,
            Message::Close(_) => {
                tracing::debug!("WebSocket client sent close frame");
                break;
            }
            Message::Binary(bytes) => {
                // Game datagram tunneled over WebSocket:
                // [16-byte punch_token][1-byte msg_type][payload...]
                if bytes.len() >= 17 {
                    if let Ok(token) = uuid::Uuid::from_slice(&bytes[..16]) {
                        if let Some(pairing) = state.pairings.get(&token) {
                            if pairing.host_peer_addr == peer_addr {
                                let _ = pairing.client_tx.send(Message::Binary(bytes));
                            } else {
                                let _ = pairing.host_tx.send(Message::Binary(bytes));
                            }
                        }
                    }
                }
                continue;
            }
            Message::Ping(_) | Message::Pong(_) => continue,
        };

        // Parse client JSON message. Malformed messages are logged and ignored without dropping the connection.
        let cmsg: ClientMessage = match serde_json::from_str(&text) {
            Ok(c) => c,
            Err(err) => {
                tracing::warn!(%text, %err, "Malformed client message received; ignoring");
                continue;
            }
        };

        match cmsg {
            ClientMessage::RegisterHost {
                name,
                max_players,
                game_version,
                udp_port,
                punch_token,
            } => {
                let public_addr = if state.skip_punch {
                    let port = udp_port.filter(|&p| p > 0).unwrap_or(5029);
                    let addr = SocketAddr::new(peer_addr.ip(), port);
                    tracing::info!(
                        %name,
                        %punch_token,
                        %addr,
                        "Skip-punch active: registering host with public IP and UDP port {port}"
                    );
                    addr
                } else {
                    tracing::info!(%name, %game_version, %punch_token, "Host registration request received, awaiting UDP punch");

                    let Some(public_addr) = await_punch_addr(&state, punch_token).await else {
                        tracing::warn!(%name, %punch_token, "Host registration failed: punch packet timeout");
                        send_hub_msg(&out_tx, &HubMessage::Error {
                            message: "Punch timeout: UDP packet not received".into(),
                        });
                        continue;
                    };
                    public_addr
                };

                // Remove previous registration if this connection had already registered one
                if let Some(old_id) = my_server_id.take() {
                    state.servers.remove(&old_id);
                }

                let id = uuid::Uuid::new_v4();
                let info = ServerInfo {
                    id,
                    name,
                    players: 1,
                    max_players,
                    game_version,
                    status: proto::ServerStatus::InLobby,
                };

                let host_relay_addr = state.resolve_relay_addr(host_header.as_deref());
                state.servers.insert(
                    id,
                    ServerRecord {
                        info: info.clone(),
                        public_udp_addr: public_addr,
                        out_tx: out_tx.clone(),
                        relay_addr: host_relay_addr,
                        peer_addr,
                        last_heartbeat: Instant::now(),
                    },
                );

                my_server_id = Some(id);
                tracing::info!(
                    server_id = %id,
                    name = %info.name,
                    public_udp = %public_addr,
                    %host_relay_addr,
                    "Host successfully registered"
                );

                send_hub_msg(&out_tx, &HubMessage::Registered { server_id: id });
            }

            ClientMessage::ListServers => {
                tracing::debug!("Client requested active server list");
                let servers: Vec<ServerInfo> =
                    state.servers.iter().map(|entry| entry.value().info.clone()).collect();
                send_hub_msg(&out_tx, &HubMessage::ServerList { servers });
            }

            ClientMessage::JoinRequest {
                server_id,
                punch_token,
                udp_port,
            } => {
                tracing::info!(%server_id, %punch_token, "Join request received for server");

                // Lookup host record and clone needed fields before dropping the map entry
                let (host_addr, host_tx, host_peer_addr, host_relay_addr) = match state.servers.get(&server_id) {
                    Some(host_entry) => (
                        host_entry.public_udp_addr,
                        host_entry.out_tx.clone(),
                        host_entry.peer_addr,
                        host_entry.relay_addr,
                    ),
                    None => {
                        tracing::warn!(%server_id, "Join request rejected: server not found");
                        send_hub_msg(&out_tx, &HubMessage::Error {
                            message: format!("Server not found: {server_id}"),
                        });
                        continue;
                    }
                };

                let client_addr = if state.skip_punch {
                    let port = udp_port.filter(|&p| p > 0).unwrap_or(5029);
                    let addr = SocketAddr::new(peer_addr.ip(), port);
                    tracing::info!(
                        %server_id,
                        %punch_token,
                        %addr,
                        "Skip-punch active: using client public IP and UDP port {port}"
                    );
                    addr
                } else {
                    let Some(client_addr) = await_punch_addr(&state, punch_token).await else {
                        tracing::warn!(%server_id, %punch_token, "Join request failed: client UDP punch timeout");
                        send_hub_msg(&out_tx, &HubMessage::Error {
                            message: "Punch timeout: client UDP packet not received".into(),
                        });
                        continue;
                    };
                    client_addr
                };

                // Send peer candidate to both the host and the joining client
                send_hub_msg(&host_tx, &HubMessage::PeerCandidate {
                    peer_addr: client_addr,
                    punch_token,
                });
                send_hub_msg(&out_tx, &HubMessage::PeerCandidate {
                    peer_addr: host_addr,
                    punch_token,
                });

                let client_relay_addr = state.resolve_relay_addr(host_header.as_deref());

                // Persist the pairing session keyed by punch_token so either peer can trigger relay fallback or WS tunnel
                state.pairings.insert(
                    punch_token,
                    crate::state::PairSession {
                        host_tx: host_tx.clone(),
                        client_tx: out_tx.clone(),
                        host_addr,
                        client_addr,
                        host_peer_addr,
                        client_peer_addr: peer_addr,
                        host_relay_addr,
                        client_relay_addr,
                        created_at: Instant::now(),
                    },
                );

                tracing::info!(
                    %server_id,
                    %punch_token,
                    %host_addr,
                    %client_addr,
                    %host_relay_addr,
                    %client_relay_addr,
                    "Signaling complete: peer candidates exchanged and pair session recorded"
                );
            }

            ClientMessage::Heartbeat { players, status } => {
                tracing::debug!(?players, ?status, "Heartbeat message received");
                if let Some(id) = my_server_id {
                    if let Some(mut entry) = state.servers.get_mut(&id) {
                        entry.last_heartbeat = Instant::now();
                        if let Some(p) = players {
                            entry.info.players = p;
                        }
                        if let Some(st) = status {
                            entry.info.status = st;
                        }
                    }
                }
            }

            ClientMessage::RelayFallback { punch_token } => {
                tracing::info!(%punch_token, "RelayFallback requested by peer");

                let (host_tx, client_tx, host_target, client_target) =
                    match state.pairings.get(&punch_token) {
                        Some(pairing) => {
                            let relay_unreachable = state.skip_punch
                                || (state.relay_public_addr.is_none() && pairing.host_relay_addr.ip().is_loopback());

                            let (h_target, c_target) = if relay_unreachable {
                                tracing::info!(
                                    %punch_token,
                                    host_direct = %pairing.host_addr,
                                    client_direct = %pairing.client_addr,
                                    "UDP relay unreachable or single-port mode; falling back to direct peer IP connection on RelayFallback"
                                );
                                (pairing.client_addr, pairing.host_addr)
                            } else {
                                (pairing.host_relay_addr, pairing.client_relay_addr)
                            };

                            (
                                pairing.host_tx.clone(),
                                pairing.client_tx.clone(),
                                h_target,
                                c_target,
                            )
                        }
                        None => {
                            tracing::warn!(%punch_token, "RelayFallback rejected: pairing not found for token");
                            send_hub_msg(&out_tx, &HubMessage::Error {
                                message: format!("Relay session not found for token: {punch_token}"),
                            });
                            continue;
                        }
                    };

                // Notify both peers to switch to relay mode using their respective resolved addresses
                send_hub_msg(&host_tx, &HubMessage::UseRelay {
                    punch_token,
                    relay_addr: host_target,
                });
                send_hub_msg(&client_tx, &HubMessage::UseRelay {
                    punch_token,
                    relay_addr: client_target,
                });

                tracing::info!(
                    %punch_token,
                    %host_target,
                    %client_target,
                    "Dispatched UseRelay to host and client with resolved relay addresses"
                );
            }
        }
    }

    // Cleanup on disconnect
    if let Some(id) = my_server_id {
        if state.servers.remove(&id).is_some() {
            tracing::info!(server_id = %id, "Host disconnected, removed from server registry");
        }
    }

    // Explicitly abort the outgoing forwarder task to prevent lingering background tasks
    forward_task.abort();
    tracing::debug!("WebSocket connection handler terminated");
}
