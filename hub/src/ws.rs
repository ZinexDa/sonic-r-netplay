use crate::{
    punch::await_punch_addr,
    state::{AppState, ServerRecord},
};
use axum::{
    extract::{
        ws::{Message, WebSocket, WebSocketUpgrade},
        State,
    },
    response::IntoResponse,
};
use futures_util::{SinkExt, StreamExt};
use proto::{ClientMessage, HubMessage, ServerId, ServerInfo};
use std::time::Instant;
use tokio::sync::mpsc;

/// Axum route handler upgrading incoming HTTP request to a WebSocket connection.
pub async fn ws_handler(
    ws: WebSocketUpgrade,
    headers: axum::http::HeaderMap,
    State(state): State<AppState>,
) -> impl IntoResponse {
    let host_header = headers
        .get(axum::http::header::HOST)
        .and_then(|h| h.to_str().ok())
        .map(|s| s.to_string());
    ws.on_upgrade(move |socket| handle_socket(socket, state, host_header))
}

/// Manages an active WebSocket connection with a host (sidecar) or client.
pub async fn handle_socket(socket: WebSocket, state: AppState, host_header: Option<String>) {
    tracing::debug!("New WebSocket connection established");

    let (mut ws_tx, mut ws_rx) = socket.split();
    let (out_tx, mut out_rx) = mpsc::unbounded_channel::<HubMessage>();

    // Spawn an asynchronous forwarder task to push outgoing HubMessages to the WebSocket.
    let forward_task = tokio::spawn(async move {
        while let Some(msg) = out_rx.recv().await {
            match serde_json::to_string(&msg) {
                Ok(text) => {
                    if ws_tx.send(Message::Text(text)).await.is_err() {
                        break;
                    }
                }
                Err(err) => {
                    tracing::error!(%err, "Failed to serialize HubMessage to JSON");
                }
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
            Message::Binary(_) => {
                tracing::debug!("Ignoring unexpected binary WebSocket message");
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
                udp_port: _,
                punch_token,
            } => {
                tracing::info!(%name, %game_version, %punch_token, "Host registration request received, awaiting UDP punch");

                let Some(public_addr) = await_punch_addr(&state, punch_token).await else {
                    tracing::warn!(%name, %punch_token, "Host registration failed: punch packet timeout");
                    let _ = out_tx.send(HubMessage::Error {
                        message: "Punch timeout: UDP packet not received".into(),
                    });
                    continue;
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

                let _ = out_tx.send(HubMessage::Registered { server_id: id });
            }

            ClientMessage::ListServers => {
                tracing::debug!("Client requested active server list");
                let servers: Vec<ServerInfo> =
                    state.servers.iter().map(|entry| entry.value().info.clone()).collect();
                let _ = out_tx.send(HubMessage::ServerList { servers });
            }

            ClientMessage::JoinRequest {
                server_id,
                punch_token,
            } => {
                tracing::info!(%server_id, %punch_token, "Join request received for server");

                // Lookup host record and clone needed fields before dropping the map entry
                let (host_addr, host_tx, host_relay_addr) = match state.servers.get(&server_id) {
                    Some(host_entry) => (
                        host_entry.public_udp_addr,
                        host_entry.out_tx.clone(),
                        host_entry.relay_addr,
                    ),
                    None => {
                        tracing::warn!(%server_id, "Join request rejected: server not found");
                        let _ = out_tx.send(HubMessage::Error {
                            message: format!("Server not found: {server_id}"),
                        });
                        continue;
                    }
                };

                let Some(client_addr) = await_punch_addr(&state, punch_token).await else {
                    tracing::warn!(%server_id, %punch_token, "Join request failed: client UDP punch timeout");
                    let _ = out_tx.send(HubMessage::Error {
                        message: "Punch timeout: client UDP packet not received".into(),
                    });
                    continue;
                };

                // Send peer candidate to both the host and the joining client
                let _ = host_tx.send(HubMessage::PeerCandidate {
                    peer_addr: client_addr,
                    punch_token,
                });
                let _ = out_tx.send(HubMessage::PeerCandidate {
                    peer_addr: host_addr,
                    punch_token,
                });

                let client_relay_addr = state.resolve_relay_addr(host_header.as_deref());

                // Persist the pairing session keyed by punch_token so either peer can trigger relay fallback
                state.pairings.insert(
                    punch_token,
                    crate::state::PairSession {
                        host_tx: host_tx.clone(),
                        client_tx: out_tx.clone(),
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

                let (host_tx, client_tx, host_relay_addr, client_relay_addr) =
                    match state.pairings.get(&punch_token) {
                        Some(pairing) => (
                            pairing.host_tx.clone(),
                            pairing.client_tx.clone(),
                            pairing.host_relay_addr,
                            pairing.client_relay_addr,
                        ),
                        None => {
                            tracing::warn!(%punch_token, "RelayFallback rejected: pairing not found for token");
                            let _ = out_tx.send(HubMessage::Error {
                                message: format!("Relay session not found for token: {punch_token}"),
                            });
                            continue;
                        }
                    };

                // Notify both peers to switch to relay mode using their respective reachable relay addresses
                let _ = host_tx.send(HubMessage::UseRelay {
                    punch_token,
                    relay_addr: host_relay_addr,
                });
                let _ = client_tx.send(HubMessage::UseRelay {
                    punch_token,
                    relay_addr: client_relay_addr,
                });

                tracing::info!(
                    %punch_token,
                    %host_relay_addr,
                    %client_relay_addr,
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
