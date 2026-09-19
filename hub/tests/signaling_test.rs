use futures_util::{SinkExt, StreamExt};
use hub::{create_router, punch::run_punch_listener, state::AppState};
use proto::{ClientMessage, HubMessage};
use std::net::SocketAddr;
use tokio::net::{TcpListener, UdpSocket};
use tokio_tungstenite::{connect_async, tungstenite::Message};

struct TestHub {
    pub ws_addr: SocketAddr,
    pub udp_addr: SocketAddr,
    pub relay_addr: SocketAddr,
    pub state: AppState,
}

async fn spawn_test_hub() -> TestHub {
    // Bind ephemeral UDP relay socket
    let relay_sock = UdpSocket::bind("127.0.0.1:0").await.unwrap();
    let relay_addr = relay_sock.local_addr().unwrap();

    let state = AppState::new(relay_addr);
    tokio::spawn(hub::relay::run_relay_listener(relay_sock, state.clone()));

    // Bind ephemeral UDP punch socket
    let udp_sock = UdpSocket::bind("127.0.0.1:0").await.unwrap();
    let udp_addr = udp_sock.local_addr().unwrap();
    drop(udp_sock); // Release so run_punch_listener can bind to it
    tokio::spawn(run_punch_listener(udp_addr, state.clone()));

    // Bind ephemeral TCP listener for Axum WS
    let tcp_listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let ws_addr = tcp_listener.local_addr().unwrap();
    let app = create_router(state.clone());

    tokio::spawn(async move {
        axum::serve(tcp_listener, app).await.unwrap();
    });

    TestHub {
        ws_addr,
        udp_addr,
        relay_addr,
        state,
    }
}

#[tokio::test]
async fn test_full_signaling_flow() {
    let hub = spawn_test_hub().await;

    // --- 1. Host Connects & Registers ---
    let ws_url = format!("ws://{}/ws", hub.ws_addr);
    let (host_ws, _) = connect_async(&ws_url).await.expect("host ws connect");
    let (mut host_tx, mut host_rx) = host_ws.split();

    // Host sends UDP punch
    let host_udp_sock = UdpSocket::bind("127.0.0.1:0").await.unwrap();
    let host_local_udp_addr = host_udp_sock.local_addr().unwrap();
    let host_punch_token = uuid::Uuid::new_v4();
    host_udp_sock
        .send_to(host_punch_token.as_bytes(), hub.udp_addr)
        .await
        .unwrap();

    // Host sends RegisterHost
    let reg_msg = ClientMessage::RegisterHost {
        name: "Test Server".into(),
        max_players: 8,
        game_version: "1.0".into(),
        udp_port: None,
        punch_token: host_punch_token,
    };
    host_tx
        .send(Message::Text(serde_json::to_string(&reg_msg).unwrap()))
        .await
        .unwrap();

    // Host receives Registered
    let msg = host_rx.next().await.unwrap().unwrap();
    let Message::Text(text) = msg else { panic!("expected text") };
    let HubMessage::Registered { server_id } = serde_json::from_str(&text).unwrap() else {
        panic!("expected Registered message");
    };

    // --- 2. Client Discovers Server ---
    let (client_ws, _) = connect_async(&ws_url).await.expect("client ws connect");
    let (mut client_tx, mut client_rx) = client_ws.split();

    let list_msg = ClientMessage::ListServers;
    client_tx
        .send(Message::Text(serde_json::to_string(&list_msg).unwrap()))
        .await
        .unwrap();

    let msg = client_rx.next().await.unwrap().unwrap();
    let Message::Text(text) = msg else { panic!("expected text") };
    let HubMessage::ServerList { servers } = serde_json::from_str(&text).unwrap() else {
        panic!("expected ServerList");
    };
    assert_eq!(servers.len(), 1);
    assert_eq!(servers[0].id, server_id);
    assert_eq!(servers[0].name, "Test Server");

    // --- 3. Client Sends UDP Punch & JoinRequest ---
    let client_udp_sock = UdpSocket::bind("127.0.0.1:0").await.unwrap();
    let client_local_udp_addr = client_udp_sock.local_addr().unwrap();
    let client_punch_token = uuid::Uuid::new_v4();
    client_udp_sock
        .send_to(client_punch_token.as_bytes(), hub.udp_addr)
        .await
        .unwrap();

    let join_msg = ClientMessage::JoinRequest {
        server_id,
        punch_token: client_punch_token,
        udp_port: None,
    };
    client_tx
        .send(Message::Text(serde_json::to_string(&join_msg).unwrap()))
        .await
        .unwrap();

    // --- 4. Both Host and Client Receive PeerCandidate ---
    // Host receives client candidate
    let host_msg = host_rx.next().await.unwrap().unwrap();
    let Message::Text(host_text) = host_msg else { panic!("expected text") };
    let HubMessage::PeerCandidate {
        peer_addr: client_addr_reported,
        punch_token: host_received_token,
    } = serde_json::from_str(&host_text).unwrap()
    else {
        panic!("expected PeerCandidate on host");
    };
    assert_eq!(client_addr_reported, client_local_udp_addr);
    assert_eq!(host_received_token, client_punch_token);

    // Client receives host candidate
    let client_msg = client_rx.next().await.unwrap().unwrap();
    let Message::Text(client_text) = client_msg else { panic!("expected text") };
    let HubMessage::PeerCandidate {
        peer_addr: host_addr_reported,
        punch_token: client_received_token,
    } = serde_json::from_str(&client_text).unwrap()
    else {
        panic!("expected PeerCandidate on client");
    };
    assert_eq!(host_addr_reported, host_local_udp_addr);
    assert_eq!(client_received_token, client_punch_token);

    // --- 5. Host Disconnect Removes Server ---
    drop(host_tx);
    drop(host_rx);
    // Give hub a moment to process WS drop
    tokio::time::sleep(std::time::Duration::from_millis(50)).await;

    client_tx
        .send(Message::Text(serde_json::to_string(&list_msg).unwrap()))
        .await
        .unwrap();

    let msg = client_rx.next().await.unwrap().unwrap();
    let Message::Text(text) = msg else { panic!("expected text") };
    let HubMessage::ServerList { servers } = serde_json::from_str(&text).unwrap() else {
        panic!("expected ServerList");
    };
    assert_eq!(servers.len(), 0);
}

#[tokio::test]
async fn test_unknown_server_join_returns_error() {
    let hub = spawn_test_hub().await;
    let ws_url = format!("ws://{}/ws", hub.ws_addr);
    let (ws, _) = connect_async(&ws_url).await.expect("ws connect");
    let (mut tx, mut rx) = ws.split();

    let random_server_id = uuid::Uuid::new_v4();
    let punch_token = uuid::Uuid::new_v4();

    let join_msg = ClientMessage::JoinRequest {
        server_id: random_server_id,
        punch_token,
        udp_port: None,
    };
    tx.send(Message::Text(serde_json::to_string(&join_msg).unwrap()))
        .await
        .unwrap();

    let msg = rx.next().await.unwrap().unwrap();
    let Message::Text(text) = msg else { panic!("expected text") };
    let HubMessage::Error { message } = serde_json::from_str(&text).unwrap() else {
        panic!("expected Error message, got: {}", text);
    };
    assert!(message.contains("Server not found"));
}

#[tokio::test]
async fn test_malformed_json_does_not_drop_connection() {
    let hub = spawn_test_hub().await;
    let ws_url = format!("ws://{}/ws", hub.ws_addr);
    let (ws, _) = connect_async(&ws_url).await.expect("ws connect");
    let (mut tx, mut rx) = ws.split();

    // Send broken JSON
    tx.send(Message::Text("this is { not valid json }".to_string()))
        .await
        .unwrap();

    // Send valid ListServers
    let list_msg = ClientMessage::ListServers;
    tx.send(Message::Text(serde_json::to_string(&list_msg).unwrap()))
        .await
        .unwrap();

    // Should receive ServerList back
    let msg = rx.next().await.unwrap().unwrap();
    let Message::Text(text) = msg else { panic!("expected text") };
    let HubMessage::ServerList { servers } = serde_json::from_str(&text).unwrap() else {
        panic!("expected ServerList");
    };
    assert!(servers.is_empty());
}

#[tokio::test]
async fn test_heartbeat_and_reaper() {
    let hub = spawn_test_hub().await;
    let ws_url = format!("ws://{}/ws", hub.ws_addr);
    let (ws, _) = connect_async(&ws_url).await.expect("ws connect");
    let (mut tx, mut rx) = ws.split();

    // Send UDP punch
    let udp_sock = UdpSocket::bind("127.0.0.1:0").await.unwrap();
    let punch_token = uuid::Uuid::new_v4();
    udp_sock
        .send_to(punch_token.as_bytes(), hub.udp_addr)
        .await
        .unwrap();

    // Register host
    let reg_msg = ClientMessage::RegisterHost {
        name: "Heartbeat Server".into(),
        max_players: 4,
        game_version: "1.0".into(),
        udp_port: None,
        punch_token,
    };
    tx.send(Message::Text(serde_json::to_string(&reg_msg).unwrap()))
        .await
        .unwrap();

    let msg = rx.next().await.unwrap().unwrap();
    let Message::Text(text) = msg else { panic!("expected text") };
    let HubMessage::Registered { server_id } = serde_json::from_str(&text).unwrap() else {
        panic!("expected Registered");
    };

    // Initial players count should be 1 (the host)
    assert_eq!(hub.state.servers.get(&server_id).unwrap().info.players, 1);

    // Send Heartbeat with players = 3 and status = InRace
    let hb_msg = ClientMessage::Heartbeat {
        players: Some(3),
        status: Some(proto::ServerStatus::InRace),
    };
    tx.send(Message::Text(serde_json::to_string(&hb_msg).unwrap()))
        .await
        .unwrap();

    // Allow hub a moment to process the message
    tokio::time::sleep(std::time::Duration::from_millis(50)).await;
    assert_eq!(hub.state.servers.get(&server_id).unwrap().info.players, 3);
    assert_eq!(
        hub.state.servers.get(&server_id).unwrap().info.status,
        proto::ServerStatus::InRace
    );

    // Manually age the server's heartbeat to simulate 20s of silence
    {
        let mut entry = hub.state.servers.get_mut(&server_id).unwrap();
        entry.last_heartbeat = std::time::Instant::now() - std::time::Duration::from_secs(20);
    }

    // Run reaper
    hub.state.reap_stale();

    // Server should now be removed from registry
    assert!(hub.state.servers.get(&server_id).is_none());
}

#[tokio::test]
async fn test_relay_fallback_coordination() {
    let hub = spawn_test_hub().await;
    let ws_url = format!("ws://{}/ws", hub.ws_addr);

    // 1. Host registers
    let (host_ws, _) = connect_async(&ws_url).await.unwrap();
    let (mut host_tx, mut host_rx) = host_ws.split();

    let host_punch_token = uuid::Uuid::new_v4();
    let host_udp_sock = UdpSocket::bind("127.0.0.1:0").await.unwrap();
    host_udp_sock
        .send_to(host_punch_token.as_bytes(), hub.udp_addr)
        .await
        .unwrap();

    let reg_msg = ClientMessage::RegisterHost {
        name: "Relay Test Server".into(),
        max_players: 4,
        game_version: "1.0".into(),
        udp_port: None,
        punch_token: host_punch_token,
    };
    host_tx
        .send(Message::Text(serde_json::to_string(&reg_msg).unwrap()))
        .await
        .unwrap();

    let msg = host_rx.next().await.unwrap().unwrap();
    let Message::Text(text) = msg else { panic!("expected text") };
    let HubMessage::Registered { server_id } = serde_json::from_str(&text).unwrap() else {
        panic!("expected Registered");
    };

    // 2. Client joins
    let (client_ws, _) = connect_async(&ws_url).await.unwrap();
    let (mut client_tx, mut client_rx) = client_ws.split();

    let client_punch_token = uuid::Uuid::new_v4();
    let client_udp_sock = UdpSocket::bind("127.0.0.1:0").await.unwrap();
    client_udp_sock
        .send_to(client_punch_token.as_bytes(), hub.udp_addr)
        .await
        .unwrap();

    let join_msg = ClientMessage::JoinRequest {
        server_id,
        punch_token: client_punch_token,
        udp_port: None,
    };
    client_tx
        .send(Message::Text(serde_json::to_string(&join_msg).unwrap()))
        .await
        .unwrap();

    // Both receive PeerCandidate
    let _ = host_rx.next().await.unwrap().unwrap();
    let _ = client_rx.next().await.unwrap().unwrap();

    // 3. Client signals RelayFallback
    let fallback_msg = ClientMessage::RelayFallback {
        punch_token: client_punch_token,
    };
    client_tx
        .send(Message::Text(serde_json::to_string(&fallback_msg).unwrap()))
        .await
        .unwrap();

    // Both host and client must receive UseRelay
    let host_msg = host_rx.next().await.unwrap().unwrap();
    let Message::Text(h_text) = host_msg else { panic!("expected text") };
    let HubMessage::UseRelay {
        punch_token: h_token,
        relay_addr: h_relay,
    } = serde_json::from_str(&h_text).unwrap()
    else {
        panic!("expected UseRelay on host, got: {h_text}");
    };
    assert_eq!(h_token, client_punch_token);
    assert_eq!(h_relay, hub.relay_addr);

    let client_msg = client_rx.next().await.unwrap().unwrap();
    let Message::Text(c_text) = client_msg else { panic!("expected text") };
    let HubMessage::UseRelay {
        punch_token: c_token,
        relay_addr: c_relay,
    } = serde_json::from_str(&c_text).unwrap()
    else {
        panic!("expected UseRelay on client, got: {c_text}");
    };
    assert_eq!(c_token, client_punch_token);
    assert_eq!(c_relay, hub.relay_addr);

    // 4. Test idempotency: host also sends RelayFallback
    host_tx
        .send(Message::Text(serde_json::to_string(&fallback_msg).unwrap()))
        .await
        .unwrap();

    // Both should receive second UseRelay without error
    let host_msg2 = host_rx.next().await.unwrap().unwrap();
    let Message::Text(h_text2) = host_msg2 else { panic!("expected text") };
    assert!(h_text2.contains("UseRelay"));

    let client_msg2 = client_rx.next().await.unwrap().unwrap();
    let Message::Text(c_text2) = client_msg2 else { panic!("expected text") };
    assert!(c_text2.contains("UseRelay"));
}

#[test]
fn test_dynamic_relay_address_resolution_from_host_header() {
    let state_dynamic = AppState::new_dynamic();

    // 1. IP in Host header -> resolves to that IP on port 9001
    let resolved = state_dynamic.resolve_relay_addr(Some("26.142.208.116:8080"));
    assert_eq!(resolved, "26.142.208.116:9001".parse::<SocketAddr>().unwrap());

    let resolved_lan = state_dynamic.resolve_relay_addr(Some("192.168.1.50:8080"));
    assert_eq!(resolved_lan, "192.168.1.50:9001".parse::<SocketAddr>().unwrap());

    // 2. Localhost or non-IP in Host header -> falls back to loopback 127.0.0.1:9001
    let resolved_lh = state_dynamic.resolve_relay_addr(Some("localhost:8080"));
    assert_eq!(resolved_lh, "127.0.0.1:9001".parse::<SocketAddr>().unwrap());

    let resolved_none = state_dynamic.resolve_relay_addr(None);
    assert_eq!(resolved_none, "127.0.0.1:9001".parse::<SocketAddr>().unwrap());

    // 3. Explicitly configured relay address overrides Host header
    let configured_addr: SocketAddr = "10.0.0.1:9999".parse().unwrap();
    let state_configured = AppState::new(configured_addr);
    let resolved_override = state_configured.resolve_relay_addr(Some("26.142.208.116:8080"));
    assert_eq!(resolved_override, configured_addr);
}

#[tokio::test]
async fn test_skip_punch_host_registration_and_join() {
    let tcp_listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let ws_addr = tcp_listener.local_addr().unwrap();
    let state = AppState::new_dynamic().with_skip_punch(true);
    let app = create_router(state.clone());

    tokio::spawn(async move {
        axum::serve(
            tcp_listener,
            app.into_make_service_with_connect_info::<SocketAddr>(),
        )
        .await
        .unwrap();
    });

    // Connect host WS
    let ws_url = format!("ws://{ws_addr}/ws");
    let (host_stream, _) = connect_async(&ws_url).await.unwrap();
    let (mut host_tx, mut host_rx) = host_stream.split();

    // Register host WITHOUT sending any UDP punch packet
    let host_punch_token = uuid::Uuid::new_v4();
    let register_msg = ClientMessage::RegisterHost {
        name: "SkipPunchRoom".into(),
        max_players: 4,
        game_version: "0.1.0".into(),
        udp_port: Some(5029),
        punch_token: host_punch_token,
    };
    host_tx
        .send(Message::Text(serde_json::to_string(&register_msg).unwrap()))
        .await
        .unwrap();

    // Host should be registered immediately
    let host_reply = host_rx.next().await.unwrap().unwrap();
    let Message::Text(h_text) = host_reply else { panic!("expected text") };
    let HubMessage::Registered { server_id } = serde_json::from_str(&h_text).unwrap() else {
        panic!("expected Registered, got: {h_text}");
    };

    // Verify server record in state
    let record = state.servers.get(&server_id).expect("server registered");
    assert_eq!(record.info.name, "SkipPunchRoom");
    assert_eq!(record.public_udp_addr.port(), 5029);

    // Connect client WS
    let (client_stream, _) = connect_async(&ws_url).await.unwrap();
    let (mut client_tx, mut client_rx) = client_stream.split();

    // Send JoinRequest WITHOUT sending any UDP punch packet
    let client_punch_token = uuid::Uuid::new_v4();
    let join_msg = ClientMessage::JoinRequest {
        server_id,
        punch_token: client_punch_token,
        udp_port: None,
    };
    client_tx
        .send(Message::Text(serde_json::to_string(&join_msg).unwrap()))
        .await
        .unwrap();

    // Both peers should receive PeerCandidate immediately
    let h_candidate = host_rx.next().await.unwrap().unwrap();
    let Message::Text(h_cand_text) = h_candidate else { panic!("expected text") };
    let HubMessage::PeerCandidate { peer_addr: c_addr, punch_token: p1 } = serde_json::from_str(&h_cand_text).unwrap() else {
        panic!("expected PeerCandidate on host, got: {h_cand_text}");
    };
    assert_eq!(p1, client_punch_token);
    assert_eq!(c_addr.port(), 5029);

    let c_candidate = client_rx.next().await.unwrap().unwrap();
    let Message::Text(c_cand_text) = c_candidate else { panic!("expected text") };
    let HubMessage::PeerCandidate { peer_addr, punch_token: p2 } = serde_json::from_str(&c_cand_text).unwrap() else {
        panic!("expected PeerCandidate on client, got: {c_cand_text}");
    };
    assert_eq!(p2, client_punch_token);
    assert_eq!(peer_addr, record.public_udp_addr);
}

#[tokio::test]
async fn test_skip_punch_relay_fallback_direct_ip_and_ws_tunneling() {
    let tcp_listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let ws_addr = tcp_listener.local_addr().unwrap();
    let state = AppState::new_dynamic().with_skip_punch(true);
    let app = create_router(state.clone());

    tokio::spawn(async move {
        axum::serve(
            tcp_listener,
            app.into_make_service_with_connect_info::<SocketAddr>(),
        )
        .await
        .unwrap();
    });

    let ws_url = format!("ws://{ws_addr}/ws");
    let (host_stream, _) = connect_async(&ws_url).await.unwrap();
    let (mut host_tx, mut host_rx) = host_stream.split();

    // Register host with explicit udp_port 5029
    let host_punch_token = uuid::Uuid::new_v4();
    let register_msg = ClientMessage::RegisterHost {
        name: "DirectFallbackRoom".into(),
        max_players: 4,
        game_version: "0.1.0".into(),
        udp_port: Some(5029),
        punch_token: host_punch_token,
    };
    host_tx
        .send(Message::Text(serde_json::to_string(&register_msg).unwrap()))
        .await
        .unwrap();

    let host_reply = host_rx.next().await.unwrap().unwrap();
    let Message::Text(h_text) = host_reply else { panic!("expected text") };
    let HubMessage::Registered { server_id } = serde_json::from_str(&h_text).unwrap() else {
        panic!("expected Registered");
    };

    // Client connects and joins with explicit udp_port 5030
    let (client_stream, _) = connect_async(&ws_url).await.unwrap();
    let (mut client_tx, mut client_rx) = client_stream.split();

    let client_punch_token = uuid::Uuid::new_v4();
    let join_msg = ClientMessage::JoinRequest {
        server_id,
        punch_token: client_punch_token,
        udp_port: Some(5030),
    };
    client_tx
        .send(Message::Text(serde_json::to_string(&join_msg).unwrap()))
        .await
        .unwrap();

    // 1. Host receives PeerCandidate with client's reported UDP port 5030
    let h_cand_msg = host_rx.next().await.unwrap().unwrap();
    let Message::Text(h_cand_text) = h_cand_msg else { panic!("expected text") };
    let HubMessage::PeerCandidate { peer_addr: client_udp_candidate, punch_token: p1 } =
        serde_json::from_str(&h_cand_text).unwrap()
    else {
        panic!("expected PeerCandidate on host");
    };
    assert_eq!(p1, client_punch_token);
    assert_eq!(client_udp_candidate.port(), 5030);

    // Client receives PeerCandidate with host's reported UDP port 5029
    let c_cand_msg = client_rx.next().await.unwrap().unwrap();
    let Message::Text(c_cand_text) = c_cand_msg else { panic!("expected text") };
    let HubMessage::PeerCandidate { peer_addr: host_udp_candidate, punch_token: p2 } =
        serde_json::from_str(&c_cand_text).unwrap()
    else {
        panic!("expected PeerCandidate on client");
    };
    assert_eq!(p2, client_punch_token);
    assert_eq!(host_udp_candidate.port(), 5029);

    // 2. Client triggers RelayFallback
    let fallback_msg = ClientMessage::RelayFallback {
        punch_token: client_punch_token,
    };
    client_tx
        .send(Message::Text(serde_json::to_string(&fallback_msg).unwrap()))
        .await
        .unwrap();

    // In single-port mode without relay_public_addr and with skip_punch,
    // host receives UseRelay pointing to the client's direct IP endpoint (port 5030)
    let host_relay_msg = host_rx.next().await.unwrap().unwrap();
    let Message::Text(h_relay_text) = host_relay_msg else { panic!("expected text") };
    let HubMessage::UseRelay { punch_token: h_rf_token, relay_addr: h_target } =
        serde_json::from_str(&h_relay_text).unwrap()
    else {
        panic!("expected UseRelay on host, got: {h_relay_text}");
    };
    assert_eq!(h_rf_token, client_punch_token);
    assert_eq!(h_target.port(), 5030);

    // Client receives UseRelay pointing to the host's direct IP endpoint (port 5029)
    let client_relay_msg = client_rx.next().await.unwrap().unwrap();
    let Message::Text(c_relay_text) = client_relay_msg else { panic!("expected text") };
    let HubMessage::UseRelay { punch_token: c_rf_token, relay_addr: c_target } =
        serde_json::from_str(&c_relay_text).unwrap()
    else {
        panic!("expected UseRelay on client, got: {c_relay_text}");
    };
    assert_eq!(c_rf_token, client_punch_token);
    assert_eq!(c_target.port(), 5029);

    // 3. Test bidirectional WebSocket datagram tunneling
    // Host tunnels datagram to client: [16-byte punch token] + payload
    let mut host_packet = client_punch_token.as_bytes().to_vec();
    host_packet.extend_from_slice(b"Sonic R game payload from host");
    host_tx.send(Message::Binary(host_packet.clone())).await.unwrap();

    let client_bin_msg = client_rx.next().await.unwrap().unwrap();
    let Message::Binary(client_received_bytes) = client_bin_msg else {
        panic!("expected Binary message on client WS");
    };
    assert_eq!(client_received_bytes, host_packet);

    // Client tunnels datagram to host
    let mut client_packet = client_punch_token.as_bytes().to_vec();
    client_packet.extend_from_slice(b"Sonic R game payload from client");
    client_tx.send(Message::Binary(client_packet.clone())).await.unwrap();

    let host_bin_msg = host_rx.next().await.unwrap().unwrap();
    let Message::Binary(host_received_bytes) = host_bin_msg else {
        panic!("expected Binary message on host WS");
    };
    assert_eq!(host_received_bytes, client_packet);
}



