use sidecar::loopback::{
    run_tunnel_session_with_socket, MSG_TYPE_GAME_DATA, MSG_TYPE_KEEPALIVE,
};
use std::{net::SocketAddr, sync::Arc, time::Duration};
use tokio::net::UdpSocket;

#[tokio::test]
async fn test_loopback_proxy_bidirectional_forwarding() {
    let tunnel_sock_a = Arc::new(UdpSocket::bind("127.0.0.1:0").await.unwrap());
    let tunnel_sock_b = Arc::new(UdpSocket::bind("127.0.0.1:0").await.unwrap());

    let tunnel_addr_a = tunnel_sock_a.local_addr().unwrap();
    let tunnel_addr_b = tunnel_sock_b.local_addr().unwrap();

    let proxy_game_sock_a = UdpSocket::bind("127.0.0.1:0").await.unwrap();
    let proxy_game_sock_b = UdpSocket::bind("127.0.0.1:0").await.unwrap();

    let proxy_game_addr_a = proxy_game_sock_a.local_addr().unwrap();
    let proxy_game_addr_b = proxy_game_sock_b.local_addr().unwrap();

    let game_client_a = UdpSocket::bind("127.0.0.1:0").await.unwrap();
    let game_client_b = UdpSocket::bind("127.0.0.1:0").await.unwrap();

    let (relay_tx_a, _) = tokio::sync::broadcast::channel::<SocketAddr>(4);
    let (relay_tx_b, _) = tokio::sync::broadcast::channel::<SocketAddr>(4);

    let punch_token = uuid::Uuid::new_v4();

    let t1_sock = tunnel_sock_a.clone();
    let h1 = tokio::spawn(async move {
        let _ = run_tunnel_session_with_socket(
            &t1_sock,
            tunnel_addr_b,
            punch_token,
            proxy_game_sock_a,
            None,
            false,
            true,
            relay_tx_a.subscribe(),
        )
        .await;
    });

    let t2_sock = tunnel_sock_b.clone();
    let h2 = tokio::spawn(async move {
        let _ = run_tunnel_session_with_socket(
            &t2_sock,
            tunnel_addr_a,
            punch_token,
            proxy_game_sock_b,
            None,
            false,
            true,
            relay_tx_b.subscribe(),
        )
        .await;
    });

    // 1. Initial handshake so both proxies learn their local game client addresses
    game_client_a
        .send_to(b"INIT_A", proxy_game_addr_a)
        .await
        .unwrap();
    game_client_b
        .send_to(b"INIT_B", proxy_game_addr_b)
        .await
        .unwrap();

    // Drain the init packets across both ends
    let mut buf = [0u8; 128];
    let (n_b, _) = game_client_b.recv_from(&mut buf).await.unwrap();
    assert_eq!(&buf[..n_b], b"INIT_A");

    let (n_a, _) = game_client_a.recv_from(&mut buf).await.unwrap();
    assert_eq!(&buf[..n_a], b"INIT_B");

    // 2. Test Client A -> Client B forwarding
    let payload_a = b"SONIC_R_STATE_POS_X_100_Y_200";
    game_client_a
        .send_to(payload_a, proxy_game_addr_a)
        .await
        .unwrap();

    let (n, from) = game_client_b.recv_from(&mut buf).await.unwrap();
    assert_eq!(&buf[..n], payload_a);
    assert_eq!(from, proxy_game_addr_b);

    // 3. Test Client B -> Client A forwarding
    let payload_b = b"TAILS_R_STATE_POS_X_300_Y_400";
    game_client_b
        .send_to(payload_b, proxy_game_addr_b)
        .await
        .unwrap();

    let (n, from) = game_client_a.recv_from(&mut buf).await.unwrap();
    assert_eq!(&buf[..n], payload_b);
    assert_eq!(from, proxy_game_addr_a);

    h1.abort();
    h2.abort();
}

#[tokio::test]
async fn test_keepalive_not_forwarded_to_game() {
    let tunnel_sock = Arc::new(UdpSocket::bind("127.0.0.1:0").await.unwrap());
    let tunnel_addr = tunnel_sock.local_addr().unwrap();

    let remote_sock = UdpSocket::bind("127.0.0.1:0").await.unwrap();
    let remote_addr = remote_sock.local_addr().unwrap();

    let proxy_game_sock = UdpSocket::bind("127.0.0.1:0").await.unwrap();
    let proxy_game_addr = proxy_game_sock.local_addr().unwrap();

    let game_client = UdpSocket::bind("127.0.0.1:0").await.unwrap();

    let (relay_tx, _) = tokio::sync::broadcast::channel::<SocketAddr>(4);
    let punch_token = uuid::Uuid::new_v4();

    let t_sock = tunnel_sock.clone();
    let handle = tokio::spawn(async move {
        let _ = run_tunnel_session_with_socket(
            &t_sock,
            remote_addr,
            punch_token,
            proxy_game_sock,
            None,
            false,
            true,
            relay_tx.subscribe(),
        )
        .await;
    });

    // Game client sends registration datagram
    game_client.send_to(b"HELLO", proxy_game_addr).await.unwrap();

    // Remote receives initial tunnel packet
    let mut remote_buf = [0u8; 128];
    let (rn, _) = remote_sock.recv_from(&mut remote_buf).await.unwrap();
    assert_eq!(rn, 16 + 1 + 5);
    assert_eq!(remote_buf[16], MSG_TYPE_GAME_DATA);
    assert_eq!(&remote_buf[17..rn], b"HELLO");

    // Remote sends keepalive packet (0x00)
    let mut keepalive_pkt = Vec::new();
    keepalive_pkt.extend_from_slice(punch_token.as_bytes());
    keepalive_pkt.push(MSG_TYPE_KEEPALIVE);
    keepalive_pkt.extend_from_slice(b"PING");
    remote_sock
        .send_to(&keepalive_pkt, tunnel_addr)
        .await
        .unwrap();

    // Verify game client receives NOTHING from keepalive
    let mut client_buf = [0u8; 128];
    let recv_timeout = tokio::time::timeout(
        Duration::from_millis(200),
        game_client.recv_from(&mut client_buf),
    )
    .await;
    assert!(
        recv_timeout.is_err(),
        "Game client must not receive keepalive packets"
    );

    // Now send game data (0x01)
    let mut game_pkt = Vec::new();
    game_pkt.extend_from_slice(punch_token.as_bytes());
    game_pkt.push(MSG_TYPE_GAME_DATA);
    game_pkt.extend_from_slice(b"REAL_GAME_DATA");
    remote_sock.send_to(&game_pkt, tunnel_addr).await.unwrap();

    let (n, from) = game_client.recv_from(&mut client_buf).await.unwrap();
    assert_eq!(&client_buf[..n], b"REAL_GAME_DATA");
    assert_eq!(from, proxy_game_addr);

    handle.abort();
}

#[tokio::test]
async fn test_unregistered_game_packet_dropped() {
    let tunnel_sock = Arc::new(UdpSocket::bind("127.0.0.1:0").await.unwrap());
    let tunnel_addr = tunnel_sock.local_addr().unwrap();

    let remote_sock = UdpSocket::bind("127.0.0.1:0").await.unwrap();
    let remote_addr = remote_sock.local_addr().unwrap();

    let proxy_game_sock = UdpSocket::bind("127.0.0.1:0").await.unwrap();
    let proxy_game_addr = proxy_game_sock.local_addr().unwrap();

    let (relay_tx, _) = tokio::sync::broadcast::channel::<SocketAddr>(4);
    let punch_token = uuid::Uuid::new_v4();

    let t_sock = tunnel_sock.clone();
    let handle = tokio::spawn(async move {
        let _ = run_tunnel_session_with_socket(
            &t_sock,
            remote_addr,
            punch_token,
            proxy_game_sock,
            None,
            false,
            true,
            relay_tx.subscribe(),
        )
        .await;
    });

    // Remote sends game data BEFORE any local game process has communicated
    let mut early_pkt = Vec::new();
    early_pkt.extend_from_slice(punch_token.as_bytes());
    early_pkt.push(MSG_TYPE_GAME_DATA);
    early_pkt.extend_from_slice(b"EARLY_DATA");
    remote_sock.send_to(&early_pkt, tunnel_addr).await.unwrap();

    // Small delay to allow proxy to process and drop cleanly
    tokio::time::sleep(Duration::from_millis(50)).await;

    // Now local game client starts up and sends packet
    let game_client = UdpSocket::bind("127.0.0.1:0").await.unwrap();
    game_client.send_to(b"I_AM_READY", proxy_game_addr).await.unwrap();

    let mut remote_buf = [0u8; 128];
    let (rn, _) = remote_sock.recv_from(&mut remote_buf).await.unwrap();
    assert_eq!(&remote_buf[17..rn], b"I_AM_READY");

    // Subsequent remote game packets should now be delivered successfully
    let mut late_pkt = Vec::new();
    late_pkt.extend_from_slice(punch_token.as_bytes());
    late_pkt.push(MSG_TYPE_GAME_DATA);
    late_pkt.extend_from_slice(b"LATE_DATA");
    remote_sock.send_to(&late_pkt, tunnel_addr).await.unwrap();

    let mut client_buf = [0u8; 128];
    let (cn, _) = game_client.recv_from(&mut client_buf).await.unwrap();
    assert_eq!(&client_buf[..cn], b"LATE_DATA");

    handle.abort();
}

#[tokio::test]
async fn test_relay_transition_during_game_session() {
    let tunnel_sock = Arc::new(UdpSocket::bind("127.0.0.1:0").await.unwrap());
    let tunnel_addr = tunnel_sock.local_addr().unwrap();

    let direct_peer_sock = UdpSocket::bind("127.0.0.1:0").await.unwrap();
    let direct_peer_addr = direct_peer_sock.local_addr().unwrap();

    let relay_mock_sock = UdpSocket::bind("127.0.0.1:0").await.unwrap();
    let relay_mock_addr = relay_mock_sock.local_addr().unwrap();

    let proxy_game_sock = UdpSocket::bind("127.0.0.1:0").await.unwrap();
    let proxy_game_addr = proxy_game_sock.local_addr().unwrap();

    let game_client = UdpSocket::bind("127.0.0.1:0").await.unwrap();

    let (relay_tx, _) = tokio::sync::broadcast::channel::<SocketAddr>(4);
    let relay_rx = relay_tx.subscribe();
    let punch_token = uuid::Uuid::new_v4();

    let t_sock = tunnel_sock.clone();
    let handle = tokio::spawn(async move {
        let _ = run_tunnel_session_with_socket(
            &t_sock,
            direct_peer_addr,
            punch_token,
            proxy_game_sock,
            None,
            false,
            false, // direct_punch_success = false, allowing relay transition!
            relay_rx,
        )
        .await;
    });

    // 1. Send packet in direct P2P mode
    game_client.send_to(b"DIRECT_PACKET", proxy_game_addr).await.unwrap();

    let mut buf = [0u8; 128];
    let (n, from) = direct_peer_sock.recv_from(&mut buf).await.unwrap();
    assert_eq!(from, tunnel_addr);
    assert_eq!(&buf[17..n], b"DIRECT_PACKET");

    // 2. Peer triggers fallback -> UseRelay message arrives
    relay_tx.send(relay_mock_addr).unwrap();
    tokio::time::sleep(Duration::from_millis(50)).await;

    // 3. Send packet after relay switch
    game_client.send_to(b"RELAY_PACKET", proxy_game_addr).await.unwrap();

    // Now the packet must arrive at relay_mock_sock, NOT direct_peer_sock!
    let (n2, from2) = relay_mock_sock.recv_from(&mut buf).await.unwrap();
    assert_eq!(from2, tunnel_addr);
    assert_eq!(&buf[17..n2], b"RELAY_PACKET");

    // 4. Relay returns a response back to the tunnel
    let mut resp_pkt = Vec::new();
    resp_pkt.extend_from_slice(punch_token.as_bytes());
    resp_pkt.push(MSG_TYPE_GAME_DATA);
    resp_pkt.extend_from_slice(b"RELAY_RESPONSE");
    relay_mock_sock.send_to(&resp_pkt, tunnel_addr).await.unwrap();

    let (cn, cfrom) = game_client.recv_from(&mut buf).await.unwrap();
    assert_eq!(&buf[..cn], b"RELAY_RESPONSE");
    assert_eq!(cfrom, proxy_game_addr);

    handle.abort();
}

#[tokio::test]
async fn test_host_mode_preset_target_game_addr() {
    let tunnel_sock_host = Arc::new(UdpSocket::bind("127.0.0.1:0").await.unwrap());
    let tunnel_sock_client = Arc::new(UdpSocket::bind("127.0.0.1:0").await.unwrap());

    let tunnel_addr_host = tunnel_sock_host.local_addr().unwrap();
    let tunnel_addr_client = tunnel_sock_client.local_addr().unwrap();

    // 1. Host game (e.g. Sonic R host listening on 127.0.0.1:5029)
    // Note: The game is passive and sends NO packet first!
    let host_game_sock = UdpSocket::bind("127.0.0.1:0").await.unwrap();
    let host_game_addr = host_game_sock.local_addr().unwrap();

    // Host sidecar proxy binds ephemeral port (127.0.0.1:0) and presets initial_game_addr = Some(host_game_addr)
    let host_proxy_sock = UdpSocket::bind("127.0.0.1:0").await.unwrap();
    let host_proxy_addr = host_proxy_sock.local_addr().unwrap();

    // Client sidecar proxy binds on a known port and learns dynamically (initial_game_addr = None)
    let client_proxy_sock = UdpSocket::bind("127.0.0.1:0").await.unwrap();
    let client_proxy_addr = client_proxy_sock.local_addr().unwrap();

    let client_game_sock = UdpSocket::bind("127.0.0.1:0").await.unwrap();

    let (relay_tx_host, _) = tokio::sync::broadcast::channel::<SocketAddr>(4);
    let (relay_tx_client, _) = tokio::sync::broadcast::channel::<SocketAddr>(4);
    let punch_token = uuid::Uuid::new_v4();

    let t_host = tunnel_sock_host.clone();
    let h_host = tokio::spawn(async move {
        let _ = run_tunnel_session_with_socket(
            &t_host,
            tunnel_addr_client,
            punch_token,
            host_proxy_sock,
            Some(host_game_addr),
            false,
            true,
            relay_tx_host.subscribe(),
        )
        .await;
    });

    let t_client = tunnel_sock_client.clone();
    let h_client = tokio::spawn(async move {
        let _ = run_tunnel_session_with_socket(
            &t_client,
            tunnel_addr_host,
            punch_token,
            client_proxy_sock,
            None,
            false,
            true,
            relay_tx_client.subscribe(),
        )
        .await;
    });

    // 2. Client game sends the FIRST datagram (e.g., NET_MSG_JOIN_REQ) to client proxy
    let join_req = b"NET_MSG_JOIN_REQ";
    client_game_sock
        .send_to(join_req, client_proxy_addr)
        .await
        .unwrap();

    // 3. Host game server must receive this datagram immediately from host proxy,
    // even though the host game server NEVER transmitted anything to host proxy yet!
    let mut host_game_buf = [0u8; 128];
    let (recv_len, from_addr) = host_game_sock
        .recv_from(&mut host_game_buf)
        .await
        .unwrap();
    assert_eq!(&host_game_buf[..recv_len], join_req);
    assert_eq!(from_addr, host_proxy_addr);

    // 4. Host game responds to the sender (host_proxy_addr)
    let join_ack = b"NET_MSG_JOIN_ACK";
    host_game_sock
        .send_to(join_ack, from_addr)
        .await
        .unwrap();

    // 5. Client game receives the response
    let mut client_game_buf = [0u8; 128];
    let (c_len, c_from) = client_game_sock
        .recv_from(&mut client_game_buf)
        .await
        .unwrap();
    assert_eq!(&client_game_buf[..c_len], join_ack);
    assert_eq!(c_from, client_proxy_addr);

    h_host.abort();
    h_client.abort();
}

#[tokio::test]
async fn test_direct_punch_success_ignores_userelay() {
    let tunnel_sock = Arc::new(UdpSocket::bind("127.0.0.1:0").await.unwrap());
    let tunnel_addr = tunnel_sock.local_addr().unwrap();

    let direct_peer_sock = UdpSocket::bind("127.0.0.1:0").await.unwrap();
    let direct_peer_addr = direct_peer_sock.local_addr().unwrap();

    let relay_mock_sock = UdpSocket::bind("127.0.0.1:0").await.unwrap();
    let relay_mock_addr = relay_mock_sock.local_addr().unwrap();

    let proxy_game_sock = UdpSocket::bind("127.0.0.1:0").await.unwrap();
    let proxy_game_addr = proxy_game_sock.local_addr().unwrap();

    let game_client = UdpSocket::bind("127.0.0.1:0").await.unwrap();

    let (relay_tx, _) = tokio::sync::broadcast::channel::<SocketAddr>(4);
    let relay_rx = relay_tx.subscribe();
    let punch_token = uuid::Uuid::new_v4();

    let t_sock = tunnel_sock.clone();
    let handle = tokio::spawn(async move {
        let _ = run_tunnel_session_with_socket(
            &t_sock,
            direct_peer_addr,
            punch_token,
            proxy_game_sock,
            None,
            false,
            true, // direct_punch_success = true!
            relay_rx,
        )
        .await;
    });

    // 1. Initial direct packet from game client
    game_client.send_to(b"DIRECT_PACKET", proxy_game_addr).await.unwrap();

    let mut buf = [0u8; 128];
    let (n, from) = direct_peer_sock.recv_from(&mut buf).await.unwrap();
    assert_eq!(from, tunnel_addr);
    assert_eq!(&buf[17..n], b"DIRECT_PACKET");

    // 2. An incoming UseRelay arrives (e.g. spurious fallback timeout from peer)
    relay_tx.send(relay_mock_addr).unwrap();
    tokio::time::sleep(Duration::from_millis(50)).await;

    // 3. Send second packet after UseRelay
    game_client.send_to(b"SECOND_PACKET", proxy_game_addr).await.unwrap();

    // The packet must STILL arrive at direct_peer_sock, NOT relay_mock_sock!
    let (n2, from2) = direct_peer_sock.recv_from(&mut buf).await.unwrap();
    assert_eq!(from2, tunnel_addr);
    assert_eq!(&buf[17..n2], b"SECOND_PACKET");

    // Verify relay_mock_sock receives nothing
    let mut relay_buf = [0u8; 128];
    let relay_recv = tokio::time::timeout(
        Duration::from_millis(100),
        relay_mock_sock.recv_from(&mut relay_buf),
    )
    .await;
    assert!(relay_recv.is_err(), "Relay mock should not receive packets when direct punch succeeded");

    handle.abort();
}
