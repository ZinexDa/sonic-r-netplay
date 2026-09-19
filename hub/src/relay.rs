use crate::state::{AppState, RelaySlots};
use std::time::Instant;
use tokio::net::UdpSocket;

/// Runs the UDP relay forwarding listener task.
///
/// Accepts packets where the first 16 bytes contain a `PunchToken` (UUID).
/// The sender's public endpoint is matched into one of two slots (slot A or slot B)
/// for that token, and the entire packet (including the 16-byte token prefix)
/// is forwarded directly to the other peer's endpoint.
pub async fn run_relay_listener(sock: UdpSocket, state: AppState) {
    let local_addr = sock
        .local_addr()
        .map(|a| a.to_string())
        .unwrap_or_else(|_| "unknown".to_string());
    tracing::info!(%local_addr, "UDP relay listener is running");

    let mut buf = [0u8; 2048];

    loop {
        let (n, from) = match sock.recv_from(&mut buf).await {
            Ok(res) => res,
            Err(err) => {
                tracing::warn!(%err, "Error receiving packet on UDP relay listener");
                continue;
            }
        };

        if n < 17 {
            tracing::debug!(
                %from,
                len = n,
                "Ignoring relay packet: payload shorter than 17-byte prefix (16B token + 1B msg_type)"
            );
            continue;
        }

        let token = match uuid::Uuid::from_slice(&buf[..16]) {
            Ok(t) => t,
            Err(err) => {
                tracing::debug!(%from, %err, "Ignoring relay packet: invalid UUID token prefix");
                continue;
            }
        };

        let (dest, packets_to_flush) = {
            let mut entry = state.relay_sessions.entry(token).or_insert_with(|| RelaySlots {
                slot_a: None,
                slot_b: None,
                pending_packets: Vec::new(),
                last_seen: Instant::now(),
            });

            entry.last_seen = Instant::now();

            let is_sender_a = if entry.slot_a == Some(from) {
                Some(true)
            } else if entry.slot_b == Some(from) {
                Some(false)
            } else if entry.slot_a.is_none() {
                entry.slot_a = Some(from);
                tracing::info!(%token, %from, "Assigned peer to relay slot A");
                Some(true)
            } else if entry.slot_b.is_none() {
                entry.slot_b = Some(from);
                tracing::info!(%token, %from, "Assigned peer to relay slot B");
                Some(false)
            } else {
                // Both slots occupied. Check for port migration or loopback/LAN shift.
                let slot_a_addr = entry.slot_a.unwrap();
                let slot_b_addr = entry.slot_b.unwrap();

                if slot_a_addr.ip() != slot_b_addr.ip() {
                    if from.ip() == slot_a_addr.ip()
                        || (from.ip().is_loopback() && slot_a_addr.ip().is_loopback())
                    {
                        tracing::info!(
                            %token,
                            old = %slot_a_addr,
                            new = %from,
                            "Peer A port/interface migrated; updated slot A"
                        );
                        entry.slot_a = Some(from);
                        Some(true)
                    } else if from.ip() == slot_b_addr.ip()
                        || (from.ip().is_loopback() && slot_b_addr.ip().is_loopback())
                    {
                        tracing::info!(
                            %token,
                            old = %slot_b_addr,
                            new = %from,
                            "Peer B port/interface migrated; updated slot B"
                        );
                        entry.slot_b = Some(from);
                        Some(false)
                    } else {
                        tracing::warn!(
                            %token,
                            %from,
                            %slot_a_addr,
                            %slot_b_addr,
                            "Received relay packet from unexpected 3rd address for active session; dropping"
                        );
                        None
                    }
                } else {
                    tracing::warn!(
                        %token,
                        %from,
                        %slot_a_addr,
                        %slot_b_addr,
                        "Received relay packet from unknown port on same IP; dropping"
                    );
                    None
                }
            };

            let dest = match is_sender_a {
                Some(true) => entry.slot_b,
                Some(false) => entry.slot_a,
                None => None,
            };

            let mut flush = Vec::new();
            if entry.slot_a.is_some() && entry.slot_b.is_some() && !entry.pending_packets.is_empty() {
                let slot_a = entry.slot_a.unwrap();
                let slot_b = entry.slot_b.unwrap();
                for queued in entry.pending_packets.drain(..) {
                    let target = if queued.from == slot_a
                        || (slot_a.ip() != slot_b.ip() && queued.from.ip() == slot_a.ip())
                    {
                        slot_b
                    } else {
                        slot_a
                    };
                    flush.push((target, queued.data));
                }
            }

            if is_sender_a.is_some() && dest.is_none() {
                if entry.pending_packets.len() >= 64 {
                    entry.pending_packets.remove(0);
                }
                entry.pending_packets.push(crate::state::QueuedRelayPacket {
                    from,
                    data: buf[..n].to_vec(),
                });
                tracing::debug!(
                    %token,
                    %from,
                    pending_count = entry.pending_packets.len(),
                    "Buffered relay packet awaiting other peer in session"
                );
            }

            (dest, flush)
        };

        for (target, data) in packets_to_flush {
            tracing::info!(
                %token,
                %target,
                len = data.len(),
                "Flushing buffered relay packet to newly connected peer"
            );
            if let Err(err) = sock.send_to(&data, target).await {
                tracing::warn!(%target, %err, "Failed to flush buffered relay packet");
            }
        }

        if let Some(dest_addr) = dest {
            tracing::debug!(
                %token,
                %from,
                %dest_addr,
                payload_len = n - 16,
                "Relaying packet between peers"
            );
            if let Err(err) = sock.send_to(&buf[..n], dest_addr).await {
                tracing::warn!(%dest_addr, %err, "Failed to relay packet to destination");
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::Duration;

    #[tokio::test]
    async fn test_relay_forwarding_between_two_peers() {
        let relay_sock = UdpSocket::bind("127.0.0.1:0").await.unwrap();
        let relay_addr = relay_sock.local_addr().unwrap();

        let state = AppState::new(relay_addr);
        tokio::spawn(run_relay_listener(relay_sock, state));

        let peer1 = UdpSocket::bind("127.0.0.1:0").await.unwrap();
        let peer2 = UdpSocket::bind("127.0.0.1:0").await.unwrap();

        let token = uuid::Uuid::new_v4();

        // 1. Peer1 sends first packet to relay (prefixed with token + msg_type 0x01)
        let mut msg1 = Vec::new();
        msg1.extend_from_slice(token.as_bytes());
        msg1.push(0x01);
        msg1.extend_from_slice(b"hello from peer 1");
        peer1.send_to(&msg1, relay_addr).await.unwrap();

        // Small yield so relay registers slot A
        tokio::time::sleep(Duration::from_millis(50)).await;

        // 2. Peer2 sends packet to relay (should be forwarded to peer1)
        let mut msg2 = Vec::new();
        msg2.extend_from_slice(token.as_bytes());
        msg2.push(0x01);
        msg2.extend_from_slice(b"hello from peer 2");
        peer2.send_to(&msg2, relay_addr).await.unwrap();

        // 3. Peer1 receives forwarded packet from peer2
        let mut recv1_buf = [0u8; 64];
        let (n1, from1) = peer1.recv_from(&mut recv1_buf).await.unwrap();
        assert_eq!(from1, relay_addr);
        assert_eq!(&recv1_buf[..16], token.as_bytes());
        assert_eq!(recv1_buf[16], 0x01);
        assert_eq!(&recv1_buf[17..n1], b"hello from peer 2");

        // 4. Now that peer2 is registered in slot B, peer1 sends another packet
        peer1.send_to(&msg1, relay_addr).await.unwrap();

        // 5. Peer2 receives forwarded packet from peer1
        let mut recv2_buf = [0u8; 64];
        let (n2, from2) = peer2.recv_from(&mut recv2_buf).await.unwrap();
        assert_eq!(from2, relay_addr);
        assert_eq!(&recv2_buf[..16], token.as_bytes());
        assert_eq!(recv2_buf[16], 0x01);
        assert_eq!(&recv2_buf[17..n2], b"hello from peer 1");
    }

    #[tokio::test]
    async fn test_relay_buffers_packets_before_peer2_connects() {
        let relay_sock = UdpSocket::bind("127.0.0.1:0").await.unwrap();
        let relay_addr = relay_sock.local_addr().unwrap();

        let state = AppState::new(relay_addr);
        tokio::spawn(run_relay_listener(relay_sock, state));

        let peer1 = UdpSocket::bind("127.0.0.1:0").await.unwrap();
        let peer2 = UdpSocket::bind("127.0.0.1:0").await.unwrap();

        let token = uuid::Uuid::new_v4();

        // 1. Peer 1 sends 3 packets before Peer 2 even exists
        for i in 1..=3 {
            let mut msg = Vec::new();
            msg.extend_from_slice(token.as_bytes());
            msg.push(0x01);
            msg.extend_from_slice(format!("buffered packet {i}").as_bytes());
            peer1.send_to(&msg, relay_addr).await.unwrap();
        }

        tokio::time::sleep(Duration::from_millis(50)).await;

        // 2. Peer 2 sends a keepalive ping (msg_type 0x00) to register with relay
        let mut ping = Vec::new();
        ping.extend_from_slice(token.as_bytes());
        ping.push(0x00);
        ping.extend_from_slice(b"PING");
        peer2.send_to(&ping, relay_addr).await.unwrap();

        // 3. Peer 2 should now immediately receive all 3 buffered packets from Peer 1
        let mut recv_buf = [0u8; 64];
        for i in 1..=3 {
            let (n, from) = tokio::time::timeout(Duration::from_secs(1), peer2.recv_from(&mut recv_buf))
                .await
                .expect("Timed out waiting for buffered packet")
                .unwrap();
            assert_eq!(from, relay_addr);
            assert_eq!(&recv_buf[..16], token.as_bytes());
            assert_eq!(recv_buf[16], 0x01);
            let expected_body = format!("buffered packet {i}");
            assert_eq!(&recv_buf[17..n], expected_body.as_bytes());
        }
    }
}
