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

        let dest = {
            let mut entry = state.relay_sessions.entry(token).or_insert_with(|| RelaySlots {
                slot_a: None,
                slot_b: None,
                last_seen: Instant::now(),
            });

            entry.last_seen = Instant::now();

            if entry.slot_a == Some(from) {
                entry.slot_b
            } else if entry.slot_b == Some(from) {
                entry.slot_a
            } else if entry.slot_a.is_none() {
                entry.slot_a = Some(from);
                tracing::info!(%token, %from, "Assigned peer to relay slot A");
                entry.slot_b
            } else if entry.slot_b.is_none() {
                entry.slot_b = Some(from);
                tracing::info!(%token, %from, "Assigned peer to relay slot B");
                entry.slot_a
            } else {
                tracing::warn!(
                    %token,
                    %from,
                    "Received relay packet from unexpected 3rd address for active session; dropping"
                );
                None
            }
        };

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
        } else {
            tracing::debug!(
                %token,
                %from,
                "Relay packet received, awaiting other peer in session"
            );
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
}
