use crate::state::{AppState, PUNCH_TIMEOUT};
use proto::PunchToken;
use std::net::SocketAddr;
use tokio::{net::UdpSocket, sync::mpsc};

/// Runs the mini-STUN UDP listener task.
///
/// Listens for incoming 16-byte UDP packets containing raw UUID bytes (`Uuid::as_bytes()`).
/// When a valid punch packet arrives, records the sender's public `SocketAddr` mapped
/// to the token in `state`, and notifies any active waiter awaiting that token.
pub async fn run_punch_listener(addr: SocketAddr, state: AppState) {
    let sock = match UdpSocket::bind(addr).await {
        Ok(s) => s,
        Err(err) => {
            tracing::error!(%addr, %err, "Failed to bind punch UDP socket");
            return;
        }
    };

    tracing::info!(local_addr = %addr, "UDP punch listener is running");
    let mut buf = [0u8; 64];

    loop {
        let (n, from) = match sock.recv_from(&mut buf).await {
            Ok(res) => res,
            Err(err) => {
                tracing::warn!(%err, "Error receiving packet on UDP punch listener");
                continue;
            }
        };

        if n != 16 {
            tracing::debug!(
                %from,
                bytes_len = n,
                "Ignoring UDP packet: invalid payload size (expected 16 bytes for UUID)"
            );
            continue;
        }

        let token = match uuid::Uuid::from_slice(&buf[..16]) {
            Ok(t) => t,
            Err(err) => {
                tracing::debug!(
                    %from,
                    %err,
                    "Ignoring UDP packet: 16 bytes could not be parsed as UUID"
                );
                continue;
            }
        };

        tracing::debug!(%token, %from, "Received UDP punch packet");
        state.record_punch(token, from);
    }
}

/// Waits for a public `SocketAddr` associated with the given `PunchToken`.
///
/// # Race Condition Handling
/// When a peer initiates hole punching, it sends:
/// 1. A raw 16-byte UDP packet containing `punch_token` to the hub's UDP port.
/// 2. A JSON message (`RegisterHost` or `JoinRequest`) over WebSocket.
///
/// Because UDP and TCP/WebSocket travel over different sockets and network paths,
/// the UDP packet might arrive:
/// - **Before** this function is invoked (already cached in `punch_addrs`).
/// - **After** this function is invoked (needs to be awaited asynchronously).
/// - **Concurrently** while this function is executing.
///
/// To prevent any lost wakeup or dead-lock race:
/// 1. We create and register an `mpsc` waiter channel in `punch_waiters` *first*.
/// 2. We then immediately inspect `punch_addrs` to see if the address has already arrived.
///    If it is present, we remove the registered waiter and return the address immediately.
/// 3. If it was not present during the check, any incoming UDP packet will find the registered
///    waiter in `punch_waiters` and send the address across the channel.
/// 4. If the packet does not arrive within `PUNCH_TIMEOUT` (~5s), the timeout expires,
///    the waiter is removed from `punch_waiters`, and `None` is returned.
pub async fn await_punch_addr(state: &AppState, token: PunchToken) -> Option<SocketAddr> {
    // 1. Register waiter channel first so no incoming packet can slip through
    let (tx, mut rx) = mpsc::unbounded_channel();
    state.punch_waiters.insert(token, tx);

    // 2. Check if address already arrived prior to or during waiter registration
    if let Some(entry) = state.punch_addrs.get(&token) {
        let addr = entry.0;
        state.punch_waiters.remove(&token);
        return Some(addr);
    }

    // 3. Await incoming packet notification via channel with timeout
    let result = match tokio::time::timeout(PUNCH_TIMEOUT, rx.recv()).await {
        Ok(Some(addr)) => Some(addr),
        _ => None,
    };

    // 4. Ensure cleanup of waiter mapping on completion or timeout
    state.punch_waiters.remove(&token);
    result
}
