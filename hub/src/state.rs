use dashmap::DashMap;
use proto::{PunchToken, ServerId, ServerInfo};
use std::{
    net::SocketAddr,
    sync::Arc,
    time::{Duration, Instant},
};
use tokio::sync::mpsc;

/// Inactivity timeout for game host servers.
/// If a host does not send any message/heartbeat for > 15 seconds,
/// it is removed from the active registry by the periodic reaper.
pub const HEARTBEAT_TIMEOUT: Duration = Duration::from_secs(15);

/// Timeout for awaiting a UDP punch packet matching a given token.
pub const PUNCH_TIMEOUT: Duration = Duration::from_secs(5);

/// Time-to-live for cached punch addresses before being swept.
pub const PUNCH_TTL: Duration = Duration::from_secs(60);

/// Inactivity time-to-live for idle relay sessions before being swept.
pub const RELAY_TTL: Duration = Duration::from_secs(30);

/// Time-to-live for peer pairing channels before being swept.
pub const PAIRING_TTL: Duration = Duration::from_secs(120);

use axum::extract::ws::Message;

/// Active server registration stored in memory.
pub struct ServerRecord {
    pub info: ServerInfo,
    pub public_udp_addr: SocketAddr,
    pub out_tx: mpsc::UnboundedSender<Message>,
    pub relay_addr: SocketAddr,
    pub peer_addr: SocketAddr,
    pub last_heartbeat: Instant,
}

/// Pairing session between a host and a joining client, keyed by punch_token.
pub struct PairSession {
    pub host_tx: mpsc::UnboundedSender<Message>,
    pub client_tx: mpsc::UnboundedSender<Message>,
    pub host_addr: SocketAddr,
    pub client_addr: SocketAddr,
    pub host_peer_addr: SocketAddr,
    pub client_peer_addr: SocketAddr,
    pub host_relay_addr: SocketAddr,
    pub client_relay_addr: SocketAddr,
    pub created_at: Instant,
}

/// Packet queued in a relay session while awaiting the other peer to connect.
#[derive(Clone, Debug)]
pub struct QueuedRelayPacket {
    pub from: SocketAddr,
    pub data: Vec<u8>,
}

/// Dynamic slots holding the two peer endpoints in a relay session.
pub struct RelaySlots {
    pub slot_a: Option<SocketAddr>,
    pub slot_b: Option<SocketAddr>,
    pub pending_packets: Vec<QueuedRelayPacket>,
    pub last_seen: Instant,
}

#[derive(Clone)]
pub struct AppState {
    pub servers: Arc<DashMap<ServerId, ServerRecord>>,
    pub punch_addrs: Arc<DashMap<PunchToken, (SocketAddr, Instant)>>,
    pub punch_waiters: Arc<DashMap<PunchToken, mpsc::UnboundedSender<SocketAddr>>>,
    pub pairings: Arc<DashMap<PunchToken, PairSession>>,
    pub relay_sessions: Arc<DashMap<PunchToken, RelaySlots>>,
    pub relay_public_addr: Option<SocketAddr>,
    pub skip_punch: bool,
}

impl Default for AppState {
    fn default() -> Self {
        Self::new_dynamic()
    }
}

impl AppState {
    pub fn new(relay_public_addr: SocketAddr) -> Self {
        Self {
            servers: Arc::new(DashMap::new()),
            punch_addrs: Arc::new(DashMap::new()),
            punch_waiters: Arc::new(DashMap::new()),
            pairings: Arc::new(DashMap::new()),
            relay_sessions: Arc::new(DashMap::new()),
            relay_public_addr: Some(relay_public_addr),
            skip_punch: false,
        }
    }

    pub fn new_dynamic() -> Self {
        Self {
            servers: Arc::new(DashMap::new()),
            punch_addrs: Arc::new(DashMap::new()),
            punch_waiters: Arc::new(DashMap::new()),
            pairings: Arc::new(DashMap::new()),
            relay_sessions: Arc::new(DashMap::new()),
            relay_public_addr: None,
            skip_punch: false,
        }
    }

    /// Sets whether UDP punch verification is skipped (for single-port/cloud environments).
    pub fn with_skip_punch(mut self, skip_punch: bool) -> Self {
        self.skip_punch = skip_punch;
        self
    }

    /// Resolves the relay public socket address advertised to a connecting peer.
    ///
    /// 1. If an explicit `relay_public_addr` was configured, returns it.
    /// 2. If `host_header` contains an IP address (e.g. from an incoming HTTP request
    ///    `Host: 26.142.208.116:8080`), returns that IP on relay port 9001.
    /// 3. Otherwise, falls back to `127.0.0.1:9001`.
    pub fn resolve_relay_addr(&self, host_header: Option<&str>) -> SocketAddr {
        if let Some(addr) = self.relay_public_addr {
            return addr;
        }

        if let Some(host) = host_header {
            let host_part = if host.starts_with('[') {
                if let Some(end) = host.find(']') {
                    &host[1..end]
                } else {
                    host
                }
            } else {
                host.split(':').next().unwrap_or(host)
            };

            if let Ok(ip) = host_part.parse::<std::net::IpAddr>() {
                return SocketAddr::new(ip, 9001);
            }
        }

        SocketAddr::from(([127, 0, 0, 1], 9001))
    }

    /// Records or updates an incoming UDP punch packet.
    ///
    /// If the token already exists (e.g. client retried sending the punch packet),
    /// both the observed `SocketAddr` and the arrival `Instant` are updated.
    /// If a waiter was waiting for this token, it is immediately notified and removed.
    pub fn record_punch(&self, token: PunchToken, addr: SocketAddr) {
        self.punch_addrs.insert(token, (addr, Instant::now()));

        if let Some((_, tx)) = self.punch_waiters.remove(&token) {
            let _ = tx.send(addr);
        }
    }

    /// Periodic cleanup of stale host records, expired punch tokens, idle relay sessions, and pairings.
    pub fn reap_stale(&self) {
        let now = Instant::now();

        // 1. Evict servers whose sidecar connection has not sent any heartbeat for >15s.
        self.servers.retain(|server_id, record| {
            if now.duration_since(record.last_heartbeat) > HEARTBEAT_TIMEOUT {
                tracing::info!(
                    %server_id,
                    name = %record.info.name,
                    "Server evicted: heartbeat timed out (>15s without activity)"
                );
                false
            } else {
                true
            }
        });

        // 2. Evict expired punch tokens older than TTL.
        self.punch_addrs.retain(|_token, (_addr, recorded_at)| {
            now.duration_since(*recorded_at) <= PUNCH_TTL
        });

        // 3. Evict idle relay sessions older than RELAY_TTL.
        self.relay_sessions.retain(|token, slots| {
            if now.duration_since(slots.last_seen) > RELAY_TTL {
                tracing::debug!(%token, "Reaped idle relay session");
                false
            } else {
                true
            }
        });

        // 4. Evict stale pairing records older than PAIRING_TTL.
        self.pairings.retain(|token, pairing| {
            if now.duration_since(pairing.created_at) > PAIRING_TTL {
                tracing::debug!(%token, "Reaped stale pairing record");
                false
            } else {
                true
            }
        });
    }
}
