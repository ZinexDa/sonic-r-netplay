use dashmap::DashMap;
use proto::{PunchToken, ServerId, ServerInfo};
use std::{
    net::{IpAddr, SocketAddr, ToSocketAddrs},
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
pub const PAIRING_TTL: Duration = Duration::from_secs(3600);

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
    pub last_activity: Instant,
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

/// Resolves an address or hostname string with a default port into a [`SocketAddr`].
///
/// Supports:
/// - Socket addresses: `"1.2.3.4:9001"`, `"[::1]:9001"`
/// - IP addresses: `"1.2.3.4"`, `"::1"` (uses `default_port`)
/// - Hostnames with port: `"de-bots3.h1cloud.net:9001"`
/// - Hostnames without port: `"de-bots3.h1cloud.net"` (uses `default_port`)
/// - URLs: `"http://de-bots3.h1cloud.net:9001/ws"` -> resolves `"de-bots3.h1cloud.net:9001"`
pub fn resolve_addr_or_host(addr_str: &str, default_port: u16) -> Option<SocketAddr> {
    let mut s = addr_str.trim();
    if s.is_empty() {
        return None;
    }

    // Strip scheme if present (e.g. "http://", "https://", "ws://", "wss://")
    if let Some(pos) = s.find("://") {
        s = &s[pos + 3..];
    }
    // Strip path or query if present (e.g. "example.com:9001/ws")
    if let Some(pos) = s.find(['/', '?', '#']) {
        s = &s[..pos];
    }

    // 1. Direct SocketAddr parse
    if let Ok(addr) = s.parse::<SocketAddr>() {
        return Some(addr);
    }

    // 2. Direct IpAddr parse (appends default_port)
    if let Ok(ip) = s.parse::<IpAddr>() {
        return Some(SocketAddr::new(ip, default_port));
    }

    // 3. Handle hostname with optional port
    let (host, port) = if s.starts_with('[') {
        if let Some(close_idx) = s.find(']') {
            let host = &s[1..close_idx];
            let port = if s[close_idx + 1..].starts_with(':') {
                s[close_idx + 2..].parse::<u16>().ok().unwrap_or(default_port)
            } else {
                default_port
            };
            (host, port)
        } else {
            (s, default_port)
        }
    } else if let Some(colon_idx) = s.rfind(':') {
        let host = &s[..colon_idx];
        let port_part = &s[colon_idx + 1..];
        if let Ok(p) = port_part.parse::<u16>() {
            (host, p)
        } else {
            (s, default_port)
        }
    } else {
        (s, default_port)
    };

    // DNS lookup: prefer IPv4 for Sonic R netplay compatibility
    if let Ok(addrs) = (host, port).to_socket_addrs() {
        let addrs: Vec<SocketAddr> = addrs.collect();
        if let Some(v4) = addrs.iter().find(|a| a.is_ipv4()) {
            return Some(*v4);
        }
        if let Some(first) = addrs.first() {
            return Some(*first);
        }
    }

    None
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
    pub single_port: bool,
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
            single_port: false,
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
            single_port: false,
        }
    }

    /// Sets whether UDP punch verification is skipped (for single-port/cloud environments).
    pub fn with_skip_punch(mut self, skip_punch: bool) -> Self {
        self.skip_punch = skip_punch;
        self
    }

    /// Sets whether single-port mode is active (suppressing UDP listeners and forcing WS datagram tunnel).
    pub fn with_single_port(mut self, single_port: bool) -> Self {
        self.single_port = single_port;
        if single_port {
            self.skip_punch = true;
        }
        self
    }

    /// Resolves the relay public socket address advertised to a connecting peer.
    ///
    /// 1. If single-port mode is active, returns loopback `127.0.0.1:9001` so peers route
    ///    all game datagrams over the active WebSocket connection.
    /// 2. If an explicit `relay_public_addr` was configured, returns it.
    /// 3. If `host_header` is provided (e.g. from an incoming HTTP request
    ///    `Host: de-bots3.h1cloud.net:8080` or `Host: 26.142.208.116:8080`),
    ///    resolves the host to an IP address on relay port 9001.
    /// 4. Otherwise, falls back to `127.0.0.1:9001`.
    pub fn resolve_relay_addr(&self, host_header: Option<&str>) -> SocketAddr {
        if self.single_port {
            return SocketAddr::from(([127, 0, 0, 1], 9001));
        }

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

            if let Some(addr) = resolve_addr_or_host(host_part, 9001) {
                return addr;
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

        // 4. Evict stale pairing records idle for longer than PAIRING_TTL.
        self.pairings.retain(|token, pairing| {
            if now.duration_since(pairing.last_activity) > PAIRING_TTL {
                tracing::debug!(%token, "Reaped stale pairing record");
                false
            } else {
                true
            }
        });
    }
}
