# Hub (Signaling Server)

Central signaling and mini-STUN server for the Sonic R network multiplayer system.

## Overview

The `hub` provides:
1. **WebSocket Signaling (`0.0.0.0:8080/ws`)**:
   - Host registration (`RegisterHost`)
   - Server discovery (`ListServers`)
   - Peer join requests (`JoinRequest`)
   - Host heartbeat tracking (`Heartbeat`)
2. **Mini-STUN UDP Punch Listener (`0.0.0.0:9000`)**:
   - Accepts 16-byte UDP packets containing raw UUID bytes of a `PunchToken`.
   - Records the sender's public `SocketAddr` as observed outside NAT.
   - Reliably wakes up waiting registration/join tasks without race conditions.
3. **UDP Relay Fallback Listener (`0.0.0.0:9001`)**:
   - Forwards gameplay packets between peers when direct P2P hole punching fails.
   - Wire format: first 16 bytes = `PunchToken`, remaining bytes = opaque payload.
   - Dynamically pairs the two peer endpoints into slots (slot A & slot B) per token, forwarding packets with the 16-byte token prefix intact.
4. **Heartbeat & Inactivity Reaper**:
   - Scans active server registrations every 5 seconds.
   - Automatically unregisters hosts that have not sent a heartbeat for > 15 seconds.
   - Cleans up idle relay sessions (>30s) and expired punch tokens.

## Architecture

- `src/state.rs`: Thread-safe server registry, punch cache, pairing sessions, relay slots (`DashMap`), and reaper logic.
- `src/punch.rs`: Mini-STUN UDP listener task and race-free `await_punch_addr` function.
- `src/relay.rs`: UDP relay packet forwarding listener task and session slot management.
- `src/ws.rs`: Axum WebSocket upgrade, message routing (`RegisterHost`, `ListServers`, `JoinRequest`, `Heartbeat`, `RelayFallback`), error resilience, and candidate/relay coordination.
- `src/main.rs`: Entry point, configuration, and task supervisor.

## Configuration & Environment Variables

- `--relay-public-addr <IP:PORT>`: CLI argument specifying the public address of the UDP relay handed to clients in `UseRelay`.
- `RELAY_PUBLIC_ADDR`: (or `HUB_PUBLIC_ADDR`) Environment variable specifying the public `IP:PORT` address of the UDP relay (e.g. `203.0.113.10:9001` or `26.142.208.116:9001`).
- **Dynamic Interface Fallback**: If no explicit relay public address is provided via CLI or environment variable, the hub dynamically resolves the relay address per connection based on the HTTP `Host` header (e.g., if a client connects to `ws://26.142.208.116:8080/ws`, the hub advertises `26.142.208.116:9001`). If the host is loopback or localhost, it falls back to `127.0.0.1:9001`.

## Running Locally

```bash
# Dynamic interface detection (or loopback 127.0.0.1:9001 for local connections)
cargo run -p hub

# With explicit CLI flag
cargo run -p hub -- --relay-public-addr 26.142.208.116:9001

# With environment variable
RELAY_PUBLIC_ADDR=26.142.208.116:9001 cargo run -p hub
```
