# Sidecar (Host & Client CLI)

Companion process that runs alongside the game host and client to handle signaling and NAT traversal with the central `hub`.

## Overview

The `sidecar` CLI supports three modes:
1. **Host Mode (`host`)**:
   - Connects to the hub WebSocket.
   - Generates a random `punch_token` (UUID v4) and sends a 16-byte UDP packet to the hub's UDP port (`127.0.0.1:9000`).
   - Sends `RegisterHost` with the specified server name.
   - Sends periodic `Heartbeat` messages every 10 seconds to keep registration active.
   - Logs incoming `HubMessage` notifications, including `PeerCandidate` when a peer joins.
2. **Join Mode (`join`)**:
   - Connects to the hub WebSocket.
   - Requests the active server list (or uses a specified `server_id`).
   - Generates a random `punch_token` (UUID v4) and sends a 16-byte UDP packet to the hub's UDP port (`127.0.0.1:9000`).
   - Sends `JoinRequest` to the hub.
   - Awaits `PeerCandidate` containing the host's public socket address and punch token.
3. **List Mode (`list`)**:
   - Connects to the hub WebSocket, requests `ListServers`, prints online servers, and exits.

## Usage

### Run Host

```bash
# Default name ("Sonic Host") and default game port (5029)
cargo run -p sidecar -- host

# Custom server name and custom local game port
cargo run -p sidecar -- host --game-port 5029 "Sonic Speedway"
```

### Query Server List

```bash
cargo run -p sidecar -- list
```

### Join a Server

```bash
# Automatically discovers and joins the first active server (using custom game port)
cargo run -p sidecar -- join --game-port 5030

# Or join a specific server ID
cargo run -p sidecar -- join --game-port 5030 <SERVER_ID>
```

### CLI Arguments & Options

- `--hub-ws-url <URL>`: WebSocket signaling endpoint URL (default: `ws://127.0.0.1:8080/ws`).
- `--hub-udp-addr <IP:PORT>`: Mini-STUN UDP endpoint address (default: `127.0.0.1:9000`).
- `--game-port <PORT>`: High-level game port setting.
  - In **host** mode, sets `--target-game-port` (the port where the game server listens, default: `5029`).
  - In **join** mode, sets `--proxy-port` (the local port the sidecar proxy binds to, default: `5030` or `5029`).
- `--target-game-port <PORT>`: (Host mode) The local port where the game server listens for incoming connections (default: `5029`).
- `--proxy-port <PORT>`: The local loopback port on `127.0.0.1` that the sidecar binds to.
  - In **host** mode, defaults to `0` (ephemeral port) so the game server can bind `5029` exclusively without port collision.
  - In **join** mode, defaults to the game port (default: `5030` or `5029`).

### Environment Variables

- `GAME_PORT`: Default local game port if `--game-port` is not specified on CLI.
- `TARGET_GAME_PORT`: Default target game port for host mode (default: `5029`).
- `PROXY_PORT`: Default bind port for local sidecar proxy (`0` for host, `5030` for join).
- `HUB_WS_URL`: WebSocket signaling endpoint (default: `ws://127.0.0.1:8080/ws`).
- `HUB_UDP_ADDR`: Mini-STUN UDP endpoint (default: `127.0.0.1:9000`).
- `SIDECAR_FORCE_RELAY`: Set to `1` or `true` to immediately bypass direct P2P hole punching and invoke the UDP relay fallback path (useful for testing).

## Wire Format

All tunnel datagrams between sidecars (whether direct P2P or relayed through the hub) use the following format:

```text
+------------------------+-------------------+----------------------------+
| 16-byte PunchToken     | 1-byte MsgType    | Payload                    |
| (UUID bytes)           | 0x00 = Keepalive  | (Optional PING/PONG or     |
|                        | 0x01 = Game Data  |  raw game datagram bytes)  |
+------------------------+-------------------+----------------------------+
```

- `MsgType = 0x00 (Keepalive)`: Used for hole punching and periodic keepalive pings (every 5s). Consumed internally by `sidecar` and never forwarded to the local game process.
- `MsgType = 0x01 (Game Data)`: Carries raw datagrams sent by the local game process. Transparently forwarded byte-for-byte to the peer's game socket.

## Local UDP Loopback Proxy

The `sidecar` exposes a transparent UDP proxy on `127.0.0.1`:

1. **Host Mode (Passive Server)**:
   - The game host (`sonicr.exe --port 5029`) starts as a passive UDP listener on `127.0.0.1:5029` and does not transmit first.
   - The host sidecar binds to an ephemeral port (`127.0.0.1:0` by default), preventing port collisions with the game process on `5029`.
   - The host sidecar presets its destination address to `127.0.0.1:5029` (`--target-game-port`).
   - When the client's join datagram arrives across the tunnel, the host sidecar immediately delivers it to `127.0.0.1:5029` without waiting to learn the host's address.
   - When `sonicr.exe` responds to the datagram's origin (the sidecar's ephemeral port), sidecar encapsulates it and forwards it back through the tunnel.

2. **Join Mode (Active Client)**:
   - The client sidecar binds `127.0.0.1:5030` (`--game-port 5030`).
   - The game client (`sonicr.exe --host 127.0.0.1 --port 5030`) initiates communication by sending its join request to the client sidecar proxy.
   - The proxy dynamically learns the client game's ephemeral socket address, forwards the packet through the tunnel, and routes replies back to the client game.

3. **Seamless Relay Switching**:
   - If direct P2P fails or the peer triggers relay fallback, the proxy switches forwarding target to the hub's relay endpoint on-the-fly without interrupting the local game session.

## Manual Testing Recipe

To verify end-to-end game traffic forwarding over loopback:

1. **Start the Hub**:
   ```bash
   cargo run -p hub
   ```

2. **Start Host Sidecar** (game port 5029):
   ```bash
   cargo run -p sidecar -- host --game-port 5029 "Test Host"
   ```

3. **Start Join Sidecar** (game port 5030):
   ```bash
   cargo run -p sidecar -- join --game-port 5030
   ```

4. **Verify Direct UDP Forwarding via PowerShell / Python / netcat**:
   - In terminal 1 (Host game listener on port 6001 sending to 5029):
     ```powershell
     $u1 = New-Object System.Net.Sockets.UdpClient(6001)
     # Send packet to Host sidecar to register local address
     $bytes = [System.Text.Encoding]::UTF8.GetBytes("HELLO_FROM_HOST_GAME")
     $u1.Send($bytes, $bytes.Length, "127.0.0.1", 5029)
     ```
   - In terminal 2 (Join game listener on port 6002 sending to 5030):
     ```powershell
     $u2 = New-Object System.Net.Sockets.UdpClient(6002)
     # Send packet to Join sidecar
     $bytes = [System.Text.Encoding]::UTF8.GetBytes("HELLO_FROM_CLIENT_GAME")
     $u2.Send($bytes, $bytes.Length, "127.0.0.1", 5030)
     ```
   - Both sides receive the packet forwarded from the other peer!



