# Sonic R Netplay

A modern networked multiplayer port of the Sonic R PC decompilation, featuring peer-to-peer gameplay, automatic NAT traversal, and a standalone graphical launcher.

## Overview

Sonic R Netplay retrofits the open-source Sonic R decompilation with a modern network transport stack. It eliminates legacy DirectPlay requirements and provides reliable internet and LAN play through an integrated signaling hub, automated lobby synchronization, and a desktop companion launcher.

## Features

- Peer-to-Peer Transport: Low-latency UDP communication directly between players.
- NAT Traversal: Automated bidirectional UDP hole punching.
- Relay Fallback: Transparent routing through the signaling hub when symmetric NAT or firewall configurations prevent direct connection.
- Graphical Launcher: Room browser, one-click hosting, automated process lifecycle, and configuration management built in Rust and egui.
- Secure Signaling: WebSocket signaling protocol supporting both unencrypted (`ws://`) and TLS-encrypted (`wss://`) endpoints, compatible with reverse proxies and Cloudflare Tunnels.
- Statically Linked: Self-contained binaries with no external runtime dependencies.

## Requirements

To play, you must own a retail copy of Sonic R for PC to supply the original game assets. This distribution does not provide copyrighted asset files.

Required asset directories:
- `GENERAL/` (menus, character models, core sprites)
- `ISLAND/` (track geometry and textures)
- `MUSIC/` (audio tracks, optional for music playback)

## How to Play

1. Download and extract the latest release archive (`Sonic-R-Netplay-v1.0.0-win64.zip`).
2. Copy the original `DATA` directory (or the individual asset folders: `GENERAL`, `ISLAND`, etc.) from your retail installation or disc into the extracted folder.
3. Run `launcher.exe`.
4. Verify your player name and set the Hub Address:
   - For local LAN play: leave the default (`127.0.0.1:8080`) or specify your host machine IP.
   - For public internet play: enter the server URL provided by your host (e.g. `wss://hub.example.com/ws`).
5. Host or Join:
   - Hosting: Click "Create Host Session", specify room options, and wait for players to enter.
   - Joining: Click "Refresh", select an available room from the list, and click "Join".
6. In the lobby, once all players are connected, the host presses F1 to start the race.

## Building from Source

### Prerequisites

- Git
- Rust (stable toolchain, 1.75 or newer recommended)
- MSYS2 with UCRT64 toolchain:
  ```bash
  pacman -S mingw-w64-ucrt-x86_64-gcc mingw-w64-ucrt-x86_64-make \
            mingw-w64-ucrt-x86_64-SDL2 mingw-w64-ucrt-x86_64-SDL2_mixer \
            mingw-w64-ucrt-x86_64-opusfile mingw-w64-ucrt-x86_64-libvorbis \
            mingw-w64-ucrt-x86_64-flac mingw-w64-ucrt-x86_64-mpg123 \
            mingw-w64-ucrt-x86_64-libxmp mingw-w64-ucrt-x86_64-wavpack
  ```

### 1. Build C Game Engine (`sonicr.exe`)

From the repository root, run the release build target via MSYS2 UCRT64:

```powershell
C:\msys64\msys2_shell.cmd -defterm -here -no-start -ucrt64 -c "cd sonic-r-main/source/sdl && make clean && make release-clean && make release -j$(nproc)"
```

The resulting executable will be located at:
`sonic-r-main/source/sdl/build-release/sonicr.exe`

### 2. Build Rust Components (Launcher, Sidecar, Hub)

Build all Rust binaries with static C runtime linkage:

```powershell
$env:RUSTFLAGS="-C target-feature=+crt-static"; cargo build --release
```

Output binaries:
- `target/release/launcher.exe` - Graphical client and lobby runner.
- `target/release/sidecar.exe` - Standalone CLI networking companion.
- `target/release/hub.exe` - Standalone signaling and relay server.

### 3. Packaging

Copy `sonicr.exe` into the same directory as `launcher.exe` along with your `DATA/` asset directory. The launcher automatically prioritizes bundled game binaries located in its own directory.

## Architecture

```
[Launcher GUI] <---> [Sidecar Network Runner] <--- WebSocket ---> [Signaling Hub]
                            |                                            |
                       UDP (Loopback)                              UDP (Relay Fallback)
                            |                                            |
                     [Sonic R Engine] <--------- UDP (P2P) ---------> [Peer]
```

- Launcher: UI interface, process supervision, configuration persistence.
- Sidecar: Signaling protocol client, STUN-like UDP hole punching, loopback tunnel adapter for game network packets.
- Hub: Lightweight Axum/WebSocket registry tracking active game servers, heartbeat liveness, and fallback packet relaying.
- Sonic R Engine: Reverse-engineered game core patched to interface with modern SDL2, hardware rendering, and loopback UDP network synchronization.

## Credits

- Sonic R decompilation and modernization by jnmartin84 (https://github.com/jnmartin84/sonic-r).
- Original game developed by Traveller's Tales and Sonic Team.

## Disclaimer

Sonic R is a registered trademark of SEGA Games Co., Ltd. and SEGA of America, Inc.

This project is an independent open-source reverse-engineering endeavor created strictly for educational and preservation purposes. It is not affiliated with, endorsed by, or sponsored by SEGA. No copyrighted game assets, audio files, textures, or proprietary game media are distributed with this software. A legally purchased retail copy of Sonic R is required to supply asset files for gameplay.
