use crate::{
    config::LauncherConfig,
    process::{self, ExecutableSource, ResolvedExecutable},
    theme::{
        COLOR_BG, COLOR_FRAME, COLOR_GOLD, COLOR_LOBBY_GREEN, COLOR_RACE_GRAY, COLOR_SONIC_BLUE,
        COLOR_STOP_RED, COLOR_TEXT_MUTED,
    },
};
use eframe::egui::{self, Color32, RichText};
use proto::{ServerId, ServerInfo, ServerStatus};
use sidecar::runner::{HostConfig, JoinConfig, RunnerEvent};
use std::{net::SocketAddr, path::PathBuf, time::Duration};
use tokio::runtime::Handle;

const DEFAULT_HUB_ADDR: &str = "127.0.0.1:8080";
pub const DEFAULT_NETPLAY_PORT: u16 = 5029;

pub struct LauncherApp {
    tokio_handle: Handle,
    pub config: LauncherConfig,
    hub_addr: String,
    host_room_name: String,
    server_list: Vec<ServerInfo>,
    is_refreshing: bool,
    refresh_rx: Option<tokio::sync::oneshot::Receiver<Result<Vec<ServerInfo>, String>>>,
    runner_event_rx: Option<tokio::sync::mpsc::Receiver<RunnerEvent>>,
    sidecar_task: Option<tokio::task::JoinHandle<()>>,
    game_child: Option<tokio::process::Child>,
    is_session_active: bool,
    is_hosting: bool,
    status_message: String,
    detected_exe: Option<ResolvedExecutable>,
}

impl LauncherApp {
    pub fn new(tokio_handle: Handle) -> Self {
        let config = LauncherConfig::load();

        let hub_addr = std::env::var("HUB_ADDR")
            .or_else(|_| std::env::var("HUB_WS_URL"))
            .ok()
            .or_else(|| config.last_hub_addr.clone())
            .unwrap_or_else(|| DEFAULT_HUB_ADDR.to_string());

        let host_room_name = config
            .last_player_name
            .clone()
            .unwrap_or_else(|| "Sonic Room".to_string());

        let detected_exe = process::resolve_sonicr_executable(config.game_dir.as_deref());

        let mut app = Self {
            tokio_handle,
            config,
            hub_addr,
            host_room_name,
            server_list: Vec::new(),
            is_refreshing: false,
            refresh_rx: None,
            runner_event_rx: None,
            sidecar_task: None,
            game_child: None,
            is_session_active: false,
            is_hosting: false,
            status_message: "Ready. Select a server or create a host session.".to_string(),
            detected_exe,
        };

        // Trigger initial server list refresh
        app.refresh_servers();
        app
    }

    pub fn is_game_dir_valid(&self) -> bool {
        if let Some(ref dir) = self.config.game_dir {
            process::validate_game_dir(dir) && self.detected_exe.is_some()
        } else {
            false
        }
    }

    pub fn browse_for_game_dir(&mut self) {
        let mut dialog = rfd::FileDialog::new().set_title("Select Sonic R Game Directory");
        if let Some(ref existing) = self.config.game_dir {
            if existing.exists() {
                dialog = dialog.set_directory(existing);
            }
        }
        if let Some(folder) = dialog.pick_folder() {
            self.set_game_dir(folder);
        }
    }

    pub fn set_game_dir(&mut self, dir: PathBuf) {
        self.config.game_dir = Some(dir.clone());
        self.detected_exe = process::resolve_sonicr_executable(Some(&dir));
        let _ = self.config.save();
        if self.is_game_dir_valid() {
            self.status_message = format!("Game directory set: {}", dir.display());
        } else {
            self.status_message =
                "Selected directory does not appear to contain Sonic R data (GENERAL, etc.)!"
                    .to_string();
        }
    }

    fn parse_hub_info(&self) -> (String, SocketAddr) {
        parse_hub_addr_str(&self.hub_addr)
    }

    pub fn refresh_servers(&mut self) {
        if self.is_refreshing {
            return;
        }

        self.config.last_hub_addr = Some(self.hub_addr.clone());
        let _ = self.config.save();

        let (ws_url, _) = self.parse_hub_info();
        let (tx, rx) = tokio::sync::oneshot::channel();
        self.is_refreshing = true;
        self.refresh_rx = Some(rx);

        self.tokio_handle.spawn(async move {
            let res = sidecar::runner::fetch_server_list(&ws_url).await;
            let _ = tx.send(res);
        });
    }

    pub fn start_host_session(&mut self) {
        if self.is_session_active {
            return;
        }

        if !self.is_game_dir_valid() {
            self.status_message = "Please select a valid Sonic R game directory first!".to_string();
            return;
        }

        let hub_input = self.hub_addr.clone();
        let name = if self.host_room_name.trim().is_empty() {
            "Sonic Host".to_string()
        } else {
            self.host_room_name.trim().to_string()
        };

        self.config.last_player_name = Some(name.clone());
        let _ = self.config.save();

        let (event_tx, event_rx) = tokio::sync::mpsc::channel(64);
        self.runner_event_rx = Some(event_rx);
        self.is_session_active = true;
        self.is_hosting = true;
        self.status_message = "Starting host session...".to_string();
        self.launch_game_process(true, DEFAULT_NETPLAY_PORT, None);

        let err_tx = event_tx.clone();
        let task = self.tokio_handle.spawn(async move {
            let (hub_ws_url, hub_udp_addr) = resolve_hub_addr_async(&hub_input).await;
            let config = HostConfig {
                hub_ws_url,
                hub_udp_addr,
                name,
                bind_port: 0,
                target_game_addr: Some(SocketAddr::from(([127, 0, 0, 1], DEFAULT_NETPLAY_PORT))),
            };
            if let Err(err) = sidecar::runner::run_host_session(config, Some(event_tx)).await {
                tracing::error!(%err, "Host session runner terminated with error");
                let _ = err_tx.send(RunnerEvent::Error(err)).await;
            }
        });
        self.sidecar_task = Some(task);
    }

    pub fn start_join_session(&mut self, server_id: ServerId) {
        if self.is_session_active {
            return;
        }

        if !self.is_game_dir_valid() {
            self.status_message = "Please select a valid Sonic R game directory first!".to_string();
            return;
        }

        let hub_input = self.hub_addr.clone();
        let (event_tx, event_rx) = tokio::sync::mpsc::channel(64);
        self.runner_event_rx = Some(event_rx);
        self.is_session_active = true;
        self.is_hosting = false;
        self.status_message = format!("Joining server {server_id}...");

        let err_tx = event_tx.clone();
        let task = self.tokio_handle.spawn(async move {
            let (hub_ws_url, hub_udp_addr) = resolve_hub_addr_async(&hub_input).await;
            let config = JoinConfig {
                hub_ws_url,
                hub_udp_addr,
                server_id: Some(server_id),
                bind_port: DEFAULT_NETPLAY_PORT,
                target_game_addr: None,
            };
            if let Err(err) = sidecar::runner::run_join_session(config, Some(event_tx)).await {
                tracing::error!(%err, "Join session runner terminated with error");
                let _ = err_tx.send(RunnerEvent::Error(err)).await;
            }
        });
        self.sidecar_task = Some(task);
    }

    pub fn stop_session(&mut self) {
        if let Some(task) = self.sidecar_task.take() {
            task.abort();
        }
        if let Some(mut child) = self.game_child.take() {
            let _ = child.start_kill();
        }
        self.runner_event_rx = None;
        self.is_session_active = false;
        self.is_hosting = false;
        self.status_message = "Session stopped. Ready.".to_string();
    }

    fn launch_game_process(&mut self, is_host: bool, port: u16, host_ip: Option<&str>) {
        let game_dir = match &self.config.game_dir {
            Some(dir) if process::validate_game_dir(dir) => dir.clone(),
            _ => {
                self.status_message =
                    "Cannot launch: valid game data folder not selected!".to_string();
                return;
            }
        };

        if let Some(ref exe) = self.detected_exe {
            tracing::info!(
                exe = %exe.path.display(),
                working_dir = %game_dir.display(),
                source = ?exe.source,
                "Launching Sonic R executable"
            );
            let player_name = if is_host {
                let n = self.host_room_name.trim();
                if !n.is_empty() {
                    Some(n)
                } else {
                    self.config.last_player_name.as_deref()
                }
            } else {
                self.config.last_player_name.as_deref()
            };
            match process::launch_game(&exe.path, &game_dir, is_host, port, host_ip, player_name) {
                Ok(child) => {
                    let pid = child.id().unwrap_or(0);
                    self.game_child = Some(child);
                    self.status_message = format!("Game active (PID: {pid}, port: {port}).");
                }
                Err(e) => {
                    self.status_message = format!("Failed to launch sonicr.exe: {e}");
                }
            }
        } else {
            self.status_message =
                "sonicr.exe not found! Place next to launcher, in game folder, or set SONICR_PATH."
                    .to_string();
        }
    }

    fn handle_runner_event(&mut self, event: RunnerEvent) {
        match event {
            RunnerEvent::Status(msg) => {
                self.status_message = msg;
            }
            RunnerEvent::Registered { server_id } => {
                self.status_message =
                    format!("Host session registered (ID: {server_id}). Waiting for players...");
                if self.is_hosting && self.game_child.is_none() {
                    self.launch_game_process(true, DEFAULT_NETPLAY_PORT, None);
                }
            }
            RunnerEvent::PeerCandidateReceived { peer_addr, .. } => {
                self.status_message =
                    format!("Peer discovered ({peer_addr}), connecting tunnel...");
            }
            RunnerEvent::TunnelEstablished {
                peer_addr,
                is_relay,
            } => {
                let mode_str = if is_relay {
                    "Relay (Hub)"
                } else {
                    "Direct P2P"
                };
                self.status_message =
                    format!("Tunnel active! Mode: {mode_str} (Peer: {peer_addr})");
            }
            RunnerEvent::ProxyBound { port } => {
                self.status_message = format!("Proxy bound on 127.0.0.1:{port}. Launching game...");
                if !self.is_hosting && self.game_child.is_none() {
                    self.launch_game_process(false, port, Some("127.0.0.1"));
                }
            }
            RunnerEvent::Error(err) => {
                let err_msg = format!("Error: {err}");
                self.stop_session();
                self.status_message = err_msg;
            }
            RunnerEvent::Stopped => {
                if self.is_session_active {
                    self.stop_session();
                    self.status_message = "Session ended (game left lobby). Ready.".to_string();
                } else {
                    self.status_message = "Session ended. Ready.".to_string();
                }
            }
        }
    }
}

impl eframe::App for LauncherApp {
    fn update(&mut self, ctx: &egui::Context, _frame: &mut eframe::Frame) {
        // 1. Poll refresh results
        if let Some(rx) = &mut self.refresh_rx {
            match rx.try_recv() {
                Ok(res) => {
                    self.is_refreshing = false;
                    self.refresh_rx = None;
                    match res {
                        Ok(servers) => {
                            self.server_list = servers;
                        }
                        Err(err) => {
                            self.status_message = format!("Refresh failed: {err}");
                        }
                    }
                    ctx.request_repaint();
                }
                Err(tokio::sync::oneshot::error::TryRecvError::Closed) => {
                    self.is_refreshing = false;
                    self.refresh_rx = None;
                    self.status_message =
                        "Refresh failed: background task terminated unexpectedly".to_string();
                    ctx.request_repaint();
                }
                Err(tokio::sync::oneshot::error::TryRecvError::Empty) => {}
            }
        }

        // 2. Poll runner events
        let mut events = Vec::new();
        let mut channel_closed = false;
        if let Some(rx) = &mut self.runner_event_rx {
            loop {
                match rx.try_recv() {
                    Ok(event) => events.push(event),
                    Err(tokio::sync::mpsc::error::TryRecvError::Empty) => break,
                    Err(tokio::sync::mpsc::error::TryRecvError::Disconnected) => {
                        channel_closed = true;
                        break;
                    }
                }
            }
        }
        for event in events {
            self.handle_runner_event(event);
            ctx.request_repaint();
        }
        if channel_closed && self.is_session_active {
            tracing::warn!("Runner event channel closed while session active; stopping session");
            self.stop_session();
            if !self.status_message.starts_with("Error:") {
                self.status_message = "Session terminated unexpectedly.".to_string();
            }
            ctx.request_repaint();
        }

        // 3. Poll game process status
        if let Some(child) = &mut self.game_child {
            match child.try_wait() {
                Ok(Some(status)) => {
                    tracing::info!(?status, "Sonic R process exited");
                    self.status_message = format!("Game exited ({status}). Session closed.");
                    self.stop_session();
                    ctx.request_repaint();
                }
                Ok(None) => {}
                Err(e) => {
                    tracing::error!(%e, "Error checking game process status");
                }
            }
        }

        if self.is_session_active || self.is_refreshing {
            ctx.request_repaint_after(Duration::from_millis(100));
        }

        let is_ready = self.is_game_dir_valid();

        // 4. Render UI
        egui::CentralPanel::default()
            .frame(egui::Frame::none().fill(COLOR_BG).inner_margin(12.0))
            .show(ctx, |ui| {
                // Header: Title
                ui.horizontal(|ui| {
                    ui.heading(
                        RichText::new("Sonic R Netplay Launcher")
                            .color(COLOR_GOLD)
                            .strong(),
                    );
                });

                ui.add_space(8.0);

                // Prominent banner if game directory is missing or invalid
                if !is_ready {
                    ui.group(|ui| {
                        ui.set_width(ui.available_width());
                        ui.horizontal(|ui| {
                            ui.label(
                                RichText::new("⚠ Select Sonic R Game Folder:")
                                    .color(COLOR_STOP_RED)
                                    .strong(),
                            );
                            ui.label(
                                RichText::new("Folder must contain game data (GENERAL, etc.)")
                                    .color(COLOR_TEXT_MUTED),
                            );
                            ui.with_layout(
                                egui::Layout::right_to_left(egui::Align::Center),
                                |ui| {
                                    let browse_btn = egui::Button::new(
                                        RichText::new("Browse...").color(Color32::WHITE).strong(),
                                    )
                                    .fill(COLOR_SONIC_BLUE);

                                    if ui.add(browse_btn).clicked() {
                                        self.browse_for_game_dir();
                                    }
                                },
                            );
                        });
                    });
                    ui.add_space(8.0);
                }

                // Top bar: Hub address & Refresh
                ui.horizontal(|ui| {
                    ui.label(RichText::new("Hub:").color(COLOR_GOLD).strong());
                    ui.add_sized(
                        [260.0, 22.0],
                        egui::TextEdit::singleline(&mut self.hub_addr),
                    );

                    let refresh_btn = egui::Button::new(
                        RichText::new(if self.is_refreshing {
                            "Scanning..."
                        } else {
                            "⟳ Refresh"
                        })
                        .color(Color32::WHITE)
                        .strong(),
                    )
                    .fill(COLOR_SONIC_BLUE);

                    if ui.add_enabled(!self.is_refreshing, refresh_btn).clicked() {
                        self.refresh_servers();
                    }
                });

                ui.add_space(8.0);

                // Server Browser Table Header
                let mut to_join = None;
                let session_active = self.is_session_active;

                ui.group(|ui| {
                    ui.set_width(ui.available_width());
                    ui.horizontal(|ui| {
                        ui.label(RichText::new("ONLINE SERVERS").color(COLOR_GOLD).strong());
                        ui.with_layout(egui::Layout::right_to_left(egui::Align::Center), |ui| {
                            ui.label(
                                RichText::new(format!("Total: {}", self.server_list.len()))
                                    .color(COLOR_TEXT_MUTED),
                            );
                        });
                    });

                    ui.separator();

                    // Server Browser List
                    let scroll_max_height = if is_ready { 135.0 } else { 100.0 };
                    egui::ScrollArea::vertical()
                        .max_height(scroll_max_height)
                        .show(ui, |ui| {
                            if self.server_list.is_empty() {
                                ui.vertical_centered(|ui| {
                                    ui.add_space(15.0);
                                    ui.label(
                                        RichText::new("No active servers found on hub.")
                                            .color(COLOR_TEXT_MUTED),
                                    );
                                    ui.label(
                                        RichText::new(
                                            "Click '⟳ Refresh' to scan or host your own room below.",
                                        )
                                        .color(COLOR_TEXT_MUTED),
                                    );
                                    ui.add_space(15.0);
                                });
                            } else {
                                for server in &self.server_list {
                                    ui.horizontal(|ui| {
                                        // Server Name
                                        ui.add_sized(
                                            [200.0, 22.0],
                                            egui::Label::new(
                                                RichText::new(&server.name)
                                                    .color(Color32::WHITE)
                                                    .strong(),
                                            ),
                                        );

                                        // Players
                                        ui.add_sized(
                                            [65.0, 22.0],
                                            egui::Label::new(
                                                RichText::new(format!(
                                                    "{}/{}",
                                                    server.players, server.max_players
                                                ))
                                                .color(COLOR_TEXT_MUTED),
                                            ),
                                        );

                                        // Status Badge
                                        let (status_text, status_color) = match server.status {
                                            ServerStatus::InLobby => {
                                                ("● In Lobby", COLOR_LOBBY_GREEN)
                                            }
                                            ServerStatus::InRace => {
                                                ("■ In Race", COLOR_RACE_GRAY)
                                            }
                                        };

                                        ui.add_sized(
                                            [85.0, 22.0],
                                            egui::Label::new(
                                                RichText::new(status_text).color(status_color),
                                            ),
                                        );

                                        // Join Button
                                        let can_join = !session_active && is_ready && server.is_joinable();
                                        let join_btn = egui::Button::new(
                                            RichText::new("Join").color(Color32::WHITE).strong(),
                                        )
                                        .fill(if can_join {
                                            COLOR_SONIC_BLUE
                                        } else {
                                            COLOR_FRAME
                                        });

                                        if ui.add_enabled(can_join, join_btn).clicked() {
                                            to_join = Some(server.id);
                                        }
                                    });
                                    ui.separator();
                                }
                            }
                        });
                });

                if let Some(server_id) = to_join {
                    self.start_join_session(server_id);
                }

                ui.add_space(8.0);

                // Host Section
                ui.group(|ui| {
                    ui.set_width(ui.available_width());
                    ui.horizontal(|ui| {
                        ui.label(RichText::new("Room Name:").color(COLOR_GOLD).strong());
                        ui.add_sized(
                            [210.0, 22.0],
                            egui::TextEdit::singleline(&mut self.host_room_name),
                        );

                        let can_host = !self.is_session_active && is_ready;
                        let host_btn = egui::Button::new(
                            RichText::new("Create Host Session")
                                .color(Color32::WHITE)
                                .strong(),
                        )
                        .fill(if can_host {
                            COLOR_SONIC_BLUE
                        } else {
                            COLOR_FRAME
                        });

                        if ui.add_enabled(can_host, host_btn).clicked() {
                            self.start_host_session();
                        }
                    });
                });

                ui.add_space(8.0);

                // Footer / Status Bar
                ui.group(|ui| {
                    ui.set_width(ui.available_width());
                    ui.horizontal(|ui| {
                        ui.add_sized(
                            [400.0, 24.0],
                            egui::Label::new(
                                RichText::new(&self.status_message)
                                    .color(COLOR_GOLD)
                                    .strong(),
                            ),
                        );

                        ui.with_layout(egui::Layout::right_to_left(egui::Align::Center), |ui| {
                            let stop_btn = egui::Button::new(
                                RichText::new("⏹ Stop").color(Color32::WHITE).strong(),
                            )
                            .fill(if self.is_session_active {
                                COLOR_STOP_RED
                            } else {
                                COLOR_FRAME
                            });

                            if ui.add_enabled(self.is_session_active, stop_btn).clicked() {
                                self.stop_session();
                            }
                        });
                    });
                });

                // Game folder and executable status line with Change button
                ui.horizontal(|ui| {
                    // Left side: Assets directory
                    match &self.config.game_dir {
                        Some(dir) => {
                            let valid = is_ready;
                            let (icon, color) = if valid {
                                ("✔ Assets:", COLOR_LOBBY_GREEN)
                            } else {
                                ("⚠ Assets (Invalid):", COLOR_STOP_RED)
                            };
                            ui.label(RichText::new(icon).color(color).size(11.0).strong());
                            let path_str = dir.to_string_lossy();
                            let display_str = if path_str.len() > 30 {
                                format!("...{}", &path_str[path_str.len() - 27..])
                            } else {
                                path_str.to_string()
                            };
                            ui.label(RichText::new(display_str).color(COLOR_TEXT_MUTED).size(11.0))
                                .on_hover_text(dir.display().to_string());

                            if ui.small_button("Change").clicked() {
                                self.browse_for_game_dir();
                            }
                        }
                        None => {
                            ui.label(
                                RichText::new("⚠ Assets: Not Set")
                                    .color(COLOR_STOP_RED)
                                    .size(11.0)
                                    .strong(),
                            );
                            if ui.small_button("Browse...").clicked() {
                                self.browse_for_game_dir();
                            }
                        }
                    }

                    // Right side: Executable
                    ui.with_layout(egui::Layout::right_to_left(egui::Align::Center), |ui| {
                        match &self.detected_exe {
                            Some(res) => {
                                let (label_text, label_color) = match res.source {
                                    ExecutableSource::Bundled => {
                                        ("✔ Using bundled sonicr.exe".to_string(), COLOR_LOBBY_GREEN)
                                    }
                                    _ => (format!("Exe: {}", res.display_label()), COLOR_TEXT_MUTED),
                                };
                                ui.label(RichText::new(label_text).color(label_color).size(11.0))
                                    .on_hover_text(res.path.display().to_string());
                            }
                            None => {
                                ui.label(
                                    RichText::new("⚠ Binary: Missing")
                                        .color(COLOR_STOP_RED)
                                        .size(11.0),
                                );
                            }
                        }
                    });
                });
            });
    }
}

pub fn extract_hub_parts(raw: &str) -> (String, String) {
    let trimmed = raw.trim();
    let (scheme, rest) = if let Some(stripped) = trimmed.strip_prefix("wss://") {
        ("wss://", stripped)
    } else if let Some(stripped) = trimmed.strip_prefix("ws://") {
        ("ws://", stripped)
    } else {
        ("ws://", trimmed)
    };

    let (authority, path) = match rest.find('/') {
        Some(idx) => {
            let (auth, p) = rest.split_at(idx);
            let normalized_path = if p == "/" || p.is_empty() {
                "/ws".to_string()
            } else {
                p.to_string()
            };
            (auth, normalized_path)
        }
        None => (rest, "/ws".to_string()),
    };

    let ws_url = format!("{scheme}{authority}{path}");
    let host = authority.split(':').next().unwrap_or("127.0.0.1");
    let clean_host = if host.is_empty() { "127.0.0.1" } else { host };
    (ws_url, clean_host.to_string())
}

pub async fn resolve_hub_addr_async(raw: &str) -> (String, SocketAddr) {
    let (ws_url, host) = extract_hub_parts(raw);
    let udp_target = format!("{host}:9000");

    let udp_addr = match tokio::net::lookup_host(&udp_target).await {
        Ok(mut addrs) => addrs
            .find(|a| a.is_ipv4())
            .or_else(|| addrs.next())
            .unwrap_or_else(|| SocketAddr::from(([127, 0, 0, 1], 9000))),
        Err(e) => {
            tracing::warn!(%udp_target, error = %e, "Failed to resolve hub UDP host; falling back to 127.0.0.1:9000");
            SocketAddr::from(([127, 0, 0, 1], 9000))
        }
    };

    (ws_url, udp_addr)
}

pub fn parse_hub_addr_str(raw: &str) -> (String, SocketAddr) {
    let (ws_url, host) = extract_hub_parts(raw);
    let udp_target = format!("{host}:9000");

    let udp_addr = udp_target
        .parse()
        .or_else(|_| {
            use std::net::ToSocketAddrs;
            udp_target
                .to_socket_addrs()
                .map(|mut iter| iter.find(|a| a.is_ipv4()).or_else(|| iter.next()))
                .ok()
                .flatten()
                .ok_or(())
        })
        .unwrap_or_else(|_| SocketAddr::from(([127, 0, 0, 1], 9000)));

    (ws_url, udp_addr)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_hub_addr_str_with_scheme() {
        let (ws, udp) = parse_hub_addr_str("ws://127.0.0.1:8080/ws");
        assert_eq!(ws, "ws://127.0.0.1:8080/ws");
        assert_eq!(udp, "127.0.0.1:9000".parse().unwrap());
    }

    #[test]
    fn test_parse_hub_addr_str_without_path() {
        let (ws, udp) = parse_hub_addr_str("ws://127.0.0.1:8080");
        assert_eq!(ws, "ws://127.0.0.1:8080/ws");
        assert_eq!(udp, "127.0.0.1:9000".parse().unwrap());
    }

    #[test]
    fn test_parse_hub_addr_str_with_trailing_slash() {
        let (ws, udp) = parse_hub_addr_str("ws://127.0.0.1:8080/");
        assert_eq!(ws, "ws://127.0.0.1:8080/ws");
        assert_eq!(udp, "127.0.0.1:9000".parse().unwrap());
    }

    #[test]
    fn test_parse_hub_addr_str_wss_cloudflare() {
        let (ws, _) = parse_hub_addr_str("wss://my-hub.trycloudflare.com");
        assert_eq!(ws, "wss://my-hub.trycloudflare.com/ws");
    }

    #[test]
    fn test_parse_hub_addr_str_plain_ip() {
        let (ws, udp) = parse_hub_addr_str("192.168.1.50");
        assert_eq!(ws, "ws://192.168.1.50/ws");
        assert_eq!(udp, "192.168.1.50:9000".parse().unwrap());
    }

    #[tokio::test]
    async fn test_resolve_hub_addr_async_localhost() {
        let (ws, udp) = resolve_hub_addr_async("localhost:8080").await;
        assert_eq!(ws, "ws://localhost:8080/ws");
        assert_eq!(udp.port(), 9000);
        assert!(udp.ip().is_loopback());
    }
}
