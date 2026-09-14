#![cfg_attr(not(debug_assertions), windows_subsystem = "windows")]

mod app;
mod config;
mod process;
mod theme;

use app::LauncherApp;
use eframe::egui;

fn main() -> eframe::Result<()> {
    let _ = rustls::crypto::ring::default_provider().install_default();
    tracing_subscriber::fmt::init();

    let rt = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()
        .expect("Failed to initialize Tokio runtime");

    let tokio_handle = rt.handle().clone();

    let native_options = eframe::NativeOptions {
        viewport: egui::ViewportBuilder::default()
            .with_inner_size([580.0, 420.0])
            .with_resizable(false)
            .with_maximize_button(false)
            .with_title("Sonic R Netplay Launcher"),
        ..Default::default()
    };

    eframe::run_native(
        "Sonic R Netplay Launcher",
        native_options,
        Box::new(move |cc| {
            theme::apply_theme(&cc.egui_ctx);
            Box::new(LauncherApp::new(tokio_handle))
        }),
    )
}
