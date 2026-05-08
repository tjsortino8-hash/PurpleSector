//! PurpleSector System Tray App
//!
//! Captures sim telemetry data via UDP/SharedMemory, batches it, and streams
//! to the cloud pipeline via gRPC. Uses a local WAL for resilience during
//! network disconnects.
//!
//! Features:
//! - System tray icon with right-click menu (Start/Stop, Settings, Stats, Quit)
//! - egui settings window (sim type, gRPC endpoint, credentials)
//! - Live stats display (samples/sec, batches sent, WAL depth)
//! - Persistent config file (TOML)
//!
//! This is the cloud-connected companion to the self-contained Tauri desktop app.

// Hide console window on Windows release builds
#![cfg_attr(
    all(target_os = "windows", not(debug_assertions)),
    windows_subsystem = "windows"
)]

mod config;
mod gui;
mod pipeline;
mod platform;
mod stats;

use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use anyhow::Result;
use eframe::egui;
use tokio::sync::mpsc;
use tracing::info;
use tray_icon::menu::{Menu, MenuEvent, MenuItem, PredefinedMenuItem};
use tray_icon::TrayIconBuilder;

use config::AppConfig;
use gui::GuiState;
use pipeline::PipelineCommand;
use stats::PipelineStats;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum UiCommand {
    OpenWindow,
}

fn main() -> Result<()> {
    // Initialize logging
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| "ps_tray_app=info,ps_telemetry_core=info".into()),
        )
        .init();

    info!("PurpleSector Tray App starting...");

    // Load config
    let config = Arc::new(Mutex::new(AppConfig::load()));
    let stats = PipelineStats::new();
    let running = Arc::new(AtomicBool::new(false));
    let (cmd_tx, cmd_rx) = mpsc::channel::<PipelineCommand>(32);
    let (ui_cmd_tx, ui_cmd_rx) = std::sync::mpsc::channel::<UiCommand>();
    let app_quit_flag = Arc::new(AtomicBool::new(false));
    let egui_ctx: Arc<std::sync::OnceLock<egui::Context>> = Arc::new(std::sync::OnceLock::new());

    // Build the tray menu
    let menu = Menu::new();
    let item_start = MenuItem::new("Start Capture", true, None);
    let item_stop = MenuItem::new("Stop Capture", true, None);
    let item_open = MenuItem::new("Open Dashboard", true, None);
    let item_quit = MenuItem::new("Quit", true, None);
    menu.append_items(&[
        &item_start,
        &item_stop,
        &PredefinedMenuItem::separator(),
        &item_open,
        &PredefinedMenuItem::separator(),
        &item_quit,
    ])?;

    let start_id = item_start.id().clone();
    let stop_id = item_stop.id().clone();
    let open_id = item_open.id().clone();
    let quit_id = item_quit.id().clone();

    // Create tray icon (using a generated purple square as placeholder)
    let icon = create_default_icon();
    let _tray_icon = TrayIconBuilder::new()
        .with_menu(Box::new(menu))
        .with_tooltip("PurpleSector — Telemetry Capture")
        .with_icon(icon)
        .build()?;

    // Spawn the tokio runtime for the pipeline manager on a background thread
    let config_clone = config.clone();
    let stats_clone = stats.clone();
    let running_clone = running.clone();
    let egui_ctx_pipeline = egui_ctx.clone();
    let rt_handle = std::thread::spawn(move || {
        let rt = tokio::runtime::Runtime::new().expect("Failed to create tokio runtime");
        rt.block_on(pipeline::run_pipeline_manager(
            config_clone,
            stats_clone,
            running_clone,
            cmd_rx,
            egui_ctx_pipeline,
        ));
    });

    // Build GUI state
    let gui_state = GuiState {
        config: config.clone(),
        stats: stats.clone(),
        pipeline_running: running.clone(),
        command_tx: cmd_tx.clone(),
        app_quit_flag: app_quit_flag.clone(),
        egui_ctx: egui_ctx.clone(),
    };

    // Process tray menu events on a background thread
    let cmd_tx_menu = cmd_tx.clone();
    let app_quit_flag_menu = app_quit_flag.clone();
    let egui_ctx_menu = egui_ctx.clone();
    let menu_handle = std::thread::spawn(move || {
        loop {
            if app_quit_flag_menu.load(Ordering::Relaxed) {
                info!("Menu thread: quit flag set, breaking loop");
                break;
            }
            match MenuEvent::receiver().try_recv() {
                Ok(event) => {
                    info!("Menu thread: received event id={:?}", event.id);
                    if event.id == start_id {
                        let _ = cmd_tx_menu.blocking_send(PipelineCommand::Start);
                    } else if event.id == stop_id {
                        let _ = cmd_tx_menu.blocking_send(PipelineCommand::Stop);
                    } else if event.id == open_id {
                        info!("Menu thread: Open Dashboard clicked, restoring window");
                        #[cfg(windows)]
                        platform::show_from_tray();
                        if let Some(ctx) = egui_ctx_menu.get() {
                            ctx.request_repaint();
                        }
                    } else if event.id == quit_id {
                        info!("Menu thread: Quit clicked, shutting down");
                        let _ = cmd_tx_menu.try_send(PipelineCommand::Quit);
                        // Give the pipeline a moment to flush before exiting.
                        // process::exit works regardless of window visibility.
                        std::thread::sleep(Duration::from_millis(200));
                        std::process::exit(0);
                    }
                }
                Err(_) => {
                    std::thread::sleep(Duration::from_millis(50));
                }
            }
        }
    });

    // UI loop (must run on the main thread for winit/eframe on Windows).
    // The tray thread sends OpenWindow/Quit commands to this loop.
    // Open the dashboard on first launch.
    let _ = ui_cmd_tx.send(UiCommand::OpenWindow);

    loop {
        match ui_cmd_rx.recv() {
            Ok(UiCommand::OpenWindow) => {
                gui::open_window(gui_state.clone());
                info!("GUI window closed, running in tray...");
            }
            Err(e) => {
                info!("Main thread: ui_cmd_rx recv error: {e:?}, breaking loop");
                break;
            }
        }
    }

    // Ensure all background threads observe quit and terminate.
    app_quit_flag.store(true, Ordering::Relaxed);
    drop(cmd_tx);

    let _ = menu_handle.join();

    // Wait for the pipeline to flush and the tokio runtime to exit cleanly.
    // A watchdog ensures the process always terminates even if a task hangs.
    let (done_tx, done_rx) = std::sync::mpsc::channel::<()>();
    std::thread::spawn(move || {
        let _ = rt_handle.join();
        let _ = done_tx.send(());
    });
    match done_rx.recv_timeout(Duration::from_secs(3)) {
        Ok(_) => info!("Pipeline shut down cleanly"),
        Err(_) => info!("Pipeline shutdown timed out, forcing exit"),
    }

    std::process::exit(0);
}

/// Create a simple 32x32 purple icon as a placeholder.
fn create_default_icon() -> tray_icon::Icon {
    let size = 32u32;
    let mut rgba = Vec::with_capacity((size * size * 4) as usize);
    for y in 0..size {
        for x in 0..size {
            // Simple purple circle
            let cx = (x as f32) - (size as f32 / 2.0);
            let cy = (y as f32) - (size as f32 / 2.0);
            let r = (size as f32 / 2.0) - 2.0;
            if cx * cx + cy * cy <= r * r {
                rgba.extend_from_slice(&[139, 92, 246, 255]); // Purple (#8B5CF6)
            } else {
                rgba.extend_from_slice(&[0, 0, 0, 0]); // Transparent
            }
        }
    }
    tray_icon::Icon::from_rgba(rgba, size, size).expect("Failed to create icon")
}
