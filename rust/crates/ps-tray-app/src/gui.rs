//! egui-based GUI windows for settings and live stats.

use eframe::egui;
use std::sync::{Arc, Mutex};
use tracing::info;

use crate::config::{AppConfig, SimType};
use crate::pipeline::PipelineCommand;
use crate::platform;
use crate::stats::PipelineStats;

/// Shared state between the tray menu and the GUI.
#[derive(Clone)]
pub struct GuiState {
    pub config: Arc<Mutex<AppConfig>>,
    pub stats: PipelineStats,
    pub pipeline_running: Arc<std::sync::atomic::AtomicBool>,
    pub command_tx: tokio::sync::mpsc::Sender<PipelineCommand>,
    pub app_quit_flag: Arc<std::sync::atomic::AtomicBool>,
    /// Holds the egui context once the window is open, so external threads
    /// can call request_repaint() to wake the event loop immediately.
    pub egui_ctx: Arc<std::sync::OnceLock<egui::Context>>,
}

/// Which view is active in the main window.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum View {
    Stats,
    Settings,
}

/// Main application window combining stats and settings.
pub struct AppWindow {
    state: GuiState,
    active_view: View,
    // Editable settings (buffered until Save)
    edit_sim: SimType,
    edit_endpoint: String,
    edit_user_id: String,
    edit_password: String,
    edit_acc_password: String,
    edit_acc_interval: String,
    edit_batch_interval: String,
    status_message: Option<(String, std::time::Instant)>,
}

impl AppWindow {
    pub fn new(state: GuiState) -> Self {
        let config = state.config.lock().unwrap().clone();
        Self {
            state,
            active_view: View::Stats,
            edit_sim: config.sim_type,
            edit_endpoint: config.grpc_endpoint.clone(),
            edit_user_id: config.user_id.clone(),
            edit_password: config.password.clone(),
            edit_acc_password: config.acc_connection_password.clone(),
            edit_acc_interval: config.acc_update_interval_ms.to_string(),
            edit_batch_interval: config.batch_interval_ms.to_string(),
            status_message: None,
        }
    }

    fn open_settings(&mut self) {
        self.reload_from_config();
        self.active_view = View::Settings;
    }

    fn render_stats(&self, ui: &mut egui::Ui) {
        let snap = self.state.stats.snapshot();
        let running = self
            .state
            .pipeline_running
            .load(std::sync::atomic::Ordering::Relaxed);

        ui.horizontal(|ui| {
            ui.heading("Pipeline Status");
            if running {
                ui.label(
                    egui::RichText::new("● Running")
                        .color(egui::Color32::from_rgb(34, 197, 94))
                        .strong(),
                );
            } else {
                ui.label(
                    egui::RichText::new("● Stopped")
                        .color(egui::Color32::from_rgb(239, 68, 68))
                        .strong(),
                );
            }
        });

        ui.add_space(12.0);

        egui::Grid::new("stats_grid")
            .num_columns(2)
            .spacing([40.0, 8.0])
            .striped(true)
            .show(ui, |ui| {
                ui.label("Samples captured:");
                ui.label(
                    egui::RichText::new(format!("{}", snap.samples_captured)).monospace(),
                );
                ui.end_row();

                ui.label("Samples/sec:");
                ui.label(
                    egui::RichText::new(format!("{:.1}", snap.samples_per_sec)).monospace(),
                );
                ui.end_row();

                ui.label("Batches sent:");
                ui.label(
                    egui::RichText::new(format!("{}", snap.batches_sent)).monospace(),
                );
                ui.end_row();

                ui.label("Batches failed:");
                let fail_color = if snap.batches_failed > 0 {
                    egui::Color32::from_rgb(239, 68, 68)
                } else {
                    egui::Color32::from_rgb(156, 163, 175)
                };
                ui.label(
                    egui::RichText::new(format!("{}", snap.batches_failed))
                        .monospace()
                        .color(fail_color),
                );
                ui.end_row();

                ui.label("WAL depth:");
                ui.label(
                    egui::RichText::new(format!("{}", snap.wal_depth)).monospace(),
                );
                ui.end_row();

                ui.label("Bytes sent:");
                ui.label(
                    egui::RichText::new(format_bytes(snap.bytes_sent)).monospace(),
                );
                ui.end_row();

                ui.label("Uptime:");
                ui.label(
                    egui::RichText::new(format_duration(snap.uptime_secs)).monospace(),
                );
                ui.end_row();
            });

        ui.add_space(16.0);

        ui.horizontal(|ui| {
            if running {
                if ui.button("⏹  Stop Capture").clicked() {
                    let _ = self.state.command_tx.try_send(PipelineCommand::Stop);
                }
            } else {
                if ui.button("▶  Start Capture").clicked() {
                    let _ = self.state.command_tx.try_send(PipelineCommand::Start);
                }
            }
        });
    }

    fn render_settings(&mut self, ui: &mut egui::Ui) {
        ui.heading("Settings");
        ui.add_space(8.0);

        // ── Sim Type ──
        ui.label(egui::RichText::new("Sim Type").strong());
        egui::ComboBox::from_id_salt("sim_type")
            .selected_text(self.edit_sim.label())
            .show_ui(ui, |ui| {
                for sim in SimType::all() {
                    ui.selectable_value(&mut self.edit_sim, *sim, sim.label());
                }
            });

        ui.add_space(12.0);

        // ── gRPC Endpoint ──
        ui.label(egui::RichText::new("gRPC Endpoint").strong());
        ui.text_edit_singleline(&mut self.edit_endpoint);
        ui.label(
            egui::RichText::new("e.g. http://localhost:50051 for local dev")
                .small()
                .weak(),
        );

        ui.add_space(12.0);

        // ── Credentials ──
        ui.label(egui::RichText::new("Login").strong());
        ui.horizontal(|ui| {
            ui.label("User ID:");
            ui.text_edit_singleline(&mut self.edit_user_id);
        });
        ui.horizontal(|ui| {
            ui.label("Password:");
            ui.add(egui::TextEdit::singleline(&mut self.edit_password).password(true));
        });
        ui.label(
            egui::RichText::new("Auth placeholder — will be replaced by OIDC browser flow")
                .small()
                .weak(),
        );

        ui.add_space(12.0);

        // ── ACC-specific ──
        ui.collapsing("ACC Settings", |ui| {
            ui.horizontal(|ui| {
                ui.label("Broadcast password:");
                ui.add(
                    egui::TextEdit::singleline(&mut self.edit_acc_password).password(true),
                );
            });
            ui.horizontal(|ui| {
                ui.label("Update interval (ms):");
                ui.text_edit_singleline(&mut self.edit_acc_interval);
            });
        });

        ui.add_space(8.0);

        // ── Batch interval ──
        ui.horizontal(|ui| {
            ui.label(egui::RichText::new("Batch interval (ms):").strong());
            ui.text_edit_singleline(&mut self.edit_batch_interval);
        });

        ui.add_space(16.0);

        // ── Save / Revert ──
        ui.horizontal(|ui| {
            if ui.button("💾  Save & Apply").clicked() {
                self.save_settings();
            }
            if ui.button("Cancel").clicked() {
                self.reload_from_config();
                self.active_view = View::Stats;
            }
        });

        // Status message
        if let Some((msg, when)) = &self.status_message {
            if when.elapsed().as_secs() < 5 {
                ui.add_space(8.0);
                ui.label(
                    egui::RichText::new(msg)
                        .color(egui::Color32::from_rgb(34, 197, 94)),
                );
            }
        }
    }

    fn save_settings(&mut self) {
        let mut config = self.state.config.lock().unwrap();
        config.sim_type = self.edit_sim;
        config.grpc_endpoint = self.edit_endpoint.clone();
        config.user_id = self.edit_user_id.clone();
        config.password = self.edit_password.clone();
        config.acc_connection_password = self.edit_acc_password.clone();
        config.acc_update_interval_ms = self
            .edit_acc_interval
            .parse()
            .unwrap_or(config.acc_update_interval_ms);
        config.batch_interval_ms = self
            .edit_batch_interval
            .parse()
            .unwrap_or(config.batch_interval_ms);

        match config.save() {
            Ok(()) => {
                self.status_message =
                    Some(("Settings saved!".into(), std::time::Instant::now()));
                // Notify pipeline of config change
                let _ = self
                    .state
                    .command_tx
                    .try_send(PipelineCommand::ReloadConfig);
                self.active_view = View::Stats;
            }
            Err(e) => {
                self.status_message = Some((
                    format!("Save failed: {e}"),
                    std::time::Instant::now(),
                ));
            }
        }
    }

    fn reload_from_config(&mut self) {
        let config = self.state.config.lock().unwrap().clone();
        self.edit_sim = config.sim_type;
        self.edit_endpoint = config.grpc_endpoint;
        self.edit_user_id = config.user_id;
        self.edit_password = config.password;
        self.edit_acc_password = config.acc_connection_password;
        self.edit_acc_interval = config.acc_update_interval_ms.to_string();
        self.edit_batch_interval = config.batch_interval_ms.to_string();
    }
}

impl eframe::App for AppWindow {
    fn update(&mut self, ctx: &egui::Context, _frame: &mut eframe::Frame) {
        // Check close_requested FIRST, before scheduling any repaints.
        // Cancel the OS close, then hide the window via Win32 while the event
        // loop keeps running. This is the correct "minimize to tray" sequence.
        let close_requested = ctx.input(|i| i.viewport().close_requested());
        if close_requested {
            info!("AppWindow: close_requested, hiding to tray");
            ctx.send_viewport_cmd(egui::ViewportCommand::CancelClose);
            #[cfg(windows)]
            platform::hide_to_tray();
            return;
        }

        // Check quit flag FIRST - exit the process cleanly
        if self.state.app_quit_flag.load(std::sync::atomic::Ordering::Relaxed) {
            info!("AppWindow: quit flag set, exiting process");
            std::process::exit(0);
        }

        // Auto-refresh stats at ~10Hz (only when not closing)
        ctx.request_repaint_after(std::time::Duration::from_millis(100));

        egui::TopBottomPanel::top("tabs").show(ctx, |ui| {
            egui::menu::bar(ui, |ui| {
                ui.menu_button("File", |ui| {
                    if ui.button("Settings").clicked() {
                        self.open_settings();
                        ui.close_menu();
                    }

                    ui.separator();

                    if ui.button("Quit").clicked() {
                        let _ = self.state.command_tx.try_send(PipelineCommand::Quit);
                        std::process::exit(0);
                    }
                });

                ui.with_layout(egui::Layout::right_to_left(egui::Align::Center), |ui| {
                    ui.label(
                        egui::RichText::new(format!("PurpleSector v{}", env!("CARGO_PKG_VERSION")))
                            .small()
                            .weak(),
                    );
                });
            });
        });

        egui::CentralPanel::default().show(ctx, |ui| match self.active_view {
            View::Stats => self.render_stats(ui),
            View::Settings => self.render_settings(ui),
        });

    }
}

// ... (rest of the code remains the same)
/// Launch the egui window (blocking on the current thread).
pub fn open_window(state: GuiState) {
    let options = eframe::NativeOptions {
        viewport: egui::ViewportBuilder::default()
            .with_inner_size([420.0, 520.0])
            .with_min_inner_size([360.0, 400.0])
            .with_title(platform::WINDOW_TITLE),
        run_and_return: true,
        ..Default::default()
    };

    let _ = eframe::run_native(
        "PurpleSector Tray App",
        options,
        Box::new(move |cc| {
            let _ = state.egui_ctx.set(cc.egui_ctx.clone());
            Ok(Box::new(AppWindow::new(state)))
        }),
    );
    info!("open_window: eframe::run_native returned");
}

// ── Formatting helpers ───────────────────────────────────────────────

fn format_bytes(bytes: u64) -> String {
    if bytes < 1024 {
        format!("{} B", bytes)
    } else if bytes < 1024 * 1024 {
        format!("{:.1} KB", bytes as f64 / 1024.0)
    } else if bytes < 1024 * 1024 * 1024 {
        format!("{:.1} MB", bytes as f64 / (1024.0 * 1024.0))
    } else {
        format!("{:.2} GB", bytes as f64 / (1024.0 * 1024.0 * 1024.0))
    }
}

fn format_duration(secs: f64) -> String {
    let h = (secs / 3600.0) as u64;
    let m = ((secs % 3600.0) / 60.0) as u64;
    let s = (secs % 60.0) as u64;
    if h > 0 {
        format!("{}h {:02}m {:02}s", h, m, s)
    } else if m > 0 {
        format!("{}m {:02}s", m, s)
    } else {
        format!("{}s", s)
    }
}
