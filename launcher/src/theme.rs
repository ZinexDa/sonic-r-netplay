use egui::{Color32, Stroke, Visuals};

pub const COLOR_BG: Color32 = Color32::from_rgb(11, 16, 38);
#[allow(dead_code)]
pub const COLOR_PANEL: Color32 = Color32::from_rgb(14, 21, 51);
pub const COLOR_FRAME: Color32 = Color32::from_rgb(8, 12, 29);
pub const COLOR_GOLD: Color32 = Color32::from_rgb(255, 208, 0);
pub const COLOR_SONIC_BLUE: Color32 = Color32::from_rgb(0, 102, 238);
pub const COLOR_BLUE_HOVER: Color32 = Color32::from_rgb(32, 128, 255);
#[allow(dead_code)]
pub const COLOR_BLUE_ACTIVE: Color32 = Color32::from_rgb(0, 78, 189);
pub const COLOR_LOBBY_GREEN: Color32 = Color32::from_rgb(0, 230, 118);
pub const COLOR_RACE_GRAY: Color32 = Color32::from_rgb(136, 136, 153);
pub const COLOR_STOP_RED: Color32 = Color32::from_rgb(230, 45, 60);
#[allow(dead_code)]
pub const COLOR_RED_HOVER: Color32 = Color32::from_rgb(255, 70, 85);
pub const COLOR_TEXT_MUTED: Color32 = Color32::from_rgb(160, 168, 204);

pub fn apply_theme(ctx: &egui::Context) {
    let mut visuals = Visuals::dark();
    visuals.panel_fill = COLOR_PANEL;
    visuals.window_fill = COLOR_PANEL;
    visuals.extreme_bg_color = COLOR_FRAME;

    // Normal widgets
    visuals.widgets.inactive.weak_bg_fill = COLOR_FRAME;
    visuals.widgets.inactive.bg_fill = COLOR_SONIC_BLUE;
    visuals.widgets.inactive.fg_stroke = Stroke::new(1.0_f32, Color32::WHITE);
    visuals.widgets.inactive.rounding = egui::Rounding::same(4.0);

    visuals.widgets.hovered.weak_bg_fill = COLOR_BLUE_HOVER;
    visuals.widgets.hovered.bg_fill = COLOR_BLUE_HOVER;
    visuals.widgets.hovered.fg_stroke = Stroke::new(1.0_f32, Color32::WHITE);
    visuals.widgets.hovered.rounding = egui::Rounding::same(4.0);

    visuals.widgets.active.weak_bg_fill = COLOR_BLUE_ACTIVE;
    visuals.widgets.active.bg_fill = COLOR_BLUE_ACTIVE;
    visuals.widgets.active.fg_stroke = Stroke::new(1.0_f32, COLOR_GOLD);
    visuals.widgets.active.rounding = egui::Rounding::same(4.0);

    visuals.widgets.noninteractive.bg_fill = COLOR_FRAME;
    visuals.widgets.noninteractive.fg_stroke = Stroke::new(1.0_f32, Color32::WHITE);
    visuals.widgets.noninteractive.rounding = egui::Rounding::same(4.0);

    ctx.set_visuals(visuals);
}
