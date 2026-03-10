#![allow(dead_code)]

mod app;
mod domain;
mod models;
mod services;
mod state;
mod util;
mod views;

fn parse_log_level(raw: &str) -> Option<tracing::Level> {
    match raw.trim().to_ascii_lowercase().as_str() {
        "trace" => Some(tracing::Level::TRACE),
        "debug" => Some(tracing::Level::DEBUG),
        "info" => Some(tracing::Level::INFO),
        "warn" | "warning" => Some(tracing::Level::WARN),
        "error" => Some(tracing::Level::ERROR),
        _ => None,
    }
}

fn default_log_level() -> tracing::Level {
    let bench_mode =
        std::env::var("KBUI_BENCH_AUTOSTART").is_ok() || std::env::var("KBUI_BENCH_SCRIPT").is_ok();
    if bench_mode {
        tracing::Level::WARN
    } else {
        tracing::Level::ERROR
    }
}

fn main() {
    let max_level = std::env::var("KBUI_LOG_LEVEL")
        .ok()
        .and_then(|raw| parse_log_level(&raw))
        .unwrap_or_else(default_log_level);
    tracing_subscriber::fmt()
        .with_max_level(max_level)
        .with_target(false)
        .init();
    app::bootstrap::run();
}
