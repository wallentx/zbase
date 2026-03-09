#![allow(dead_code)]

mod app;
mod domain;
mod models;
mod services;
mod state;
mod util;
mod views;

fn main() {
    tracing_subscriber::fmt()
        .with_max_level(tracing::Level::WARN)
        .with_target(false)
        .init();
    app::bootstrap::run();
}
