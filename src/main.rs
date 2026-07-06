mod command;
mod config;
mod database;
mod resp;
mod server;

use std::str::FromStr;

use clap::Parser;
use tracing::{Level, error};

use config::Config;
use server::run;

#[tokio::main]
async fn main() {
    let config = Config::parse();
    init_tracing(&config.log_level);

    if let Err(cause) = run(&config).await {
        error!(address = %config.address(), %cause, "failed to start server");
        std::process::exit(1);
    }
}

/// Installs the global tracing subscriber at the requested level, falling back
/// to `INFO` if the level string is not recognised.
fn init_tracing(level: &str) {
    let level = Level::from_str(level).unwrap_or(Level::INFO);
    tracing_subscriber::fmt()
        .with_max_level(level)
        .with_target(false)
        .init();
}
