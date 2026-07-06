//! Runtime configuration, parsed from command-line flags with environment
//! variable fallbacks. Keeping every knob in one typed struct means the rest of
//! the program reads settings from a value rather than from scattered
//! constants or `std::env` lookups.

use std::time::Duration;

use clap::Parser;

/// Command-line configuration for the server. Each flag also reads an
/// environment variable, so `--port 6400` and `REDIS_CLONE_PORT=6400` are
/// equivalent.
#[derive(Parser, Debug, Clone)]
#[command(
    name = "redis_clone",
    about = "A small Redis-compatible in-memory server"
)]
pub struct Config {
    /// Address to bind the TCP listener to.
    #[arg(long, env = "REDIS_CLONE_HOST", default_value = "127.0.0.1")]
    pub host: String,

    /// Port to listen on.
    #[arg(long, short, env = "REDIS_CLONE_PORT", default_value_t = 7335)]
    pub port: u16,

    /// Log verbosity: error, warn, info, debug, or trace.
    #[arg(long, env = "REDIS_CLONE_LOG", default_value = "info")]
    pub log_level: String,

    /// How often, in seconds, the background sweeper evicts expired keys.
    #[arg(long, env = "REDIS_CLONE_SWEEP_SECS", default_value_t = 10)]
    pub sweep_secs: u64,
}

impl Config {
    /// The `host:port` string to bind to.
    pub fn address(&self) -> String {
        format!("{}:{}", self.host, self.port)
    }

    /// The sweeper interval, clamped to at least one second so a misconfigured
    /// `0` cannot spin the eviction task.
    pub fn sweep_interval(&self) -> Duration {
        Duration::from_secs(self.sweep_secs.max(1))
    }
}
