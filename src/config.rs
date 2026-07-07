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

    /// Path to the JSON snapshot file, loaded on startup and written by SAVE
    /// and on shutdown.
    #[arg(long, env = "REDIS_CLONE_SNAPSHOT", default_value = "dump.rdb.json")]
    pub snapshot_file: String,

    /// Append every write to a log and replay it on startup (durability at the
    /// cost of throughput). When enabled, the AOF is used for recovery instead
    /// of the snapshot.
    #[arg(long, env = "REDIS_CLONE_APPENDONLY", default_value_t = false)]
    pub appendonly: bool,

    /// AOF fsync policy: always, everysec, or no.
    #[arg(long, env = "REDIS_CLONE_APPENDFSYNC", default_value = "everysec")]
    pub appendfsync: String,

    /// Path to the append-only file.
    #[arg(long, env = "REDIS_CLONE_AOF_FILE", default_value = "appendonly.aof")]
    pub aof_file: String,
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
