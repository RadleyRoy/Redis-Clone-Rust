//! The append-only file (AOF): a write-ahead log of every mutating command.
//!
//! Each write is appended as a RESP array — the same framing clients send — so
//! replaying the log is just re-feeding those requests through the normal
//! command path. The fsync policy trades durability against throughput, exactly
//! as Redis' `appendfsync` does.

use std::io;
use std::path::Path;
use std::str::FromStr;

use tokio::fs::{File, OpenOptions};
use tokio::io::AsyncWriteExt;
use tokio::sync::Mutex;

use crate::resp;

/// When to flush the AOF to stable storage.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum FsyncPolicy {
    /// `fsync` after every write — safest, slowest.
    Always,
    /// `fsync` roughly once per second from a background task.
    EverySec,
    /// Never `fsync` explicitly; leave it to the operating system.
    No,
}

impl FromStr for FsyncPolicy {
    type Err = String;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        match value.to_ascii_lowercase().as_str() {
            "always" => Ok(FsyncPolicy::Always),
            "everysec" => Ok(FsyncPolicy::EverySec),
            "no" => Ok(FsyncPolicy::No),
            other => Err(format!(
                "unknown fsync policy '{other}' (expected always, everysec, or no)"
            )),
        }
    }
}

/// A handle to the append-only file. Appends are serialized behind a mutex so
/// interleaved writes from concurrent connections never corrupt an entry.
pub struct Aof {
    file: Mutex<File>,
    policy: FsyncPolicy,
}

impl Aof {
    /// Opens (creating if needed) the AOF for appending.
    pub async fn open(path: &Path, policy: FsyncPolicy) -> io::Result<Self> {
        let file = OpenOptions::new()
            .create(true)
            .append(true)
            .open(path)
            .await?;
        Ok(Self {
            file: Mutex::new(file),
            policy,
        })
    }

    /// Appends one command (as a RESP array), fsyncing immediately under the
    /// `Always` policy.
    pub async fn append(&self, tokens: &[String]) -> io::Result<()> {
        let payload = resp::array(tokens);
        let mut file = self.file.lock().await;
        file.write_all(payload.as_bytes()).await?;
        if self.policy == FsyncPolicy::Always {
            file.sync_data().await?;
        }
        Ok(())
    }

    /// Flushes buffered writes to stable storage.
    pub async fn sync(&self) -> io::Result<()> {
        self.file.lock().await.sync_data().await
    }
}
