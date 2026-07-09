//! The on-disk snapshot format, decoupled from the live in-memory types.
//!
//! Persisting a separate, flat representation (rather than serializing the
//! runtime structures directly) keeps the wire/disk format stable and avoids
//! writing redundant internal state — for example a sorted set keeps two
//! in-memory indexes but only needs its `(member, score)` pairs on disk. TTLs
//! are stored as *remaining seconds* so they survive a restart, where an
//! absolute `Instant` would be meaningless.

use serde::{Deserialize, Serialize};

/// A full point-in-time copy of the keyspace.
#[derive(Debug, Serialize, Deserialize)]
pub struct Snapshot {
    pub entries: Vec<SnapshotEntry>,
}

/// One key: its name, optional remaining TTL, and value.
#[derive(Debug, Serialize, Deserialize)]
pub struct SnapshotEntry {
    pub key: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub ttl_secs: Option<u64>,
    pub value: SerValue,
}

/// A serializable view of a stored value.
///
/// Sorted-set scores are stored as their string form rather than as JSON
/// numbers: `serde_json` cannot represent a non-finite `f64` and silently
/// serializes `inf`/`-inf`/`NaN` as `null`, which then fails to deserialize —
/// a single such score would make the entire snapshot unloadable. The string
/// form round-trips every `f64` exactly.
#[derive(Debug, Serialize, Deserialize)]
pub enum SerValue {
    Str(String),
    List(Vec<String>),
    Set(Vec<String>),
    ZSet(Vec<(String, String)>),
    Hash(Vec<(String, String)>),
}
