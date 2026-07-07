//! The in-memory key/value store shared by every client connection.
//!
//! All keys live in a single map from name to [`Entry`]. Storing one [`Value`]
//! per key (instead of a separate map per type) means a key holds exactly one
//! type at a time, expiry applies uniformly to every type, and `DEL` works
//! regardless of what the key contains. Type mismatches are reported with a
//! typed [`StoreError`].
//!
//! Read commands take a shared read lock and only upgrade to a write lock when
//! they actually need to evict an expired key, so concurrent reads of live keys
//! do not block one another.

use std::collections::{HashMap, HashSet};
use std::fmt;
use std::io;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};

use tokio::sync::RwLock;
use tokio::sync::mpsc::UnboundedSender;
use tokio::time::{Duration, Instant};
use tracing::warn;

use crate::database::aof::Aof;
use crate::database::data_structure::{RHash, RList, RSet, RSortedSet};
use crate::database::pubsub::PubSub;
use crate::database::snapshot::{SerValue, Snapshot, SnapshotEntry};

/// Errors the store can return to a caller.
#[derive(Debug, PartialEq, Eq)]
pub enum StoreError {
    /// A command was applied to a key holding a different type.
    WrongType,
    /// A numeric command (`INCR`/`INCRBY`/`DECR`) was applied to a string that
    /// does not parse as an integer.
    NotAnInteger,
    /// `HINCRBY` was applied to a hash field that does not parse as an integer.
    NotAHashInteger,
    /// An increment/decrement would overflow `i64`.
    Overflow,
    /// A command (e.g. `LSET`) required a key that does not exist.
    NoSuchKey,
    /// A list index was out of range (`LSET`).
    IndexOutOfRange,
}

impl fmt::Display for StoreError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            StoreError::WrongType => {
                f.write_str("WRONGTYPE Operation against a key holding the wrong kind of value")
            }
            StoreError::NotAnInteger => f.write_str("ERR value is not an integer or out of range"),
            StoreError::NotAHashInteger => f.write_str("ERR hash value is not an integer"),
            StoreError::Overflow => f.write_str("ERR increment or decrement would overflow"),
            StoreError::NoSuchKey => f.write_str("ERR no such key"),
            StoreError::IndexOutOfRange => f.write_str("ERR index out of range"),
        }
    }
}

/// The value stored at a key. A key holds exactly one of these at a time.
enum Value {
    Str(String),
    List(RList),
    Set(RSet),
    SortedSet(RSortedSet),
    Hash(RHash),
}

/// A stored value together with its optional expiry deadline.
struct Entry {
    value: Value,
    expires_at: Option<Instant>,
}

impl Entry {
    fn is_expired(&self) -> bool {
        matches!(self.expires_at, Some(deadline) if Instant::now() >= deadline)
    }
}

type Store = HashMap<String, Entry>;

/// Returns a mutable reference to a live (non-expired) entry, lazily evicting
/// the key first if it has expired. `None` means "no such live key".
fn live_entry<'a>(store: &'a mut Store, key: &str) -> Option<&'a mut Entry> {
    if store.get(key).is_some_and(Entry::is_expired) {
        store.remove(key);
        return None;
    }
    store.get_mut(key)
}

/// Redis-style glob matching, used by `KEYS`. Supports `*` (any run, including
/// empty), `?` (exactly one byte), `[...]` character classes (with `^`/`!`
/// negation and `a-z` ranges), and `\` escaping. Matching is byte-wise, as in
/// Redis' own `stringmatchlen`.
fn glob_match(pattern: &str, text: &str) -> bool {
    glob_match_bytes(pattern.as_bytes(), text.as_bytes())
}

fn glob_match_bytes(mut pattern: &[u8], mut text: &[u8]) -> bool {
    while let Some((&token, rest)) = pattern.split_first() {
        match token {
            b'*' => {
                // Collapse a run of `*` into one, then try to match the tail at
                // every position (including the end).
                let mut tail_pattern = rest;
                while let Some((&b'*', next)) = tail_pattern.split_first() {
                    tail_pattern = next;
                }
                if tail_pattern.is_empty() {
                    return true;
                }
                let mut tail_text = text;
                loop {
                    if glob_match_bytes(tail_pattern, tail_text) {
                        return true;
                    }
                    match tail_text.split_first() {
                        Some((_, next)) => tail_text = next,
                        None => return false,
                    }
                }
            }
            b'?' => match text.split_first() {
                Some((_, next)) => {
                    text = next;
                    pattern = rest;
                }
                None => return false,
            },
            b'[' => {
                let ch = match text.first() {
                    Some(&ch) => ch,
                    None => return false,
                };
                let (matched, consumed) = match_class(rest, ch);
                if !matched {
                    return false;
                }
                pattern = &rest[consumed..];
                text = &text[1..];
            }
            b'\\' => {
                // A backslash escapes the next byte (or is a literal backslash
                // at end of pattern).
                let (literal, next_pattern) = match rest.split_first() {
                    Some((&literal, next)) => (literal, next),
                    None => (b'\\', rest),
                };
                match text.split_first() {
                    Some((&ch, next)) if ch == literal => {
                        text = next;
                        pattern = next_pattern;
                    }
                    _ => return false,
                }
            }
            literal => match text.split_first() {
                Some((&ch, next)) if ch == literal => {
                    text = next;
                    pattern = rest;
                }
                _ => return false,
            },
        }
    }
    text.is_empty()
}

/// Matches one byte against a `[...]` class whose opening bracket has already
/// been consumed. Returns whether it matched and how many bytes of `class`
/// (up to and including the closing `]`) were consumed.
fn match_class(class: &[u8], ch: u8) -> (bool, usize) {
    let mut i = 0;
    let negate = matches!(class.first(), Some(b'^') | Some(b'!'));
    if negate {
        i += 1;
    }
    let mut matched = false;
    while i < class.len() && class[i] != b']' {
        if class[i] == b'\\' && i + 1 < class.len() {
            matched |= class[i + 1] == ch;
            i += 2;
        } else if i + 2 < class.len() && class[i + 1] == b'-' && class[i + 2] != b']' {
            let (lo, hi) = (class[i].min(class[i + 2]), class[i].max(class[i + 2]));
            matched |= (lo..=hi).contains(&ch);
            i += 3;
        } else {
            matched |= class[i] == ch;
            i += 1;
        }
    }
    let consumed = if i < class.len() { i + 1 } else { i };
    (matched != negate, consumed)
}

/// Generates a `fn(store, key) -> Result<&mut Inner, StoreError>` that fetches
/// the inner collection for `key`, creating an empty one when the key is absent
/// (or expired) and returning [`StoreError::WrongType`] when the key holds
/// another type. This is the get-or-create path shared by the write commands.
macro_rules! entry_accessor {
    ($name:ident, $variant:ident, $inner:ty) => {
        fn $name(store: &mut Store, key: String) -> Result<&mut $inner, StoreError> {
            if store.get(&key).is_some_and(Entry::is_expired) {
                store.remove(&key);
            }
            let entry = store.entry(key).or_insert_with(|| Entry {
                value: Value::$variant(<$inner>::new()),
                expires_at: None,
            });
            match &mut entry.value {
                Value::$variant(inner) => Ok(inner),
                _ => Err(StoreError::WrongType),
            }
        }
    };
}

entry_accessor!(list_or_create, List, RList);
entry_accessor!(set_or_create, Set, RSet);
entry_accessor!(sorted_set_or_create, SortedSet, RSortedSet);
entry_accessor!(hash_or_create, Hash, RHash);

/// Collects a key's set members for the multi-key set operations
/// (`SINTER`/`SUNION`/`SDIFF`). A missing or expired key is an empty set; a key
/// of another type is a [`StoreError::WrongType`].
fn set_members_of(store: &Store, key: &str) -> Result<HashSet<String>, StoreError> {
    match store.get(key) {
        Some(entry) if !entry.is_expired() => match &entry.value {
            Value::Set(set) => Ok(set.smembers().into_iter().collect()),
            _ => Err(StoreError::WrongType),
        },
        _ => Ok(HashSet::new()),
    }
}

/// Server-wide counters that back the `INFO` command. Shared across every
/// connection alongside the key store.
struct Stats {
    started_at: Instant,
    connected_clients: AtomicUsize,
}

impl Stats {
    fn new() -> Self {
        Self {
            started_at: Instant::now(),
            connected_clients: AtomicUsize::new(0),
        }
    }
}

/// Persistence configuration shared across connections: where snapshots live,
/// the optional append-only log, and a flag marking that commands are currently
/// being replayed from that log (so they must not be re-appended to it).
#[derive(Default)]
struct Persistence {
    snapshot_path: Option<PathBuf>,
    aof: Option<Aof>,
    replaying: AtomicBool,
}

/// A thread-safe, in-memory store. Cloning shares the same underlying data, so
/// every connection task operates on one database.
#[derive(Clone)]
pub struct Database {
    store: Arc<RwLock<Store>>,
    stats: Arc<Stats>,
    persistence: Arc<Persistence>,
    pubsub: Arc<PubSub>,
}

impl Default for Database {
    fn default() -> Self {
        Self {
            store: Arc::new(RwLock::new(HashMap::new())),
            stats: Arc::new(Stats::new()),
            persistence: Arc::new(Persistence::default()),
            pubsub: Arc::new(PubSub::default()),
        }
    }
}

impl Database {
    pub fn new() -> Self {
        Self::default()
    }

    /// Constructs a database with the given persistence backends: an optional
    /// snapshot path and an optional append-only log.
    pub fn with_persistence(snapshot_path: Option<PathBuf>, aof: Option<Aof>) -> Self {
        let mut database = Self::new();
        database.persistence = Arc::new(Persistence {
            snapshot_path,
            aof,
            replaying: AtomicBool::new(false),
        });
        database
    }

    /// Reads the live entry for `key` under a shared read lock, passing it to
    /// `read`. Returns `absent` when the key is missing. If the key exists but
    /// has expired it is evicted under a write lock and treated as absent, so
    /// only genuinely live keys take the read-lock fast path.
    async fn read_entry<T>(&self, key: &str, absent: T, read: impl FnOnce(&Entry) -> T) -> T {
        {
            let store = self.store.read().await;
            match store.get(key) {
                Some(entry) if !entry.is_expired() => return read(entry),
                None => return absent,
                Some(_) => {} // expired: fall through to eviction below
            }
        }
        let mut store = self.store.write().await;
        if store.get(key).is_some_and(Entry::is_expired) {
            store.remove(key);
        }
        absent
    }

    // --- Strings --------------------------------------------------------

    pub async fn set(&self, key: String, value: String, ttl: Option<u64>) {
        let mut store = self.store.write().await;
        let expires_at = ttl.map(|seconds| Instant::now() + Duration::from_secs(seconds));
        store.insert(
            key,
            Entry {
                value: Value::Str(value),
                expires_at,
            },
        );
    }

    pub async fn get(&self, key: &str) -> Result<Option<String>, StoreError> {
        self.read_entry(key, Ok(None), |entry| match &entry.value {
            Value::Str(value) => Ok(Some(value.clone())),
            _ => Err(StoreError::WrongType),
        })
        .await
    }

    /// Deletes `key` regardless of the type it holds. Returns `true` if a live
    /// key was removed.
    pub async fn delete(&self, key: &str) -> bool {
        let mut store = self.store.write().await;
        // Treat an already-expired key as "not present".
        if live_entry(&mut store, key).is_none() {
            return false;
        }
        store.remove(key).is_some()
    }

    /// Deletes each of `keys`, returning how many live keys were removed. Backs
    /// the variadic `DEL key [key ...]`.
    pub async fn del(&self, keys: &[String]) -> usize {
        let mut removed = 0;
        for key in keys {
            if self.delete(key).await {
                removed += 1;
            }
        }
        removed
    }

    /// Adds `delta` to the integer value at `key`, creating it (starting from 0)
    /// if absent and preserving any existing TTL. Backs `INCR`/`DECR`/`INCRBY`/
    /// `DECRBY`. Returns the new value.
    pub async fn incr_by(&self, key: String, delta: i64) -> Result<i64, StoreError> {
        let mut store = self.store.write().await;
        if store.get(&key).is_some_and(Entry::is_expired) {
            store.remove(&key);
        }
        let entry = store.entry(key).or_insert_with(|| Entry {
            value: Value::Str("0".to_string()),
            expires_at: None,
        });
        match &mut entry.value {
            Value::Str(current) => {
                let value = current
                    .parse::<i64>()
                    .map_err(|_| StoreError::NotAnInteger)?;
                let next = value.checked_add(delta).ok_or(StoreError::Overflow)?;
                *current = next.to_string();
                Ok(next)
            }
            _ => Err(StoreError::WrongType),
        }
    }

    /// Appends `suffix` to the string at `key`, creating it if absent. Returns
    /// the new length.
    pub async fn append(&self, key: String, suffix: &str) -> Result<usize, StoreError> {
        let mut store = self.store.write().await;
        if store.get(&key).is_some_and(Entry::is_expired) {
            store.remove(&key);
        }
        let entry = store.entry(key).or_insert_with(|| Entry {
            value: Value::Str(String::new()),
            expires_at: None,
        });
        match &mut entry.value {
            Value::Str(current) => {
                current.push_str(suffix);
                Ok(current.len())
            }
            _ => Err(StoreError::WrongType),
        }
    }

    pub async fn strlen(&self, key: &str) -> Result<usize, StoreError> {
        self.read_entry(key, Ok(0), |entry| match &entry.value {
            Value::Str(value) => Ok(value.len()),
            _ => Err(StoreError::WrongType),
        })
        .await
    }

    /// Returns the string value of each key, with `None` for keys that are
    /// missing, expired, or hold a non-string type (as Redis' `MGET` does).
    pub async fn mget(&self, keys: &[String]) -> Vec<Option<String>> {
        let store = self.store.read().await;
        keys.iter()
            .map(|key| match store.get(key) {
                Some(entry) if !entry.is_expired() => match &entry.value {
                    Value::Str(value) => Some(value.clone()),
                    _ => None,
                },
                _ => None,
            })
            .collect()
    }

    /// Sets every (key, value) pair, overwriting any existing values.
    pub async fn mset(&self, pairs: Vec<(String, String)>) {
        let mut store = self.store.write().await;
        for (key, value) in pairs {
            store.insert(
                key,
                Entry {
                    value: Value::Str(value),
                    expires_at: None,
                },
            );
        }
    }

    /// Sets `key` only if it does not already exist. Returns `true` if it was
    /// set.
    pub async fn setnx(&self, key: String, value: String) -> bool {
        let mut store = self.store.write().await;
        if live_entry(&mut store, &key).is_some() {
            return false;
        }
        store.insert(
            key,
            Entry {
                value: Value::Str(value),
                expires_at: None,
            },
        );
        true
    }

    /// Sets `key` to `value` and returns the previous string value (or `None`).
    /// Clears any existing TTL, as Redis' `GETSET` does.
    pub async fn getset(&self, key: String, value: String) -> Result<Option<String>, StoreError> {
        let mut store = self.store.write().await;
        let previous = match live_entry(&mut store, &key) {
            Some(entry) => match &entry.value {
                Value::Str(current) => Some(current.clone()),
                _ => return Err(StoreError::WrongType),
            },
            None => None,
        };
        store.insert(
            key,
            Entry {
                value: Value::Str(value),
                expires_at: None,
            },
        );
        Ok(previous)
    }

    // --- Lists ----------------------------------------------------------

    pub async fn lpush(&self, key: String, values: Vec<String>) -> Result<usize, StoreError> {
        let mut store = self.store.write().await;
        let list = list_or_create(&mut store, key)?;
        for value in values {
            list.lpush(value);
        }
        Ok(list.len())
    }

    pub async fn rpush(&self, key: String, values: Vec<String>) -> Result<usize, StoreError> {
        let mut store = self.store.write().await;
        let list = list_or_create(&mut store, key)?;
        for value in values {
            list.rpush(value);
        }
        Ok(list.len())
    }

    pub async fn lpop(&self, key: &str) -> Result<Option<String>, StoreError> {
        self.pop(key, |list| list.lpop()).await
    }

    pub async fn rpop(&self, key: &str) -> Result<Option<String>, StoreError> {
        self.pop(key, |list| list.rpop()).await
    }

    /// Shared implementation of `LPOP`/`RPOP`: pops via `take`, and drops the
    /// key entirely once its list becomes empty (as Redis does).
    async fn pop(
        &self,
        key: &str,
        take: impl FnOnce(&mut RList) -> Option<String>,
    ) -> Result<Option<String>, StoreError> {
        let mut store = self.store.write().await;
        let (popped, is_empty) = match live_entry(&mut store, key) {
            None => return Ok(None),
            Some(entry) => match &mut entry.value {
                Value::List(list) => (take(list), list.is_empty()),
                _ => return Err(StoreError::WrongType),
            },
        };
        if is_empty {
            store.remove(key);
        }
        Ok(popped)
    }

    pub async fn lrange(
        &self,
        key: &str,
        start: i64,
        stop: i64,
    ) -> Result<Vec<String>, StoreError> {
        self.read_entry(key, Ok(Vec::new()), |entry| match &entry.value {
            Value::List(list) => Ok(list.lrange(start, stop)),
            _ => Err(StoreError::WrongType),
        })
        .await
    }

    pub async fn llen(&self, key: &str) -> Result<usize, StoreError> {
        self.read_entry(key, Ok(0), |entry| match &entry.value {
            Value::List(list) => Ok(list.len()),
            _ => Err(StoreError::WrongType),
        })
        .await
    }

    pub async fn lindex(&self, key: &str, index: i64) -> Result<Option<String>, StoreError> {
        self.read_entry(key, Ok(None), |entry| match &entry.value {
            Value::List(list) => Ok(list.lindex(index)),
            _ => Err(StoreError::WrongType),
        })
        .await
    }

    /// Overwrites the element at `index`. Errors with [`StoreError::NoSuchKey`]
    /// if the key is missing and [`StoreError::IndexOutOfRange`] if the index is
    /// out of bounds.
    pub async fn lset(&self, key: &str, index: i64, value: String) -> Result<(), StoreError> {
        let mut store = self.store.write().await;
        match live_entry(&mut store, key) {
            None => Err(StoreError::NoSuchKey),
            Some(entry) => match &mut entry.value {
                Value::List(list) => {
                    if list.lset(index, value) {
                        Ok(())
                    } else {
                        Err(StoreError::IndexOutOfRange)
                    }
                }
                _ => Err(StoreError::WrongType),
            },
        }
    }

    // --- Sets -----------------------------------------------------------

    pub async fn sadd(&self, key: String, values: Vec<String>) -> Result<usize, StoreError> {
        let mut store = self.store.write().await;
        let set = set_or_create(&mut store, key)?;
        let mut added = 0;
        for value in values {
            if set.sadd(value) {
                added += 1;
            }
        }
        Ok(added)
    }

    pub async fn srem(&self, key: &str, values: &[String]) -> Result<usize, StoreError> {
        let mut store = self.store.write().await;
        let (removed, is_empty) = match live_entry(&mut store, key) {
            None => return Ok(0),
            Some(entry) => match &mut entry.value {
                Value::Set(set) => {
                    let removed = values.iter().filter(|value| set.srem(value)).count();
                    (removed, set.is_empty())
                }
                _ => return Err(StoreError::WrongType),
            },
        };
        if is_empty {
            store.remove(key);
        }
        Ok(removed)
    }

    pub async fn smembers(&self, key: &str) -> Result<Vec<String>, StoreError> {
        self.read_entry(key, Ok(Vec::new()), |entry| match &entry.value {
            Value::Set(set) => Ok(set.smembers()),
            _ => Err(StoreError::WrongType),
        })
        .await
    }

    pub async fn sismember(&self, key: &str, value: &str) -> Result<bool, StoreError> {
        self.read_entry(key, Ok(false), |entry| match &entry.value {
            Value::Set(set) => Ok(set.sismember(value)),
            _ => Err(StoreError::WrongType),
        })
        .await
    }

    pub async fn scard(&self, key: &str) -> Result<usize, StoreError> {
        self.read_entry(key, Ok(0), |entry| match &entry.value {
            Value::Set(set) => Ok(set.scard()),
            _ => Err(StoreError::WrongType),
        })
        .await
    }

    /// Removes and returns an arbitrary member, dropping the key if the set
    /// becomes empty.
    pub async fn spop(&self, key: &str) -> Result<Option<String>, StoreError> {
        let mut store = self.store.write().await;
        let (popped, is_empty) = match live_entry(&mut store, key) {
            None => return Ok(None),
            Some(entry) => match &mut entry.value {
                Value::Set(set) => (set.spop(), set.is_empty()),
                _ => return Err(StoreError::WrongType),
            },
        };
        if is_empty {
            store.remove(key);
        }
        Ok(popped)
    }

    /// Returns the intersection of the given sets. Missing keys are empty sets.
    pub async fn sinter(&self, keys: &[String]) -> Result<Vec<String>, StoreError> {
        let store = self.store.read().await;
        let mut keys = keys.iter();
        let mut result = match keys.next() {
            Some(key) => set_members_of(&store, key)?,
            None => return Ok(Vec::new()),
        };
        for key in keys {
            let other = set_members_of(&store, key)?;
            result.retain(|member| other.contains(member));
        }
        Ok(result.into_iter().collect())
    }

    /// Returns the union of the given sets.
    pub async fn sunion(&self, keys: &[String]) -> Result<Vec<String>, StoreError> {
        let store = self.store.read().await;
        let mut result = HashSet::new();
        for key in keys {
            result.extend(set_members_of(&store, key)?);
        }
        Ok(result.into_iter().collect())
    }

    /// Returns the members of the first set that are not in any of the rest.
    pub async fn sdiff(&self, keys: &[String]) -> Result<Vec<String>, StoreError> {
        let store = self.store.read().await;
        let mut keys = keys.iter();
        let mut result = match keys.next() {
            Some(key) => set_members_of(&store, key)?,
            None => return Ok(Vec::new()),
        };
        for key in keys {
            for member in set_members_of(&store, key)? {
                result.remove(&member);
            }
        }
        Ok(result.into_iter().collect())
    }

    // --- Sorted sets ----------------------------------------------------

    pub async fn zadd(
        &self,
        key: String,
        members: Vec<(f64, String)>,
    ) -> Result<usize, StoreError> {
        let mut store = self.store.write().await;
        let zset = sorted_set_or_create(&mut store, key)?;
        let mut added = 0;
        for (score, member) in members {
            if zset.zadd(score, member) {
                added += 1;
            }
        }
        Ok(added)
    }

    pub async fn zrem(&self, key: &str, member: &str) -> Result<bool, StoreError> {
        let mut store = self.store.write().await;
        let (removed, is_empty) = match live_entry(&mut store, key) {
            None => return Ok(false),
            Some(entry) => match &mut entry.value {
                Value::SortedSet(zset) => (zset.zrem(member), zset.is_empty()),
                _ => return Err(StoreError::WrongType),
            },
        };
        if is_empty {
            store.remove(key);
        }
        Ok(removed)
    }

    pub async fn zrange(
        &self,
        key: &str,
        start: i64,
        stop: i64,
    ) -> Result<Vec<String>, StoreError> {
        self.read_entry(key, Ok(Vec::new()), |entry| match &entry.value {
            Value::SortedSet(zset) => Ok(zset.zrange(start, stop)),
            _ => Err(StoreError::WrongType),
        })
        .await
    }

    pub async fn zscore(&self, key: &str, member: &str) -> Result<Option<f64>, StoreError> {
        self.read_entry(key, Ok(None), |entry| match &entry.value {
            Value::SortedSet(zset) => Ok(zset.zscore(member)),
            _ => Err(StoreError::WrongType),
        })
        .await
    }

    pub async fn zcard(&self, key: &str) -> Result<usize, StoreError> {
        self.read_entry(key, Ok(0), |entry| match &entry.value {
            Value::SortedSet(zset) => Ok(zset.zcard()),
            _ => Err(StoreError::WrongType),
        })
        .await
    }

    pub async fn zrank(&self, key: &str, member: &str) -> Result<Option<usize>, StoreError> {
        self.read_entry(key, Ok(None), |entry| match &entry.value {
            Value::SortedSet(zset) => Ok(zset.zrank(member)),
            _ => Err(StoreError::WrongType),
        })
        .await
    }

    /// Increments `member`'s score by `increment`, creating the sorted set (and
    /// the member, from 0) if absent. Returns the new score.
    pub async fn zincrby(
        &self,
        key: String,
        increment: f64,
        member: String,
    ) -> Result<f64, StoreError> {
        let mut store = self.store.write().await;
        Ok(sorted_set_or_create(&mut store, key)?.zincrby(increment, member))
    }

    pub async fn zrange_with_scores(
        &self,
        key: &str,
        start: i64,
        stop: i64,
    ) -> Result<Vec<(String, f64)>, StoreError> {
        self.read_entry(key, Ok(Vec::new()), |entry| match &entry.value {
            Value::SortedSet(zset) => Ok(zset.zrange_with_scores(start, stop)),
            _ => Err(StoreError::WrongType),
        })
        .await
    }

    pub async fn zrange_by_score(
        &self,
        key: &str,
        min: f64,
        min_inclusive: bool,
        max: f64,
        max_inclusive: bool,
    ) -> Result<Vec<String>, StoreError> {
        self.read_entry(key, Ok(Vec::new()), |entry| match &entry.value {
            Value::SortedSet(zset) => {
                Ok(zset.zrange_by_score(min, min_inclusive, max, max_inclusive))
            }
            _ => Err(StoreError::WrongType),
        })
        .await
    }

    // --- Hashes ---------------------------------------------------------

    pub async fn hset(
        &self,
        key: String,
        field: String,
        value: String,
    ) -> Result<bool, StoreError> {
        let mut store = self.store.write().await;
        Ok(hash_or_create(&mut store, key)?.hset(field, value))
    }

    pub async fn hget(&self, key: &str, field: &str) -> Result<Option<String>, StoreError> {
        self.read_entry(key, Ok(None), |entry| match &entry.value {
            Value::Hash(hash) => Ok(hash.hget(field).cloned()),
            _ => Err(StoreError::WrongType),
        })
        .await
    }

    pub async fn hdel(&self, key: &str, field: &str) -> Result<bool, StoreError> {
        let mut store = self.store.write().await;
        let (removed, is_empty) = match live_entry(&mut store, key) {
            None => return Ok(false),
            Some(entry) => match &mut entry.value {
                Value::Hash(hash) => (hash.hdel(field), hash.is_empty()),
                _ => return Err(StoreError::WrongType),
            },
        };
        if is_empty {
            store.remove(key);
        }
        Ok(removed)
    }

    /// Returns the hash as a flat `[field, value, field, value, ...]` vector,
    /// the order `HGETALL` replies in.
    pub async fn hgetall(&self, key: &str) -> Result<Vec<String>, StoreError> {
        self.read_entry(key, Ok(Vec::new()), |entry| match &entry.value {
            Value::Hash(hash) => {
                let mut flat = Vec::with_capacity(hash.hlen() * 2);
                for (field, value) in hash.hgetall() {
                    flat.push(field);
                    flat.push(value);
                }
                Ok(flat)
            }
            _ => Err(StoreError::WrongType),
        })
        .await
    }

    pub async fn hkeys(&self, key: &str) -> Result<Vec<String>, StoreError> {
        self.read_entry(key, Ok(Vec::new()), |entry| match &entry.value {
            Value::Hash(hash) => Ok(hash.hkeys()),
            _ => Err(StoreError::WrongType),
        })
        .await
    }

    pub async fn hvals(&self, key: &str) -> Result<Vec<String>, StoreError> {
        self.read_entry(key, Ok(Vec::new()), |entry| match &entry.value {
            Value::Hash(hash) => Ok(hash.hvals()),
            _ => Err(StoreError::WrongType),
        })
        .await
    }

    pub async fn hlen(&self, key: &str) -> Result<usize, StoreError> {
        self.read_entry(key, Ok(0), |entry| match &entry.value {
            Value::Hash(hash) => Ok(hash.hlen()),
            _ => Err(StoreError::WrongType),
        })
        .await
    }

    pub async fn hexists(&self, key: &str, field: &str) -> Result<bool, StoreError> {
        self.read_entry(key, Ok(false), |entry| match &entry.value {
            Value::Hash(hash) => Ok(hash.hexists(field)),
            _ => Err(StoreError::WrongType),
        })
        .await
    }

    /// Increments the integer value of `field` by `delta`, creating the hash
    /// (and the field, starting from 0) if absent. Returns the new value.
    pub async fn hincrby(&self, key: String, field: String, delta: i64) -> Result<i64, StoreError> {
        let mut store = self.store.write().await;
        let hash = hash_or_create(&mut store, key)?;
        let current = match hash.hget(&field) {
            Some(value) => value
                .parse::<i64>()
                .map_err(|_| StoreError::NotAHashInteger)?,
            None => 0,
        };
        let next = current.checked_add(delta).ok_or(StoreError::Overflow)?;
        hash.hset(field, next.to_string());
        Ok(next)
    }

    // --- Generic key operations -----------------------------------------

    /// Counts how many of `keys` name a live key. Duplicates are counted each
    /// time, matching Redis' `EXISTS`.
    pub async fn exists(&self, keys: &[String]) -> usize {
        let store = self.store.read().await;
        keys.iter()
            .filter(|key| matches!(store.get(*key), Some(entry) if !entry.is_expired()))
            .count()
    }

    /// Returns the Redis type name of `key` (`string`/`list`/`set`/`zset`/
    /// `hash`), or `none` if it is missing or expired.
    pub async fn type_of(&self, key: &str) -> &'static str {
        self.read_entry(key, "none", |entry| match &entry.value {
            Value::Str(_) => "string",
            Value::List(_) => "list",
            Value::Set(_) => "set",
            Value::SortedSet(_) => "zset",
            Value::Hash(_) => "hash",
        })
        .await
    }

    /// Returns every live key whose name matches the glob `pattern`.
    pub async fn keys(&self, pattern: &str) -> Vec<String> {
        let store = self.store.read().await;
        store
            .iter()
            .filter(|(_, entry)| !entry.is_expired())
            .filter(|(key, _)| glob_match(pattern, key))
            .map(|(key, _)| key.clone())
            .collect()
    }

    /// Sets a TTL of `seconds` on `key`. A non-positive TTL deletes the key
    /// immediately (as Redis does). Returns `true` if the key existed.
    pub async fn expire(&self, key: &str, seconds: i64) -> bool {
        let mut store = self.store.write().await;
        if live_entry(&mut store, key).is_none() {
            return false;
        }
        if seconds <= 0 {
            store.remove(key);
        } else if let Some(entry) = store.get_mut(key) {
            entry.expires_at = Some(Instant::now() + Duration::from_secs(seconds as u64));
        }
        true
    }

    /// Returns the remaining TTL in seconds: `-2` if the key does not exist,
    /// `-1` if it exists but has no expiry, otherwise the seconds remaining.
    pub async fn ttl(&self, key: &str) -> i64 {
        self.read_entry(key, -2, |entry| match entry.expires_at {
            // `read_entry` only runs this for live entries, so the deadline is
            // in the future here.
            Some(deadline) => deadline.saturating_duration_since(Instant::now()).as_secs() as i64,
            None => -1,
        })
        .await
    }

    /// Removes any TTL from `key`. Returns `true` only if a TTL was actually
    /// removed.
    pub async fn persist(&self, key: &str) -> bool {
        let mut store = self.store.write().await;
        match live_entry(&mut store, key) {
            Some(entry) if entry.expires_at.is_some() => {
                entry.expires_at = None;
                true
            }
            _ => false,
        }
    }

    /// Renames `key` to `new_key`, moving its value and TTL. Returns `false` if
    /// `key` does not exist (the caller turns this into `ERR no such key`).
    pub async fn rename(&self, key: &str, new_key: String) -> bool {
        let mut store = self.store.write().await;
        if live_entry(&mut store, key).is_none() {
            return false;
        }
        match store.remove(key) {
            Some(entry) => {
                store.insert(new_key, entry);
                true
            }
            None => false,
        }
    }

    /// Returns the number of live keys in the store.
    pub async fn dbsize(&self) -> usize {
        let store = self.store.read().await;
        store.values().filter(|entry| !entry.is_expired()).count()
    }

    /// Removes every key.
    pub async fn flushall(&self) {
        self.store.write().await.clear();
    }

    // --- Server / maintenance -------------------------------------------

    /// Evicts every expired key under one write lock, returning how many were
    /// removed. Backs the background sweeper; lazy eviction still happens on
    /// access, so this is an optimisation, not a correctness requirement.
    pub async fn sweep_expired(&self) -> usize {
        let mut store = self.store.write().await;
        let before = store.len();
        store.retain(|_, entry| !entry.is_expired());
        before - store.len()
    }

    /// Records a newly connected client, returning the new client count.
    pub fn client_connected(&self) -> usize {
        self.stats.connected_clients.fetch_add(1, Ordering::Relaxed) + 1
    }

    /// Records a client disconnecting.
    pub fn client_disconnected(&self) {
        self.stats.connected_clients.fetch_sub(1, Ordering::Relaxed);
    }

    /// Renders the `INFO` reply: a human-readable, Redis-style block of
    /// `field:value` lines grouped into sections.
    pub async fn info(&self) -> String {
        let uptime = self.stats.started_at.elapsed().as_secs();
        let clients = self.stats.connected_clients.load(Ordering::Relaxed);
        let keys = self.dbsize().await;
        format!(
            "# Server\r\n\
             server_name:redis_clone\r\n\
             uptime_in_seconds:{uptime}\r\n\
             \r\n# Clients\r\n\
             connected_clients:{clients}\r\n\
             \r\n# Keyspace\r\n\
             keys:{keys}\r\n"
        )
    }

    // --- Pub/Sub --------------------------------------------------------

    /// Hands out a process-unique id for a subscribing connection.
    pub fn next_subscriber_id(&self) -> u64 {
        self.pubsub.next_id()
    }

    /// Registers a connection's mailbox as a subscriber on `channel`.
    pub fn subscribe(&self, channel: String, id: u64, sender: UnboundedSender<String>) {
        self.pubsub.subscribe(channel, id, sender);
    }

    /// Removes a connection's subscription to `channel`.
    pub fn unsubscribe(&self, channel: &str, id: u64) {
        self.pubsub.unsubscribe(channel, id);
    }

    /// Publishes `payload` to `channel`, returning the number of subscribers
    /// that received it.
    pub fn publish(&self, channel: &str, payload: &str) -> usize {
        self.pubsub.publish(channel, payload)
    }

    // --- Persistence ----------------------------------------------------

    /// Builds a serializable snapshot of every live key, recording each TTL as
    /// the number of seconds remaining.
    pub async fn snapshot(&self) -> Snapshot {
        let store = self.store.read().await;
        let now = Instant::now();
        let entries = store
            .iter()
            .filter(|(_, entry)| !entry.is_expired())
            .map(|(key, entry)| {
                let ttl_secs = entry
                    .expires_at
                    .map(|deadline| deadline.saturating_duration_since(now).as_secs());
                let value = match &entry.value {
                    Value::Str(value) => SerValue::Str(value.clone()),
                    Value::List(list) => SerValue::List(list.lrange(0, -1)),
                    Value::Set(set) => SerValue::Set(set.smembers()),
                    Value::SortedSet(zset) => SerValue::ZSet(zset.zrange_with_scores(0, -1)),
                    Value::Hash(hash) => SerValue::Hash(hash.hgetall()),
                };
                SnapshotEntry {
                    key: key.clone(),
                    ttl_secs,
                    value,
                }
            })
            .collect();
        Snapshot { entries }
    }

    /// Replaces the entire keyspace with the contents of `snapshot`, turning
    /// each stored remaining-seconds TTL back into a deadline.
    pub async fn load_snapshot(&self, snapshot: Snapshot) {
        let mut store = self.store.write().await;
        store.clear();
        for entry in snapshot.entries {
            let value = match entry.value {
                SerValue::Str(value) => Value::Str(value),
                SerValue::List(items) => {
                    let mut list = RList::new();
                    for item in items {
                        list.rpush(item);
                    }
                    Value::List(list)
                }
                SerValue::Set(items) => {
                    let mut set = RSet::new();
                    for item in items {
                        set.sadd(item);
                    }
                    Value::Set(set)
                }
                SerValue::ZSet(items) => {
                    let mut zset = RSortedSet::new();
                    for (member, score) in items {
                        zset.zadd(score, member);
                    }
                    Value::SortedSet(zset)
                }
                SerValue::Hash(items) => {
                    let mut hash = RHash::new();
                    for (field, value) in items {
                        hash.hset(field, value);
                    }
                    Value::Hash(hash)
                }
            };
            let expires_at = entry
                .ttl_secs
                .map(|secs| Instant::now() + Duration::from_secs(secs));
            store.insert(entry.key, Entry { value, expires_at });
        }
    }

    /// Writes a JSON snapshot to `path`, via a temporary file and rename so a
    /// crash mid-write cannot corrupt an existing snapshot.
    pub async fn save_to(&self, path: &Path) -> io::Result<()> {
        let snapshot = self.snapshot().await;
        let json = serde_json::to_vec_pretty(&snapshot).map_err(io::Error::other)?;
        let temp = path.with_extension("tmp");
        tokio::fs::write(&temp, json).await?;
        tokio::fs::rename(&temp, path).await?;
        Ok(())
    }

    /// Loads a snapshot from `path` if it exists. Returns `true` if a snapshot
    /// was loaded, `false` if the file was simply absent.
    pub async fn load_from(&self, path: &Path) -> io::Result<bool> {
        match tokio::fs::read(path).await {
            Ok(bytes) => {
                let snapshot: Snapshot =
                    serde_json::from_slice(&bytes).map_err(io::Error::other)?;
                self.load_snapshot(snapshot).await;
                Ok(true)
            }
            Err(error) if error.kind() == io::ErrorKind::NotFound => Ok(false),
            Err(error) => Err(error),
        }
    }

    /// The configured snapshot path, if persistence is enabled.
    pub fn snapshot_path(&self) -> Option<&Path> {
        self.persistence.snapshot_path.as_deref()
    }

    /// Writes a snapshot to the configured path. A no-op (still `Ok`) when no
    /// snapshot path is configured. Backs the `SAVE`/`BGSAVE` commands and the
    /// save-on-shutdown path.
    pub async fn save(&self) -> io::Result<()> {
        match self.snapshot_path() {
            Some(path) => self.save_to(path).await,
            None => Ok(()),
        }
    }

    /// Appends a successfully executed write command to the AOF, if one is
    /// configured. Skipped while replaying, so replayed commands are not logged
    /// back to the file they came from.
    pub async fn log_write(&self, tokens: &[String]) {
        if self.persistence.replaying.load(Ordering::Relaxed) {
            return;
        }
        if let Some(aof) = &self.persistence.aof
            && let Err(cause) = aof.append(tokens).await
        {
            warn!(%cause, "failed to append to AOF");
        }
    }

    /// Marks the start of AOF replay, suppressing re-logging until
    /// [`end_replay`](Self::end_replay).
    pub fn begin_replay(&self) {
        self.persistence.replaying.store(true, Ordering::Relaxed);
    }

    /// Marks the end of AOF replay.
    pub fn end_replay(&self) {
        self.persistence.replaying.store(false, Ordering::Relaxed);
    }

    /// Flushes the AOF to disk, if one is configured.
    pub async fn sync_aof(&self) -> io::Result<()> {
        match &self.persistence.aof {
            Some(aof) => aof.sync().await,
            None => Ok(()),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn set_get_and_delete() {
        let db = Database::new();
        db.set("k".to_string(), "v".to_string(), None).await;
        assert_eq!(db.get("k").await.unwrap(), Some("v".to_string()));
        assert!(db.delete("k").await);
        assert!(!db.delete("k").await);
        assert_eq!(db.get("k").await.unwrap(), None);
    }

    #[tokio::test]
    async fn delete_works_across_every_type() {
        let db = Database::new();
        db.rpush("list".to_string(), vec!["a".to_string()])
            .await
            .unwrap();
        assert!(db.delete("list").await);
        assert_eq!(
            db.lrange("list", 0, -1).await.unwrap(),
            Vec::<String>::new()
        );
    }

    #[tokio::test]
    async fn wrong_type_is_reported() {
        let db = Database::new();
        db.lpush("list".to_string(), vec!["a".to_string()])
            .await
            .unwrap();
        assert_eq!(db.get("list").await, Err(StoreError::WrongType));
    }

    #[tokio::test]
    async fn expired_keys_are_evicted_lazily() {
        let db = Database::new();
        db.set("k".to_string(), "v".to_string(), Some(0)).await;
        assert_eq!(db.get("k").await.unwrap(), None);
    }

    #[tokio::test]
    async fn sweep_removes_only_expired_keys() {
        let db = Database::new();
        db.set("live".to_string(), "v".to_string(), None).await;
        db.set("gone".to_string(), "v".to_string(), Some(0)).await;
        assert_eq!(db.sweep_expired().await, 1);
        assert_eq!(db.dbsize().await, 1);
        assert_eq!(db.get("live").await.unwrap(), Some("v".to_string()));
    }

    #[tokio::test]
    async fn snapshot_round_trips_every_type() {
        let source = Database::new();
        source.set("s".to_string(), "v".to_string(), None).await;
        source
            .rpush("l".to_string(), vec!["a".to_string(), "b".to_string()])
            .await
            .unwrap();
        source
            .sadd("set".to_string(), vec!["x".to_string()])
            .await
            .unwrap();
        source
            .zadd("z".to_string(), vec![(1.5, "m".to_string())])
            .await
            .unwrap();
        source
            .hset("h".to_string(), "f".to_string(), "1".to_string())
            .await
            .unwrap();

        // Round-trip the snapshot into a fresh database.
        let snapshot = source.snapshot().await;
        let restored = Database::new();
        restored.load_snapshot(snapshot).await;

        assert_eq!(restored.get("s").await.unwrap(), Some("v".to_string()));
        assert_eq!(
            restored.lrange("l", 0, -1).await.unwrap(),
            vec!["a".to_string(), "b".to_string()]
        );
        assert!(restored.sismember("set", "x").await.unwrap());
        assert_eq!(restored.zscore("z", "m").await.unwrap(), Some(1.5));
        assert_eq!(
            restored.hget("h", "f").await.unwrap(),
            Some("1".to_string())
        );
        assert_eq!(restored.dbsize().await, 5);
    }

    #[tokio::test]
    async fn info_reports_clients_and_keys() {
        let db = Database::new();
        db.set("k".to_string(), "v".to_string(), None).await;
        assert_eq!(db.client_connected(), 1);
        assert_eq!(db.client_connected(), 2);
        db.client_disconnected();
        let info = db.info().await;
        assert!(info.contains("connected_clients:1"), "{info}");
        assert!(info.contains("keys:1"), "{info}");
        assert!(info.contains("uptime_in_seconds:"), "{info}");
    }

    #[tokio::test]
    async fn popping_the_last_element_removes_the_key() {
        let db = Database::new();
        db.rpush("l".to_string(), vec!["only".to_string()])
            .await
            .unwrap();
        assert_eq!(db.lpop("l").await.unwrap(), Some("only".to_string()));
        // A fresh push must succeed, proving the old (empty) key was cleared.
        assert_eq!(
            db.rpush("l".to_string(), vec!["x".to_string()])
                .await
                .unwrap(),
            1
        );
    }
}
