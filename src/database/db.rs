//! The in-memory key/value store shared by every client connection.
//!
//! All keys live in a single map from name to [`Entry`]. Storing one [`Value`]
//! per key (instead of a separate map per type) means a key holds exactly one
//! type at a time, expiry applies uniformly to every type, and `DEL` works
//! regardless of what the key contains. Type mismatches are reported with the
//! same [`WRONG_TYPE`] error Redis uses.

use std::collections::HashMap;
use std::sync::Arc;

use tokio::sync::RwLock;
use tokio::time::{Duration, Instant};

use crate::database::data_structure::{RList, RSet, RSortedSet};

/// Error message returned when a command is applied to a key that holds a
/// different type, mirroring Redis' `WRONGTYPE` reply.
pub const WRONG_TYPE: &str = "WRONGTYPE Operation against a key holding the wrong kind of value";

/// The value stored at a key. A key holds exactly one of these at a time.
enum Value {
    Str(String),
    List(RList),
    Set(RSet),
    SortedSet(RSortedSet),
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

/// Generates a `fn(store, key) -> Result<&mut Inner, String>` that fetches the
/// inner collection for `key`, creating an empty one when the key is absent (or
/// expired) and returning [`WRONG_TYPE`] when the key holds another type. This
/// is the get-or-create path shared by the write commands.
macro_rules! entry_accessor {
    ($name:ident, $variant:ident, $inner:ty) => {
        fn $name(store: &mut Store, key: String) -> Result<&mut $inner, String> {
            if store.get(&key).is_some_and(Entry::is_expired) {
                store.remove(&key);
            }
            let entry = store.entry(key).or_insert_with(|| Entry {
                value: Value::$variant(<$inner>::new()),
                expires_at: None,
            });
            match &mut entry.value {
                Value::$variant(inner) => Ok(inner),
                _ => Err(WRONG_TYPE.to_string()),
            }
        }
    };
}

entry_accessor!(list_or_create, List, RList);
entry_accessor!(set_or_create, Set, RSet);
entry_accessor!(sorted_set_or_create, SortedSet, RSortedSet);

/// A thread-safe, in-memory store. Cloning shares the same underlying data, so
/// every connection task operates on one database.
#[derive(Clone, Default)]
pub struct Database {
    store: Arc<RwLock<Store>>,
}

impl Database {
    pub fn new() -> Self {
        Self::default()
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

    pub async fn get(&self, key: &str) -> Result<Option<String>, String> {
        let mut store = self.store.write().await;
        match live_entry(&mut store, key) {
            None => Ok(None),
            Some(entry) => match &entry.value {
                Value::Str(value) => Ok(Some(value.clone())),
                _ => Err(WRONG_TYPE.to_string()),
            },
        }
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

    // --- Lists ----------------------------------------------------------

    pub async fn lpush(&self, key: String, value: String) -> Result<usize, String> {
        let mut store = self.store.write().await;
        let list = list_or_create(&mut store, key)?;
        list.lpush(value);
        Ok(list.len())
    }

    pub async fn rpush(&self, key: String, value: String) -> Result<usize, String> {
        let mut store = self.store.write().await;
        let list = list_or_create(&mut store, key)?;
        list.rpush(value);
        Ok(list.len())
    }

    pub async fn lpop(&self, key: &str) -> Result<Option<String>, String> {
        self.pop(key, |list| list.lpop()).await
    }

    pub async fn rpop(&self, key: &str) -> Result<Option<String>, String> {
        self.pop(key, |list| list.rpop()).await
    }

    /// Shared implementation of `LPOP`/`RPOP`: pops via `take`, and drops the
    /// key entirely once its list becomes empty (as Redis does).
    async fn pop(
        &self,
        key: &str,
        take: impl FnOnce(&mut RList) -> Option<String>,
    ) -> Result<Option<String>, String> {
        let mut store = self.store.write().await;
        let (popped, is_empty) = match live_entry(&mut store, key) {
            None => return Ok(None),
            Some(entry) => match &mut entry.value {
                Value::List(list) => (take(list), list.is_empty()),
                _ => return Err(WRONG_TYPE.to_string()),
            },
        };
        if is_empty {
            store.remove(key);
        }
        Ok(popped)
    }

    pub async fn lrange(&self, key: &str, start: i64, stop: i64) -> Result<Vec<String>, String> {
        let mut store = self.store.write().await;
        match live_entry(&mut store, key) {
            None => Ok(Vec::new()),
            Some(entry) => match &entry.value {
                Value::List(list) => Ok(list.lrange(start, stop)),
                _ => Err(WRONG_TYPE.to_string()),
            },
        }
    }

    // --- Sets -----------------------------------------------------------

    pub async fn sadd(&self, key: String, value: String) -> Result<bool, String> {
        let mut store = self.store.write().await;
        Ok(set_or_create(&mut store, key)?.sadd(value))
    }

    pub async fn srem(&self, key: &str, value: &str) -> Result<bool, String> {
        let mut store = self.store.write().await;
        let (removed, is_empty) = match live_entry(&mut store, key) {
            None => return Ok(false),
            Some(entry) => match &mut entry.value {
                Value::Set(set) => (set.srem(value), set.is_empty()),
                _ => return Err(WRONG_TYPE.to_string()),
            },
        };
        if is_empty {
            store.remove(key);
        }
        Ok(removed)
    }

    pub async fn smembers(&self, key: &str) -> Result<Vec<String>, String> {
        let mut store = self.store.write().await;
        match live_entry(&mut store, key) {
            None => Ok(Vec::new()),
            Some(entry) => match &entry.value {
                Value::Set(set) => Ok(set.smembers()),
                _ => Err(WRONG_TYPE.to_string()),
            },
        }
    }

    pub async fn sismember(&self, key: &str, value: &str) -> Result<bool, String> {
        let mut store = self.store.write().await;
        match live_entry(&mut store, key) {
            None => Ok(false),
            Some(entry) => match &entry.value {
                Value::Set(set) => Ok(set.sismember(value)),
                _ => Err(WRONG_TYPE.to_string()),
            },
        }
    }

    // --- Sorted sets ----------------------------------------------------

    pub async fn zadd(&self, key: String, member: String, score: f64) -> Result<bool, String> {
        let mut store = self.store.write().await;
        Ok(sorted_set_or_create(&mut store, key)?.zadd(score, member))
    }

    pub async fn zrem(&self, key: &str, member: &str) -> Result<bool, String> {
        let mut store = self.store.write().await;
        let (removed, is_empty) = match live_entry(&mut store, key) {
            None => return Ok(false),
            Some(entry) => match &mut entry.value {
                Value::SortedSet(zset) => (zset.zrem(member), zset.is_empty()),
                _ => return Err(WRONG_TYPE.to_string()),
            },
        };
        if is_empty {
            store.remove(key);
        }
        Ok(removed)
    }

    pub async fn zrange(&self, key: &str, start: i64, stop: i64) -> Result<Vec<String>, String> {
        let mut store = self.store.write().await;
        match live_entry(&mut store, key) {
            None => Ok(Vec::new()),
            Some(entry) => match &entry.value {
                Value::SortedSet(zset) => Ok(zset.zrange(start, stop)),
                _ => Err(WRONG_TYPE.to_string()),
            },
        }
    }

    pub async fn zscore(&self, key: &str, member: &str) -> Result<Option<f64>, String> {
        let mut store = self.store.write().await;
        match live_entry(&mut store, key) {
            None => Ok(None),
            Some(entry) => match &entry.value {
                Value::SortedSet(zset) => Ok(zset.zscore(member)),
                _ => Err(WRONG_TYPE.to_string()),
            },
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
        db.rpush("list".to_string(), "a".to_string()).await.unwrap();
        assert!(db.delete("list").await);
        assert_eq!(
            db.lrange("list", 0, -1).await.unwrap(),
            Vec::<String>::new()
        );
    }

    #[tokio::test]
    async fn wrong_type_is_reported() {
        let db = Database::new();
        db.lpush("list".to_string(), "a".to_string()).await.unwrap();
        assert_eq!(db.get("list").await, Err(WRONG_TYPE.to_string()));
    }

    #[tokio::test]
    async fn expired_keys_are_evicted_lazily() {
        let db = Database::new();
        db.set("k".to_string(), "v".to_string(), Some(0)).await;
        assert_eq!(db.get("k").await.unwrap(), None);
    }

    #[tokio::test]
    async fn popping_the_last_element_removes_the_key() {
        let db = Database::new();
        db.rpush("l".to_string(), "only".to_string()).await.unwrap();
        assert_eq!(db.lpop("l").await.unwrap(), Some("only".to_string()));
        // A fresh push must succeed, proving the old (empty) key was cleared.
        assert_eq!(db.rpush("l".to_string(), "x".to_string()).await.unwrap(), 1);
    }
}
