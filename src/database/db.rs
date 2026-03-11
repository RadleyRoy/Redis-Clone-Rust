use std::collections::HashMap;
use std::sync::Arc;

use tokio::sync::RwLock;
use tokio::time::{Duration, Instant};

use crate::database::data_structure::{RList, RSet, RSortedSet};

#[derive(Clone)]
pub struct Database {
    db: Arc<RwLock<HashMap<String, String>>>,
    expiry: Arc<RwLock<HashMap<String, Instant>>>,
    list: Arc<RwLock<HashMap<String, RList>>>,
    set: Arc<RwLock<HashMap<String, RSet>>>,
    sorted_set: Arc<RwLock<HashMap<String, RSortedSet>>>,
}

impl Database {
    pub fn new() -> Self {
        Database {
            db: Arc::new(RwLock::new(HashMap::new())),
            expiry: Arc::new(RwLock::new(HashMap::new())),
            list: Arc::new(RwLock::new(HashMap::new())),
            set: Arc::new(RwLock::new(HashMap::new())),
            sorted_set: Arc::new(RwLock::new(HashMap::new())),
        }
    }

    pub async fn set(&self, key: String, value: String, ttl: Option<u64>) {
        let mut db = self.db.write().await;
        db.insert(key.clone(), value);

        if let Some(seconds) = ttl {
            let expiry_time = Instant::now() + Duration::from_secs(seconds);
            let mut expiry_map = self.expiry.write().await;
            expiry_map.insert(key, expiry_time);
        }
    }

    pub async fn get(&self, key: &str) -> Option<String> {
        if self.is_expired(key).await {
            self.delete(key).await;
            return None;
        }
        let db = self.db.read().await;
        db.get(key).cloned()
    }

    pub async fn is_expired(&self, key: &str) -> bool {
        let expiry_map = self.expiry.read().await;
        if let Some(&expiry_time) = expiry_map.get(key) {
            return Instant::now() > expiry_time;
        }
        false
    }

    pub async fn delete(&self, key: &str) -> bool {
        let mut db = self.db.write().await;
        let mut expiry = self.expiry.write().await;
        db.remove(key);
        expiry.remove(key).is_some()
    }

    pub async fn lpush(&self, key: String, value: String) {
        let mut list_map = self.list.write().await;
        let list = list_map.entry(key).or_insert_with(RList::new);
        list.lpush(value);
    }

    pub async fn rpush(&self, key: String, value: String) {
        let mut list_map = self.list.write().await;
        let list = list_map.entry(key).or_insert_with(RList::new);
        list.rpush(value);
    }

    pub async fn lpop(&self, key: &str) -> Option<String> {
        let mut list_map = self.list.write().await;
        if let Some(list) = list_map.get_mut(key) {
            list.lpop()
        } else {
            None
        }
    }

    pub async fn rpop(&self, key: &str) -> Option<String> {
        let mut list_map = self.list.write().await;
        if let Some(list) = list_map.get_mut(key) {
            list.rpop()
        } else {
            None
        }
    }

    pub async fn lrange(&self, start: usize, end: usize, key: &str) -> Option<Vec<String>> {
        let list_map = self.list.read().await;
        list_map.get(key).map(|list| list.lrange(start, end))
    }

    pub async fn sadd(&self, key: String, value: String) -> bool {
        let mut set_map = self.set.write().await;
        let set = set_map.entry(key).or_insert_with(RSet::new);
        set.sadd(value)
    }

    pub async fn srem(&self, key: &str, value: &str) -> bool {
        let mut set_map = self.set.write().await;
        if let Some(set) = set_map.get_mut(key) {
            set.srem(value)
        } else {
            false
        }
    }

    pub async fn smembers(&self, key: &str) -> Option<Vec<String>> {
        let set_map = self.set.read().await;
        set_map.get(key).map(|set| set.smembers())
    }

    pub async fn sismember(&self, key: &str, value: &str) -> bool {
        let set_map = self.set.read().await;
        if let Some(set) = set_map.get(key) {
            set.sismember(value)
        } else {
            false
        }
    }

    pub async fn zadd(&self, key: String, member: String, score: f64) -> bool {
        let mut sorted_set_map = self.sorted_set.write().await;
        let sorted_set = sorted_set_map.entry(key).or_insert_with(RSortedSet::new);
        sorted_set.zadd(score, member)
    }

    pub async fn zrem(&self, key: &str, member: &str) -> bool {
        let mut sorted_set_map = self.sorted_set.write().await;
        if let Some(sorted_set) = sorted_set_map.get_mut(key) {
            sorted_set.zrem(member)
        } else {
            false
        }
    }

    pub async fn zrange(&self, key: &str, start: usize, end: usize) -> Option<Vec<String>> {
        let sorted_set_map = self.sorted_set.read().await;
        sorted_set_map
            .get(key)
            .map(|sorted_set| sorted_set.zrange(start, end))
    }

    pub async fn zscore(&self, key: &str, member: &str) -> Option<f64> {
        let sorted_set_map = self.sorted_set.read().await;
        sorted_set_map
            .get(key)
            .and_then(|sorted_set| sorted_set.zscore(member))
    }
}
