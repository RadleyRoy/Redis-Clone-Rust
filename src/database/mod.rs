use std::sync::Arc;
use std::collections::HashMap;

use tokio::sync::RwLock;
use tokio::time::{ Instant, Duration }; 

#[derive(Clone)]
pub struct Database
{
    db: Arc<RwLock<HashMap<String, String>>>,
    expiry: Arc<RwLock<HashMap<String, Instant>>>,
}

impl Database
{
    pub fn new() -> Self
    {
        Database {
            db: Arc::new(RwLock::new(HashMap::new())),
            expiry: Arc::new(RwLock::new(HashMap::new())),
        }
    }

    pub async fn set(&self, key: String, value: String, ttl: Option<u64>)
    {
        let mut db = self.db.write().await;
        db.insert(key.clone(), value);
        
        if let Some(seconds) = ttl
        {
            let expiry_time = Instant::now() + Duration::from_secs(seconds);
            let mut expiry_map = self.expiry.write().await;
            expiry_map.insert(key, expiry_time);
        }
    }

    pub async fn get(&self, key: &str) -> Option<String>
    {
        if self.is_expired(key).await
        {
            self.delete(key).await;
            return None;
        }
        let db = self.db.read().await;
        db.get(key).cloned()
    }

    pub async fn is_expired(&self, key: &str) -> bool
    {
        let expiry_map = self.expiry.read().await;
        if let Some(&expiry_time) = expiry_map.get(key)
        {
            return Instant::now() > expiry_time;
        }
        false
    }

    pub async fn delete(&self, key: &str) -> bool
    {
        let mut db = self.db.write().await;
        let mut expiry = self.expiry.write().await;
        db.remove(key);
        expiry.remove(key).is_some()
    }
}