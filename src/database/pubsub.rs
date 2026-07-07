//! The publish/subscribe registry: a map from channel name to the set of
//! subscribers currently listening on it.
//!
//! Each connection owns one unbounded mpsc channel — its "mailbox". Subscribing
//! registers that mailbox's sender under a channel name; publishing looks up the
//! channel and pushes the (pre-encoded) message into every registered mailbox.
//! The connection task drains its mailbox and writes the messages to its socket.

use std::collections::HashMap;
use std::sync::Mutex;
use std::sync::atomic::{AtomicU64, Ordering};

use tokio::sync::mpsc::UnboundedSender;

use crate::resp;

/// A shared registry of channel subscriptions. Guarded by a plain `Mutex`
/// because every operation is a quick map update with no `.await` held across
/// the lock.
#[derive(Default)]
pub struct PubSub {
    channels: Mutex<HashMap<String, HashMap<u64, UnboundedSender<String>>>>,
    next_id: AtomicU64,
}

impl PubSub {
    /// Hands out a process-unique subscriber id for a new connection.
    pub fn next_id(&self) -> u64 {
        self.next_id.fetch_add(1, Ordering::Relaxed)
    }

    /// Registers `sender` as a subscriber `id` on `channel`.
    pub fn subscribe(&self, channel: String, id: u64, sender: UnboundedSender<String>) {
        self.channels
            .lock()
            .unwrap()
            .entry(channel)
            .or_default()
            .insert(id, sender);
    }

    /// Removes subscriber `id` from `channel`, dropping the channel entry once it
    /// has no subscribers left.
    pub fn unsubscribe(&self, channel: &str, id: u64) {
        let mut channels = self.channels.lock().unwrap();
        if let Some(subscribers) = channels.get_mut(channel) {
            subscribers.remove(&id);
            if subscribers.is_empty() {
                channels.remove(channel);
            }
        }
    }

    /// Delivers `payload` to every subscriber of `channel`, returning how many
    /// received it. The message is encoded once as the standard pub/sub reply
    /// `["message", channel, payload]`.
    pub fn publish(&self, channel: &str, payload: &str) -> usize {
        let channels = self.channels.lock().unwrap();
        let Some(subscribers) = channels.get(channel) else {
            return 0;
        };
        let message = resp::array(&[
            "message".to_string(),
            channel.to_string(),
            payload.to_string(),
        ]);
        subscribers
            .values()
            .filter(|sender| sender.send(message.clone()).is_ok())
            .count()
    }
}
