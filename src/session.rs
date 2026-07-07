//! Per-connection state and the command-handling entry point.
//!
//! Most commands are stateless and go straight to [`command::dispatch`], but a
//! few need to remember something about *this* connection between requests:
//!
//!   * **Transactions** (`MULTI`/`EXEC`/`DISCARD`) queue commands and run them
//!     together.
//!   * **Pub/Sub** (`SUBSCRIBE`/`UNSUBSCRIBE`) puts the connection into a mode
//!     where it also receives messages pushed from other connections.
//!
//! Those live here, on the [`Session`]; the connection loop in
//! [`server`](crate::server) owns the socket and the mailbox receiver.

use std::collections::HashSet;

use tokio::sync::mpsc::{UnboundedReceiver, UnboundedSender, unbounded_channel};

use crate::command::{self, Command};
use crate::database::db::Database;
use crate::resp;

/// A transaction opened with `MULTI`: the queued commands plus a flag recording
/// whether any of them failed to parse (which makes `EXEC` abort).
struct Transaction {
    commands: Vec<Vec<String>>,
    dirty: bool,
}

/// Everything a single connection needs to remember between requests.
pub struct Session {
    db: Database,
    id: u64,
    mailbox: UnboundedSender<String>,
    channels: HashSet<String>,
    transaction: Option<Transaction>,
}

impl Session {
    /// Creates a session for `db`, returning it with the receiving end of its
    /// pub/sub mailbox (drained by the connection loop and written to the
    /// socket).
    pub fn new(db: Database) -> (Self, UnboundedReceiver<String>) {
        let (mailbox, receiver) = unbounded_channel();
        let id = db.next_subscriber_id();
        let session = Self {
            db,
            id,
            mailbox,
            channels: HashSet::new(),
            transaction: None,
        };
        (session, receiver)
    }

    /// Whether the connection is currently in subscribe mode.
    pub fn is_subscribed(&self) -> bool {
        !self.channels.is_empty()
    }

    /// Handles one request, returning the RESP reply (which may be empty, e.g.
    /// for a blank line).
    pub async fn handle(&mut self, tokens: &[String]) -> String {
        let Some(name) = tokens.first() else {
            return String::new();
        };
        let name = name.to_ascii_uppercase();
        match name.as_str() {
            "MULTI" => return self.begin_multi(),
            "EXEC" => return self.exec().await,
            "DISCARD" => return self.discard(),
            "SUBSCRIBE" => return self.subscribe(&tokens[1..]),
            "UNSUBSCRIBE" => return self.unsubscribe(&tokens[1..]),
            // While subscribed, RESP2 only permits a handful of commands.
            other if self.is_subscribed() && other != "PING" => {
                return resp::error(&format!(
                    "ERR Can't execute '{}': only (UN)SUBSCRIBE / PING are allowed in subscribe mode",
                    other.to_ascii_lowercase()
                ));
            }
            _ => {}
        }

        // Inside a transaction, validate and queue; otherwise run immediately.
        match self.transaction.as_mut() {
            Some(transaction) => match Command::parse(tokens) {
                Ok(_) => {
                    transaction.commands.push(tokens.to_vec());
                    resp::simple_string("QUEUED")
                }
                Err(error) => {
                    transaction.dirty = true;
                    resp::error(&error.to_string())
                }
            },
            None => command::dispatch(tokens, &self.db).await,
        }
    }

    /// Unsubscribes from everything still open; called when the connection ends.
    pub fn cleanup(&self) {
        for channel in &self.channels {
            self.db.unsubscribe(channel, self.id);
        }
    }

    // --- Transactions ---------------------------------------------------

    fn begin_multi(&mut self) -> String {
        if self.transaction.is_some() {
            return resp::error("ERR MULTI calls can not be nested");
        }
        self.transaction = Some(Transaction {
            commands: Vec::new(),
            dirty: false,
        });
        resp::simple_string("OK")
    }

    fn discard(&mut self) -> String {
        match self.transaction.take() {
            Some(_) => resp::simple_string("OK"),
            None => resp::error("ERR DISCARD without MULTI"),
        }
    }

    async fn exec(&mut self) -> String {
        let transaction = match self.transaction.take() {
            Some(transaction) => transaction,
            None => return resp::error("ERR EXEC without MULTI"),
        };
        if transaction.dirty {
            return resp::error("EXECABORT Transaction discarded because of previous errors.");
        }
        // The reply is an array of each queued command's raw reply.
        let mut reply = format!("*{}\r\n", transaction.commands.len());
        for command in &transaction.commands {
            reply.push_str(&command::dispatch(command, &self.db).await);
        }
        reply
    }

    // --- Pub/Sub --------------------------------------------------------

    fn subscribe(&mut self, channels: &[String]) -> String {
        if channels.is_empty() {
            return resp::error("ERR wrong number of arguments for 'subscribe' command");
        }
        let mut reply = String::new();
        for channel in channels {
            if self.channels.insert(channel.clone()) {
                self.db
                    .subscribe(channel.clone(), self.id, self.mailbox.clone());
            }
            reply.push_str(&subscription_reply(
                "subscribe",
                channel,
                self.channels.len(),
            ));
        }
        reply
    }

    fn unsubscribe(&mut self, channels: &[String]) -> String {
        let targets: Vec<String> = if channels.is_empty() {
            self.channels.iter().cloned().collect()
        } else {
            channels.to_vec()
        };
        if targets.is_empty() {
            return subscription_reply_null("unsubscribe");
        }
        let mut reply = String::new();
        for channel in targets {
            if self.channels.remove(&channel) {
                self.db.unsubscribe(&channel, self.id);
            }
            reply.push_str(&subscription_reply(
                "unsubscribe",
                &channel,
                self.channels.len(),
            ));
        }
        reply
    }
}

/// A `[kind, channel, count]` confirmation: two bulk strings and an integer.
fn subscription_reply(kind: &str, channel: &str, count: usize) -> String {
    format!(
        "*3\r\n{}{}{}",
        resp::bulk_string(kind),
        resp::bulk_string(channel),
        resp::integer(count as i64)
    )
}

/// The `UNSUBSCRIBE` confirmation when nothing was subscribed: a null channel
/// and a zero count.
fn subscription_reply_null(kind: &str) -> String {
    format!(
        "*3\r\n{}{}{}",
        resp::bulk_string(kind),
        resp::null(),
        resp::integer(0)
    )
}
