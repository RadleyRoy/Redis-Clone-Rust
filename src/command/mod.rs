//! Command handling, split into three clear stages:
//!
//! 1. [`Command::parse`] turns a raw inline request into a typed [`Command`],
//!    reporting a protocol error for anything malformed.
//! 2. [`execute`] runs a parsed command against the [`Database`].
//! 3. [`resp`](crate::resp) turns the result into a RESP reply.
//!
//! Splitting parsing from execution keeps each stage small and independently
//! testable, and means invalid client input produces an error reply instead of
//! panicking a connection task.

use std::str::FromStr;

use crate::database::db::Database;
use crate::resp;

/// A fully parsed, validated client command.
pub enum Command {
    Set {
        key: String,
        value: String,
        ttl: Option<u64>,
    },
    Get {
        key: String,
    },
    Del {
        key: String,
    },
    LPush {
        key: String,
        value: String,
    },
    RPush {
        key: String,
        value: String,
    },
    LPop {
        key: String,
    },
    RPop {
        key: String,
    },
    LRange {
        key: String,
        start: i64,
        stop: i64,
    },
    SAdd {
        key: String,
        value: String,
    },
    SRem {
        key: String,
        value: String,
    },
    SMembers {
        key: String,
    },
    SIsMember {
        key: String,
        value: String,
    },
    ZAdd {
        key: String,
        member: String,
        score: f64,
    },
    ZRem {
        key: String,
        member: String,
    },
    ZRange {
        key: String,
        start: i64,
        stop: i64,
    },
    ZScore {
        key: String,
        member: String,
    },
}

impl Command {
    /// Parses one inline command line. Command names are case-insensitive.
    /// Returns an `ERR ...` message (without the RESP framing) on failure.
    pub fn parse(input: &str) -> Result<Command, String> {
        let mut parts = input.split_whitespace();
        let name = parts.next().ok_or("ERR empty command")?;
        let args: Vec<&str> = parts.collect();

        let command = match name.to_ascii_uppercase().as_str() {
            "SET" => match args.as_slice() {
                [key, value] => Command::Set {
                    key: key.to_string(),
                    value: value.to_string(),
                    ttl: None,
                },
                [key, value, option, ttl]
                    if option.eq_ignore_ascii_case("EX") || option.eq_ignore_ascii_case("EXP") =>
                {
                    Command::Set {
                        key: key.to_string(),
                        value: value.to_string(),
                        ttl: Some(parse_number(ttl, "integer")?),
                    }
                }
                _ => return Err(arity("SET")),
            },
            "GET" => Command::Get {
                key: single_key(&args, "GET")?,
            },
            "DEL" => Command::Del {
                key: single_key(&args, "DEL")?,
            },
            "LPUSH" => {
                let (key, value) = key_value(&args, "LPUSH")?;
                Command::LPush { key, value }
            }
            "RPUSH" => {
                let (key, value) = key_value(&args, "RPUSH")?;
                Command::RPush { key, value }
            }
            "LPOP" => Command::LPop {
                key: single_key(&args, "LPOP")?,
            },
            "RPOP" => Command::RPop {
                key: single_key(&args, "RPOP")?,
            },
            "LRANGE" => match args.as_slice() {
                [key, start, stop] => Command::LRange {
                    key: key.to_string(),
                    start: parse_number(start, "integer")?,
                    stop: parse_number(stop, "integer")?,
                },
                _ => return Err(arity("LRANGE")),
            },
            "SADD" => {
                let (key, value) = key_value(&args, "SADD")?;
                Command::SAdd { key, value }
            }
            "SREM" => {
                let (key, value) = key_value(&args, "SREM")?;
                Command::SRem { key, value }
            }
            "SMEMBERS" => Command::SMembers {
                key: single_key(&args, "SMEMBERS")?,
            },
            "SISMEMBER" => {
                let (key, value) = key_value(&args, "SISMEMBER")?;
                Command::SIsMember { key, value }
            }
            "ZADD" => match args.as_slice() {
                [key, score, member] => Command::ZAdd {
                    key: key.to_string(),
                    member: member.to_string(),
                    score: parse_number(score, "float")?,
                },
                _ => return Err(arity("ZADD")),
            },
            "ZREM" => {
                let (key, member) = key_value(&args, "ZREM")?;
                Command::ZRem { key, member }
            }
            "ZRANGE" => match args.as_slice() {
                [key, start, stop] => Command::ZRange {
                    key: key.to_string(),
                    start: parse_number(start, "integer")?,
                    stop: parse_number(stop, "integer")?,
                },
                _ => return Err(arity("ZRANGE")),
            },
            "ZSCORE" => {
                let (key, member) = key_value(&args, "ZSCORE")?;
                Command::ZScore { key, member }
            }
            _ => return Err(format!("ERR unknown command '{name}'")),
        };
        Ok(command)
    }
}

/// Parses and executes one inline request, returning the RESP reply to send
/// back. A blank line produces no reply (an empty string).
pub async fn handle(input: &str, db: &Database) -> String {
    let input = input.trim();
    if input.is_empty() {
        return String::new();
    }
    match Command::parse(input) {
        Ok(command) => execute(command, db).await,
        Err(message) => resp::error(&message),
    }
}

/// Runs a parsed command and encodes its outcome as a RESP reply.
async fn execute(command: Command, db: &Database) -> String {
    match command {
        Command::Set { key, value, ttl } => {
            db.set(key, value, ttl).await;
            resp::simple_string("OK")
        }
        Command::Get { key } => reply_optional(db.get(&key).await),
        Command::Del { key } => resp::integer(db.delete(&key).await as i64),
        Command::LPush { key, value } => reply_count(db.lpush(key, value).await),
        Command::RPush { key, value } => reply_count(db.rpush(key, value).await),
        Command::LPop { key } => reply_optional(db.lpop(&key).await),
        Command::RPop { key } => reply_optional(db.rpop(&key).await),
        Command::LRange { key, start, stop } => reply_array(db.lrange(&key, start, stop).await),
        Command::SAdd { key, value } => reply_bool(db.sadd(key, value).await),
        Command::SRem { key, value } => reply_bool(db.srem(&key, &value).await),
        Command::SMembers { key } => reply_array(db.smembers(&key).await),
        Command::SIsMember { key, value } => reply_bool(db.sismember(&key, &value).await),
        Command::ZAdd { key, member, score } => reply_bool(db.zadd(key, member, score).await),
        Command::ZRem { key, member } => reply_bool(db.zrem(&key, &member).await),
        Command::ZRange { key, start, stop } => reply_array(db.zrange(&key, start, stop).await),
        Command::ZScore { key, member } => match db.zscore(&key, &member).await {
            Ok(Some(score)) => resp::bulk_string(&format_score(score)),
            Ok(None) => resp::null(),
            Err(message) => resp::error(&message),
        },
    }
}

// --- Reply encoding helpers -------------------------------------------------

fn reply_optional(result: Result<Option<String>, String>) -> String {
    match result {
        Ok(Some(value)) => resp::bulk_string(&value),
        Ok(None) => resp::null(),
        Err(message) => resp::error(&message),
    }
}

fn reply_array(result: Result<Vec<String>, String>) -> String {
    match result {
        Ok(values) => resp::array(&values),
        Err(message) => resp::error(&message),
    }
}

fn reply_bool(result: Result<bool, String>) -> String {
    match result {
        Ok(value) => resp::integer(value as i64),
        Err(message) => resp::error(&message),
    }
}

fn reply_count(result: Result<usize, String>) -> String {
    match result {
        Ok(count) => resp::integer(count as i64),
        Err(message) => resp::error(&message),
    }
}

/// Formats a sorted-set score the way Redis does: integral scores print
/// without a decimal point (`3`, not `3.0`).
fn format_score(score: f64) -> String {
    score.to_string()
}

// --- Argument parsing helpers -----------------------------------------------

fn arity(command: &str) -> String {
    format!(
        "ERR wrong number of arguments for '{}' command",
        command.to_ascii_lowercase()
    )
}

fn single_key(args: &[&str], command: &str) -> Result<String, String> {
    match args {
        [key] => Ok(key.to_string()),
        _ => Err(arity(command)),
    }
}

fn key_value(args: &[&str], command: &str) -> Result<(String, String), String> {
    match args {
        [key, value] => Ok((key.to_string(), value.to_string())),
        _ => Err(arity(command)),
    }
}

fn parse_number<T: FromStr>(raw: &str, kind: &str) -> Result<T, String> {
    raw.parse::<T>()
        .map_err(|_| format!("ERR value '{raw}' is not a valid {kind}"))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn set_and_get_roundtrip() {
        let db = Database::new();
        assert_eq!(handle("SET name radley", &db).await, "+OK\r\n");
        assert_eq!(handle("GET name", &db).await, "$6\r\nradley\r\n");
        assert_eq!(handle("GET missing", &db).await, "$-1\r\n");
    }

    #[tokio::test]
    async fn command_names_are_case_insensitive() {
        let db = Database::new();
        assert_eq!(handle("set k v", &db).await, "+OK\r\n");
        assert_eq!(handle("Get k", &db).await, "$1\r\nv\r\n");
    }

    #[tokio::test]
    async fn del_returns_an_integer_reply() {
        let db = Database::new();
        handle("SET k v", &db).await;
        assert_eq!(handle("DEL k", &db).await, ":1\r\n");
        assert_eq!(handle("DEL k", &db).await, ":0\r\n");
    }

    #[tokio::test]
    async fn lrange_uses_redis_argument_order() {
        let db = Database::new();
        for value in ["a", "b", "c"] {
            handle(&format!("RPUSH l {value}"), &db).await;
        }
        assert_eq!(
            handle("LRANGE l 0 -1", &db).await,
            "*3\r\n$1\r\na\r\n$1\r\nb\r\n$1\r\nc\r\n"
        );
    }

    #[tokio::test]
    async fn wrong_type_produces_an_error_reply() {
        let db = Database::new();
        handle("LPUSH l a", &db).await;
        assert!(handle("GET l", &db).await.starts_with("-WRONGTYPE"));
    }

    #[tokio::test]
    async fn malformed_input_never_panics() {
        let db = Database::new();
        assert!(handle("SET k v EX abc", &db).await.starts_with("-ERR"));
        assert!(handle("LRANGE l x y", &db).await.starts_with("-ERR"));
        assert_eq!(
            handle("FOO bar", &db).await,
            "-ERR unknown command 'FOO'\r\n"
        );
        assert_eq!(handle("GET", &db).await, arity_reply("GET"));
        assert_eq!(handle("", &db).await, "");
    }

    fn arity_reply(command: &str) -> String {
        resp::error(&arity(command))
    }
}
