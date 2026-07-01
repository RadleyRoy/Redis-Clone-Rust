//! Command handling, split into three clear stages:
//!
//! 1. [`Command::parse`] turns the already-tokenized request into a typed
//!    [`Command`], reporting a [`CommandError`] for anything malformed.
//! 2. [`execute`] runs a parsed command against the [`Database`].
//! 3. [`resp`](crate::resp) turns the result into a RESP reply.
//!
//! Parsing takes a slice of tokens rather than a raw line, so the same code
//! serves both the inline protocol and RESP array requests — the server does
//! the tokenizing and this module never cares which framing was used.

use std::fmt;
use std::str::FromStr;

use crate::database::db::{Database, StoreError};
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

/// Everything that can go wrong while parsing a request. Rendering happens via
/// [`fmt::Display`], which produces the exact text of the RESP error (without
/// the leading `-` or trailing CRLF that [`resp::error`] adds).
#[derive(Debug, PartialEq, Eq)]
pub enum CommandError {
    Empty,
    Unknown(String),
    WrongArity(String),
    NotAnInteger(String),
    NotAFloat(String),
}

impl fmt::Display for CommandError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            CommandError::Empty => f.write_str("ERR empty command"),
            CommandError::Unknown(name) => write!(f, "ERR unknown command '{name}'"),
            CommandError::WrongArity(command) => write!(
                f,
                "ERR wrong number of arguments for '{}' command",
                command.to_ascii_lowercase()
            ),
            CommandError::NotAnInteger(value) => {
                write!(f, "ERR value '{value}' is not a valid integer")
            }
            CommandError::NotAFloat(value) => {
                write!(f, "ERR value '{value}' is not a valid float")
            }
        }
    }
}

impl Command {
    /// Parses a tokenized request (command name followed by its arguments).
    /// Command names are case-insensitive.
    pub fn parse(tokens: &[String]) -> Result<Command, CommandError> {
        let (name, args) = tokens.split_first().ok_or(CommandError::Empty)?;

        let command = match name.to_ascii_uppercase().as_str() {
            "SET" => match args {
                [key, value] => Command::Set {
                    key: key.clone(),
                    value: value.clone(),
                    ttl: None,
                },
                [key, value, option, ttl]
                    if option.eq_ignore_ascii_case("EX") || option.eq_ignore_ascii_case("EXP") =>
                {
                    Command::Set {
                        key: key.clone(),
                        value: value.clone(),
                        ttl: Some(parse_number(ttl, CommandError::NotAnInteger)?),
                    }
                }
                _ => return Err(arity("SET")),
            },
            "GET" => Command::Get {
                key: single_key(args, "GET")?,
            },
            "DEL" => Command::Del {
                key: single_key(args, "DEL")?,
            },
            "LPUSH" => {
                let (key, value) = key_value(args, "LPUSH")?;
                Command::LPush { key, value }
            }
            "RPUSH" => {
                let (key, value) = key_value(args, "RPUSH")?;
                Command::RPush { key, value }
            }
            "LPOP" => Command::LPop {
                key: single_key(args, "LPOP")?,
            },
            "RPOP" => Command::RPop {
                key: single_key(args, "RPOP")?,
            },
            "LRANGE" => match args {
                [key, start, stop] => Command::LRange {
                    key: key.clone(),
                    start: parse_number(start, CommandError::NotAnInteger)?,
                    stop: parse_number(stop, CommandError::NotAnInteger)?,
                },
                _ => return Err(arity("LRANGE")),
            },
            "SADD" => {
                let (key, value) = key_value(args, "SADD")?;
                Command::SAdd { key, value }
            }
            "SREM" => {
                let (key, value) = key_value(args, "SREM")?;
                Command::SRem { key, value }
            }
            "SMEMBERS" => Command::SMembers {
                key: single_key(args, "SMEMBERS")?,
            },
            "SISMEMBER" => {
                let (key, value) = key_value(args, "SISMEMBER")?;
                Command::SIsMember { key, value }
            }
            "ZADD" => match args {
                [key, score, member] => Command::ZAdd {
                    key: key.clone(),
                    member: member.clone(),
                    score: parse_number(score, CommandError::NotAFloat)?,
                },
                _ => return Err(arity("ZADD")),
            },
            "ZREM" => {
                let (key, member) = key_value(args, "ZREM")?;
                Command::ZRem { key, member }
            }
            "ZRANGE" => match args {
                [key, start, stop] => Command::ZRange {
                    key: key.clone(),
                    start: parse_number(start, CommandError::NotAnInteger)?,
                    stop: parse_number(stop, CommandError::NotAnInteger)?,
                },
                _ => return Err(arity("ZRANGE")),
            },
            "ZSCORE" => {
                let (key, member) = key_value(args, "ZSCORE")?;
                Command::ZScore { key, member }
            }
            _ => return Err(CommandError::Unknown(name.clone())),
        };
        Ok(command)
    }
}

/// Parses a tokenized request and executes it, returning the RESP reply. An
/// empty token list (a blank line) produces no reply.
pub async fn dispatch(tokens: &[String], db: &Database) -> String {
    if tokens.is_empty() {
        return String::new();
    }
    match Command::parse(tokens) {
        Ok(command) => execute(command, db).await,
        Err(error) => resp::error(&error.to_string()),
    }
}

/// Convenience wrapper for inline input: splits on whitespace and dispatches.
/// Only used by tests; the server calls [`dispatch`] with already-read tokens.
#[cfg(test)]
async fn handle(input: &str, db: &Database) -> String {
    let tokens: Vec<String> = input.split_whitespace().map(str::to_string).collect();
    dispatch(&tokens, db).await
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
            Err(error) => resp::error(&error.to_string()),
        },
    }
}

// --- Reply encoding helpers -------------------------------------------------

fn reply_optional(result: Result<Option<String>, StoreError>) -> String {
    match result {
        Ok(Some(value)) => resp::bulk_string(&value),
        Ok(None) => resp::null(),
        Err(error) => resp::error(&error.to_string()),
    }
}

fn reply_array(result: Result<Vec<String>, StoreError>) -> String {
    match result {
        Ok(values) => resp::array(&values),
        Err(error) => resp::error(&error.to_string()),
    }
}

fn reply_bool(result: Result<bool, StoreError>) -> String {
    match result {
        Ok(value) => resp::integer(value as i64),
        Err(error) => resp::error(&error.to_string()),
    }
}

fn reply_count(result: Result<usize, StoreError>) -> String {
    match result {
        Ok(count) => resp::integer(count as i64),
        Err(error) => resp::error(&error.to_string()),
    }
}

/// Formats a sorted-set score the way Redis does: integral scores print
/// without a decimal point (`3`, not `3.0`).
fn format_score(score: f64) -> String {
    score.to_string()
}

// --- Argument parsing helpers -----------------------------------------------

fn arity(command: &str) -> CommandError {
    CommandError::WrongArity(command.to_string())
}

fn single_key(args: &[String], command: &str) -> Result<String, CommandError> {
    match args {
        [key] => Ok(key.clone()),
        _ => Err(arity(command)),
    }
}

fn key_value(args: &[String], command: &str) -> Result<(String, String), CommandError> {
    match args {
        [key, value] => Ok((key.clone(), value.clone())),
        _ => Err(arity(command)),
    }
}

fn parse_number<T: FromStr>(
    raw: &str,
    to_error: fn(String) -> CommandError,
) -> Result<T, CommandError> {
    raw.parse::<T>().map_err(|_| to_error(raw.to_string()))
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
        assert_eq!(
            handle("GET", &db).await,
            resp::error(&arity("GET").to_string())
        );
        assert_eq!(handle("", &db).await, "");
    }

    #[test]
    fn command_error_messages_match_redis() {
        assert_eq!(
            CommandError::Unknown("X".into()).to_string(),
            "ERR unknown command 'X'"
        );
        assert_eq!(
            arity("SET").to_string(),
            "ERR wrong number of arguments for 'set' command"
        );
        assert_eq!(
            CommandError::NotAnInteger("z".into()).to_string(),
            "ERR value 'z' is not a valid integer"
        );
    }
}
