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

use tokio::spawn;
use tracing::warn;

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
        keys: Vec<String>,
    },
    LPush {
        key: String,
        values: Vec<String>,
    },
    RPush {
        key: String,
        values: Vec<String>,
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
        values: Vec<String>,
    },
    SRem {
        key: String,
        values: Vec<String>,
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
        members: Vec<(f64, String)>,
    },
    ZRem {
        key: String,
        member: String,
    },
    ZRange {
        key: String,
        start: i64,
        stop: i64,
        with_scores: bool,
    },
    ZScore {
        key: String,
        member: String,
    },
    HSet {
        key: String,
        field: String,
        value: String,
    },
    HGet {
        key: String,
        field: String,
    },
    HDel {
        key: String,
        field: String,
    },
    HGetAll {
        key: String,
    },
    HKeys {
        key: String,
    },
    HVals {
        key: String,
    },
    HLen {
        key: String,
    },
    HExists {
        key: String,
        field: String,
    },
    HIncrBy {
        key: String,
        field: String,
        delta: i64,
    },
    Exists {
        keys: Vec<String>,
    },
    Type {
        key: String,
    },
    Keys {
        pattern: String,
    },
    Expire {
        key: String,
        seconds: i64,
    },
    Ttl {
        key: String,
    },
    Persist {
        key: String,
    },
    Rename {
        key: String,
        new_key: String,
    },
    Ping {
        message: Option<String>,
    },
    Echo {
        message: String,
    },
    DbSize,
    FlushAll,
    Info,
    Save,
    BgSave,
    Publish {
        channel: String,
        message: String,
    },
    IncrBy {
        key: String,
        delta: i64,
    },
    Append {
        key: String,
        value: String,
    },
    StrLen {
        key: String,
    },
    MGet {
        keys: Vec<String>,
    },
    MSet {
        pairs: Vec<(String, String)>,
    },
    SetNx {
        key: String,
        value: String,
    },
    GetSet {
        key: String,
        value: String,
    },
    LLen {
        key: String,
    },
    LIndex {
        key: String,
        index: i64,
    },
    LSet {
        key: String,
        index: i64,
        value: String,
    },
    SCard {
        key: String,
    },
    SPop {
        key: String,
    },
    SInter {
        keys: Vec<String>,
    },
    SUnion {
        keys: Vec<String>,
    },
    SDiff {
        keys: Vec<String>,
    },
    ZCard {
        key: String,
    },
    ZRank {
        key: String,
        member: String,
    },
    ZIncrBy {
        key: String,
        increment: f64,
        member: String,
    },
    ZRangeByScore {
        key: String,
        min: f64,
        min_inclusive: bool,
        max: f64,
        max_inclusive: bool,
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
                keys: non_empty_args(args, "DEL")?,
            },
            "LPUSH" => {
                let (key, values) = key_and_values(args, "LPUSH")?;
                Command::LPush { key, values }
            }
            "RPUSH" => {
                let (key, values) = key_and_values(args, "RPUSH")?;
                Command::RPush { key, values }
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
                let (key, values) = key_and_values(args, "SADD")?;
                Command::SAdd { key, values }
            }
            "SREM" => {
                let (key, values) = key_and_values(args, "SREM")?;
                Command::SRem { key, values }
            }
            "SMEMBERS" => Command::SMembers {
                key: single_key(args, "SMEMBERS")?,
            },
            "SISMEMBER" => {
                let (key, value) = key_value(args, "SISMEMBER")?;
                Command::SIsMember { key, value }
            }
            "ZADD" => match args {
                [key, rest @ ..] if !rest.is_empty() && rest.len() % 2 == 0 => {
                    let mut members = Vec::with_capacity(rest.len() / 2);
                    for pair in rest.chunks_exact(2) {
                        let score = parse_score(&pair[0])?;
                        members.push((score, pair[1].clone()));
                    }
                    Command::ZAdd {
                        key: key.clone(),
                        members,
                    }
                }
                _ => return Err(arity("ZADD")),
            },
            "ZREM" => {
                let (key, member) = key_value(args, "ZREM")?;
                Command::ZRem { key, member }
            }
            "ZRANGE" => match args {
                [key, start, stop] | [key, start, stop, _]
                    if args.len() == 3 || args[3].eq_ignore_ascii_case("WITHSCORES") =>
                {
                    Command::ZRange {
                        key: key.clone(),
                        start: parse_number(start, CommandError::NotAnInteger)?,
                        stop: parse_number(stop, CommandError::NotAnInteger)?,
                        with_scores: args.len() == 4,
                    }
                }
                _ => return Err(arity("ZRANGE")),
            },
            "ZSCORE" => {
                let (key, member) = key_value(args, "ZSCORE")?;
                Command::ZScore { key, member }
            }
            "HSET" => match args {
                [key, field, value] => Command::HSet {
                    key: key.clone(),
                    field: field.clone(),
                    value: value.clone(),
                },
                _ => return Err(arity("HSET")),
            },
            "HGET" => {
                let (key, field) = key_value(args, "HGET")?;
                Command::HGet { key, field }
            }
            "HDEL" => {
                let (key, field) = key_value(args, "HDEL")?;
                Command::HDel { key, field }
            }
            "HGETALL" => Command::HGetAll {
                key: single_key(args, "HGETALL")?,
            },
            "HKEYS" => Command::HKeys {
                key: single_key(args, "HKEYS")?,
            },
            "HVALS" => Command::HVals {
                key: single_key(args, "HVALS")?,
            },
            "HLEN" => Command::HLen {
                key: single_key(args, "HLEN")?,
            },
            "HEXISTS" => {
                let (key, field) = key_value(args, "HEXISTS")?;
                Command::HExists { key, field }
            }
            "HINCRBY" => match args {
                [key, field, delta] => Command::HIncrBy {
                    key: key.clone(),
                    field: field.clone(),
                    delta: parse_number(delta, CommandError::NotAnInteger)?,
                },
                _ => return Err(arity("HINCRBY")),
            },
            "EXISTS" => Command::Exists {
                keys: non_empty_args(args, "EXISTS")?,
            },
            "TYPE" => Command::Type {
                key: single_key(args, "TYPE")?,
            },
            "KEYS" => Command::Keys {
                pattern: single_key(args, "KEYS")?,
            },
            "EXPIRE" => match args {
                [key, seconds] => Command::Expire {
                    key: key.clone(),
                    seconds: parse_number(seconds, CommandError::NotAnInteger)?,
                },
                _ => return Err(arity("EXPIRE")),
            },
            "TTL" => Command::Ttl {
                key: single_key(args, "TTL")?,
            },
            "PERSIST" => Command::Persist {
                key: single_key(args, "PERSIST")?,
            },
            "RENAME" => {
                let (key, new_key) = key_value(args, "RENAME")?;
                Command::Rename { key, new_key }
            }
            "PING" => match args {
                [] => Command::Ping { message: None },
                [message] => Command::Ping {
                    message: Some(message.clone()),
                },
                _ => return Err(arity("PING")),
            },
            "ECHO" => Command::Echo {
                message: single_key(args, "ECHO")?,
            },
            "DBSIZE" => match args {
                [] => Command::DbSize,
                _ => return Err(arity("DBSIZE")),
            },
            "FLUSHALL" => match args {
                [] => Command::FlushAll,
                _ => return Err(arity("FLUSHALL")),
            },
            // `INFO [section]` — the optional section argument is accepted and
            // ignored; the full block is always returned.
            "INFO" => Command::Info,
            "SAVE" => Command::Save,
            "BGSAVE" => Command::BgSave,
            "PUBLISH" => {
                let (channel, message) = key_value(args, "PUBLISH")?;
                Command::Publish { channel, message }
            }
            "INCR" => Command::IncrBy {
                key: single_key(args, "INCR")?,
                delta: 1,
            },
            "DECR" => Command::IncrBy {
                key: single_key(args, "DECR")?,
                delta: -1,
            },
            "INCRBY" => match args {
                [key, delta] => Command::IncrBy {
                    key: key.clone(),
                    delta: parse_number(delta, CommandError::NotAnInteger)?,
                },
                _ => return Err(arity("INCRBY")),
            },
            "DECRBY" => match args {
                [key, delta] => Command::IncrBy {
                    key: key.clone(),
                    delta: negate(parse_number(delta, CommandError::NotAnInteger)?)?,
                },
                _ => return Err(arity("DECRBY")),
            },
            "APPEND" => {
                let (key, value) = key_value(args, "APPEND")?;
                Command::Append { key, value }
            }
            "STRLEN" => Command::StrLen {
                key: single_key(args, "STRLEN")?,
            },
            "MGET" => Command::MGet {
                keys: non_empty_args(args, "MGET")?,
            },
            "MSET" => {
                if args.is_empty() || args.len() % 2 != 0 {
                    return Err(arity("MSET"));
                }
                Command::MSet {
                    pairs: args
                        .chunks_exact(2)
                        .map(|pair| (pair[0].clone(), pair[1].clone()))
                        .collect(),
                }
            }
            "SETNX" => {
                let (key, value) = key_value(args, "SETNX")?;
                Command::SetNx { key, value }
            }
            "GETSET" => {
                let (key, value) = key_value(args, "GETSET")?;
                Command::GetSet { key, value }
            }
            "LLEN" => Command::LLen {
                key: single_key(args, "LLEN")?,
            },
            "LINDEX" => match args {
                [key, index] => Command::LIndex {
                    key: key.clone(),
                    index: parse_number(index, CommandError::NotAnInteger)?,
                },
                _ => return Err(arity("LINDEX")),
            },
            "LSET" => match args {
                [key, index, value] => Command::LSet {
                    key: key.clone(),
                    index: parse_number(index, CommandError::NotAnInteger)?,
                    value: value.clone(),
                },
                _ => return Err(arity("LSET")),
            },
            "SCARD" => Command::SCard {
                key: single_key(args, "SCARD")?,
            },
            "SPOP" => Command::SPop {
                key: single_key(args, "SPOP")?,
            },
            "SINTER" => Command::SInter {
                keys: non_empty_args(args, "SINTER")?,
            },
            "SUNION" => Command::SUnion {
                keys: non_empty_args(args, "SUNION")?,
            },
            "SDIFF" => Command::SDiff {
                keys: non_empty_args(args, "SDIFF")?,
            },
            "ZCARD" => Command::ZCard {
                key: single_key(args, "ZCARD")?,
            },
            "ZRANK" => {
                let (key, member) = key_value(args, "ZRANK")?;
                Command::ZRank { key, member }
            }
            "ZINCRBY" => match args {
                [key, increment, member] => Command::ZIncrBy {
                    key: key.clone(),
                    increment: parse_score(increment)?,
                    member: member.clone(),
                },
                _ => return Err(arity("ZINCRBY")),
            },
            "ZRANGEBYSCORE" => match args {
                [key, min, max] => {
                    let (min, min_inclusive) = parse_score_bound(min)?;
                    let (max, max_inclusive) = parse_score_bound(max)?;
                    Command::ZRangeByScore {
                        key: key.clone(),
                        min,
                        min_inclusive,
                        max,
                        max_inclusive,
                    }
                }
                _ => return Err(arity("ZRANGEBYSCORE")),
            },
            _ => return Err(CommandError::Unknown(name.clone())),
        };
        Ok(command)
    }

    /// Whether this command mutates the keyspace, and so should be appended to
    /// the AOF once it succeeds. `SAVE`/`BGSAVE` are excluded: they persist
    /// state without changing it.
    fn is_write(&self) -> bool {
        matches!(
            self,
            Command::Set { .. }
                | Command::Del { .. }
                | Command::LPush { .. }
                | Command::RPush { .. }
                | Command::LPop { .. }
                | Command::RPop { .. }
                | Command::SAdd { .. }
                | Command::SRem { .. }
                | Command::ZAdd { .. }
                | Command::ZRem { .. }
                | Command::HSet { .. }
                | Command::HDel { .. }
                | Command::HIncrBy { .. }
                | Command::Expire { .. }
                | Command::Persist { .. }
                | Command::Rename { .. }
                | Command::FlushAll
                | Command::IncrBy { .. }
                | Command::Append { .. }
                | Command::MSet { .. }
                | Command::SetNx { .. }
                | Command::GetSet { .. }
                | Command::LSet { .. }
                | Command::ZIncrBy { .. }
        )
    }
}

/// Parses a tokenized request and executes it, returning the RESP reply. An
/// empty token list (a blank line) produces no reply.
pub async fn dispatch(tokens: &[String], db: &Database) -> String {
    if tokens.is_empty() {
        return String::new();
    }
    match Command::parse(tokens) {
        Ok(command) => {
            // SPOP removes an *arbitrary* member, so logging it verbatim would
            // replay to a different member on restart, diverging from the
            // persisted state. Log the equivalent `SREM` of the member actually
            // removed instead — the same rewrite Redis applies.
            if let Command::SPop { key } = command {
                let popped = db.spop(&key).await;
                if let Ok(Some(member)) = &popped {
                    db.log_write(&[String::from("SREM"), key, member.clone()])
                        .await;
                }
                return reply_optional(popped);
            }
            let is_write = command.is_write();
            let reply = execute(command, db).await;
            // Append successful writes to the AOF; error replies start with '-'.
            if is_write && !reply.starts_with('-') {
                db.log_write(tokens).await;
            }
            reply
        }
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
        Command::Del { keys } => resp::integer(db.del(&keys).await as i64),
        Command::LPush { key, values } => reply_count(db.lpush(key, values).await),
        Command::RPush { key, values } => reply_count(db.rpush(key, values).await),
        Command::LPop { key } => reply_optional(db.lpop(&key).await),
        Command::RPop { key } => reply_optional(db.rpop(&key).await),
        Command::LRange { key, start, stop } => reply_array(db.lrange(&key, start, stop).await),
        Command::SAdd { key, values } => reply_count(db.sadd(key, values).await),
        Command::SRem { key, values } => reply_count(db.srem(&key, &values).await),
        Command::SMembers { key } => reply_array(db.smembers(&key).await),
        Command::SIsMember { key, value } => reply_bool(db.sismember(&key, &value).await),
        Command::ZAdd { key, members } => reply_count(db.zadd(key, members).await),
        Command::ZRem { key, member } => reply_bool(db.zrem(&key, &member).await),
        Command::ZRange {
            key,
            start,
            stop,
            with_scores,
        } => {
            if with_scores {
                match db.zrange_with_scores(&key, start, stop).await {
                    Ok(pairs) => {
                        let mut flat = Vec::with_capacity(pairs.len() * 2);
                        for (member, score) in pairs {
                            flat.push(member);
                            flat.push(format_score(score));
                        }
                        resp::array(&flat)
                    }
                    Err(error) => resp::error(&error.to_string()),
                }
            } else {
                reply_array(db.zrange(&key, start, stop).await)
            }
        }
        Command::ZScore { key, member } => match db.zscore(&key, &member).await {
            Ok(Some(score)) => resp::bulk_string(&format_score(score)),
            Ok(None) => resp::null(),
            Err(error) => resp::error(&error.to_string()),
        },
        Command::HSet { key, field, value } => reply_bool(db.hset(key, field, value).await),
        Command::HGet { key, field } => reply_optional(db.hget(&key, &field).await),
        Command::HDel { key, field } => reply_bool(db.hdel(&key, &field).await),
        Command::HGetAll { key } => reply_array(db.hgetall(&key).await),
        Command::HKeys { key } => reply_array(db.hkeys(&key).await),
        Command::HVals { key } => reply_array(db.hvals(&key).await),
        Command::HLen { key } => reply_count(db.hlen(&key).await),
        Command::HExists { key, field } => reply_bool(db.hexists(&key, &field).await),
        Command::HIncrBy { key, field, delta } => reply_signed(db.hincrby(key, field, delta).await),
        Command::Exists { keys } => resp::integer(db.exists(&keys).await as i64),
        Command::Type { key } => resp::simple_string(db.type_of(&key).await),
        Command::Keys { pattern } => resp::array(&db.keys(&pattern).await),
        Command::Expire { key, seconds } => resp::integer(db.expire(&key, seconds).await as i64),
        Command::Ttl { key } => resp::integer(db.ttl(&key).await),
        Command::Persist { key } => resp::integer(db.persist(&key).await as i64),
        Command::Rename { key, new_key } => {
            if db.rename(&key, new_key).await {
                resp::simple_string("OK")
            } else {
                resp::error("ERR no such key")
            }
        }
        Command::Ping { message } => match message {
            Some(message) => resp::bulk_string(&message),
            None => resp::simple_string("PONG"),
        },
        Command::Echo { message } => resp::bulk_string(&message),
        Command::DbSize => resp::integer(db.dbsize().await as i64),
        Command::FlushAll => {
            db.flushall().await;
            resp::simple_string("OK")
        }
        Command::Info => resp::bulk_string(&db.info().await),
        Command::Save => match db.save().await {
            Ok(()) => resp::simple_string("OK"),
            Err(cause) => resp::error(&format!("ERR {cause}")),
        },
        Command::BgSave => {
            let db = db.clone();
            spawn(async move {
                if let Err(cause) = db.save().await {
                    warn!(%cause, "background save failed");
                }
            });
            resp::simple_string("Background saving started")
        }
        Command::Publish { channel, message } => {
            resp::integer(db.publish(&channel, &message) as i64)
        }
        Command::IncrBy { key, delta } => reply_signed(db.incr_by(key, delta).await),
        Command::Append { key, value } => reply_count(db.append(key, &value).await),
        Command::StrLen { key } => reply_count(db.strlen(&key).await),
        Command::MGet { keys } => resp::nullable_array(&db.mget(&keys).await),
        Command::MSet { pairs } => {
            db.mset(pairs).await;
            resp::simple_string("OK")
        }
        Command::SetNx { key, value } => resp::integer(db.setnx(key, value).await as i64),
        Command::GetSet { key, value } => reply_optional(db.getset(key, value).await),
        Command::LLen { key } => reply_count(db.llen(&key).await),
        Command::LIndex { key, index } => reply_optional(db.lindex(&key, index).await),
        Command::LSet { key, index, value } => reply_status(db.lset(&key, index, value).await),
        Command::SCard { key } => reply_count(db.scard(&key).await),
        Command::SPop { key } => reply_optional(db.spop(&key).await),
        Command::SInter { keys } => reply_array(db.sinter(&keys).await),
        Command::SUnion { keys } => reply_array(db.sunion(&keys).await),
        Command::SDiff { keys } => reply_array(db.sdiff(&keys).await),
        Command::ZCard { key } => reply_count(db.zcard(&key).await),
        Command::ZRank { key, member } => match db.zrank(&key, &member).await {
            Ok(Some(rank)) => resp::integer(rank as i64),
            Ok(None) => resp::null(),
            Err(error) => resp::error(&error.to_string()),
        },
        Command::ZIncrBy {
            key,
            increment,
            member,
        } => match db.zincrby(key, increment, member).await {
            Ok(score) => resp::bulk_string(&format_score(score)),
            Err(error) => resp::error(&error.to_string()),
        },
        Command::ZRangeByScore {
            key,
            min,
            min_inclusive,
            max,
            max_inclusive,
        } => reply_array(
            db.zrange_by_score(&key, min, min_inclusive, max, max_inclusive)
                .await,
        ),
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

fn reply_signed(result: Result<i64, StoreError>) -> String {
    match result {
        Ok(value) => resp::integer(value),
        Err(error) => resp::error(&error.to_string()),
    }
}

fn reply_status(result: Result<(), StoreError>) -> String {
    match result {
        Ok(()) => resp::simple_string("OK"),
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

/// Requires at least one argument, returning them all. Used by variadic
/// commands such as `EXISTS`, `DEL` and `SINTER`.
fn non_empty_args(args: &[String], command: &str) -> Result<Vec<String>, CommandError> {
    if args.is_empty() {
        Err(arity(command))
    } else {
        Ok(args.to_vec())
    }
}

/// Splits `[key, value, value, ...]` into the key and its one-or-more values,
/// for variadic writers such as `LPUSH`, `RPUSH`, `SADD` and `SREM`.
fn key_and_values(args: &[String], command: &str) -> Result<(String, Vec<String>), CommandError> {
    match args {
        [key, values @ ..] if !values.is_empty() => Ok((key.clone(), values.to_vec())),
        _ => Err(arity(command)),
    }
}

fn parse_number<T: FromStr>(
    raw: &str,
    to_error: fn(String) -> CommandError,
) -> Result<T, CommandError> {
    raw.parse::<T>().map_err(|_| to_error(raw.to_string()))
}

/// Parses a sorted-set score. `f64::from_str` accepts `"nan"`, which Redis
/// rejects (a `NaN` score has no meaningful order), so it is refused here;
/// `inf`/`-inf` are allowed, as Redis allows.
fn parse_score(raw: &str) -> Result<f64, CommandError> {
    let score: f64 = raw
        .parse()
        .map_err(|_| CommandError::NotAFloat(raw.to_string()))?;
    if score.is_nan() {
        return Err(CommandError::NotAFloat(raw.to_string()));
    }
    Ok(score)
}

/// Negates a `DECRBY` amount, guarding against the one value (`i64::MIN`) whose
/// negation overflows.
fn negate(value: i64) -> Result<i64, CommandError> {
    value
        .checked_neg()
        .ok_or_else(|| CommandError::NotAnInteger(value.to_string()))
}

/// Parses a `ZRANGEBYSCORE` bound into `(score, inclusive)`. A leading `(`
/// marks an exclusive bound; `+inf`/`inf`/`-inf` are accepted.
fn parse_score_bound(raw: &str) -> Result<(f64, bool), CommandError> {
    let (inclusive, number) = match raw.strip_prefix('(') {
        Some(rest) => (false, rest),
        None => (true, raw),
    };
    let score = match number.to_ascii_lowercase().as_str() {
        "+inf" | "inf" => f64::INFINITY,
        "-inf" => f64::NEG_INFINITY,
        other => {
            let score: f64 = other
                .parse()
                .map_err(|_| CommandError::NotAFloat(raw.to_string()))?;
            if score.is_nan() {
                return Err(CommandError::NotAFloat(raw.to_string()));
            }
            score
        }
    };
    Ok((score, inclusive))
}

#[cfg(test)]
mod tests {
    use super::*;

    use crate::database::aof::{Aof, FsyncPolicy};

    /// A scratch file path that is removed when it goes out of scope, so AOF
    /// tests do not leave files behind or collide with one another.
    struct TempPath(std::path::PathBuf);

    impl TempPath {
        fn new(tag: &str) -> Self {
            let unique = std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos();
            let path = std::env::temp_dir().join(format!("redis_clone_test_{tag}_{unique}.aof"));
            Self(path)
        }
    }

    impl Drop for TempPath {
        fn drop(&mut self) {
            let _ = std::fs::remove_file(&self.0);
        }
    }

    #[tokio::test]
    async fn spop_is_logged_to_the_aof_as_a_deterministic_srem() {
        let temp = TempPath::new("spop");
        let aof = Aof::open(&temp.0, FsyncPolicy::Always).await.unwrap();
        let db = Database::with_persistence(None, Some(aof));

        handle("SADD s a b c", &db).await;
        let popped = handle("SPOP s", &db).await;
        // Reply is the popped member as a bulk string, e.g. "$1\r\na\r\n".
        let member = popped.lines().nth(1).unwrap().to_string();

        db.sync_aof().await.unwrap();
        let logged = std::fs::read_to_string(&temp.0).unwrap();
        // The AOF must record the equivalent SREM of the member actually
        // removed, never the non-deterministic SPOP.
        assert!(!logged.contains("SPOP"), "AOF logged raw SPOP: {logged:?}");
        assert!(logged.contains("SREM"), "AOF missing SREM: {logged:?}");
        assert!(logged.contains(&member), "AOF missing member: {logged:?}");
    }

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
    async fn hash_set_get_and_incr() {
        let db = Database::new();
        assert_eq!(handle("HSET h f 1", &db).await, ":1\r\n"); // new field
        assert_eq!(handle("HSET h f 2", &db).await, ":0\r\n"); // overwrite
        assert_eq!(handle("HGET h f", &db).await, "$1\r\n2\r\n");
        assert_eq!(handle("HGET h missing", &db).await, "$-1\r\n");
        assert_eq!(handle("HLEN h", &db).await, ":1\r\n");
        assert_eq!(handle("HINCRBY h f 5", &db).await, ":7\r\n");
        assert_eq!(handle("HEXISTS h f", &db).await, ":1\r\n");
        assert_eq!(handle("HDEL h f", &db).await, ":1\r\n");
        // Hash is now empty and dropped, so HGET returns nil, not WRONGTYPE.
        assert_eq!(handle("HGET h f", &db).await, "$-1\r\n");
    }

    #[tokio::test]
    async fn variadic_commands_count_correctly() {
        let db = Database::new();
        // RPUSH with several values reports the final length.
        assert_eq!(handle("RPUSH l a b c", &db).await, ":3\r\n");
        assert_eq!(
            handle("LRANGE l 0 -1", &db).await,
            "*3\r\n$1\r\na\r\n$1\r\nb\r\n$1\r\nc\r\n"
        );
        // SADD returns the number of *new* members added.
        assert_eq!(handle("SADD s a b c a", &db).await, ":3\r\n");
        assert_eq!(handle("SREM s a b x", &db).await, ":2\r\n");
        // ZADD returns the number of new members across score/member pairs.
        assert_eq!(handle("ZADD z 1 a 2 b 3 c", &db).await, ":3\r\n");
        // DEL reports how many of the listed keys existed.
        assert_eq!(handle("DEL l s z missing", &db).await, ":3\r\n");
    }

    #[tokio::test]
    async fn string_number_and_length_commands() {
        let db = Database::new();
        assert_eq!(handle("INCR counter", &db).await, ":1\r\n");
        assert_eq!(handle("INCRBY counter 9", &db).await, ":10\r\n");
        assert_eq!(handle("DECR counter", &db).await, ":9\r\n");
        assert_eq!(handle("DECRBY counter 4", &db).await, ":5\r\n");
        assert_eq!(handle("APPEND s ab", &db).await, ":2\r\n");
        assert_eq!(handle("APPEND s cd", &db).await, ":4\r\n");
        assert_eq!(handle("STRLEN s", &db).await, ":4\r\n");
        assert!(
            handle("INCR s", &db)
                .await
                .starts_with("-ERR value is not an integer")
        );
    }

    #[tokio::test]
    async fn mget_mset_setnx_getset() {
        let db = Database::new();
        assert_eq!(handle("MSET a 1 b 2", &db).await, "+OK\r\n");
        // MGET returns a value per key, nil for the missing one.
        assert_eq!(
            handle("MGET a missing b", &db).await,
            "*3\r\n$1\r\n1\r\n$-1\r\n$1\r\n2\r\n"
        );
        assert_eq!(handle("SETNX a 9", &db).await, ":0\r\n"); // already exists
        assert_eq!(handle("SETNX c 9", &db).await, ":1\r\n");
        assert_eq!(handle("GETSET a 5", &db).await, "$1\r\n1\r\n"); // old value
        assert_eq!(handle("GET a", &db).await, "$1\r\n5\r\n");
    }

    #[tokio::test]
    async fn list_index_and_set_operations() {
        let db = Database::new();
        for value in ["a", "b", "c"] {
            handle(&format!("RPUSH l {value}"), &db).await;
        }
        assert_eq!(handle("LLEN l", &db).await, ":3\r\n");
        assert_eq!(handle("LINDEX l -1", &db).await, "$1\r\nc\r\n");
        assert_eq!(handle("LSET l 0 z", &db).await, "+OK\r\n");
        assert_eq!(handle("LINDEX l 0", &db).await, "$1\r\nz\r\n");
        assert!(
            handle("LSET l 9 x", &db)
                .await
                .starts_with("-ERR index out of range")
        );
        assert!(
            handle("LSET missing 0 x", &db)
                .await
                .starts_with("-ERR no such key")
        );

        handle("SADD s1 a", &db).await;
        handle("SADD s1 b", &db).await;
        handle("SADD s2 b", &db).await;
        assert_eq!(handle("SCARD s1", &db).await, ":2\r\n");
        assert_eq!(handle("SINTER s1 s2", &db).await, "*1\r\n$1\r\nb\r\n");
    }

    #[tokio::test]
    async fn sorted_set_rank_incr_and_withscores() {
        let db = Database::new();
        handle("ZADD z 1 a", &db).await;
        handle("ZADD z 2 b", &db).await;
        assert_eq!(handle("ZCARD z", &db).await, ":2\r\n");
        assert_eq!(handle("ZRANK z b", &db).await, ":1\r\n");
        assert_eq!(handle("ZRANK z missing", &db).await, "$-1\r\n");
        assert_eq!(handle("ZINCRBY z 5 a", &db).await, "$1\r\n6\r\n"); // 1 + 5
        // a now has score 6, so b (2) ranks first.
        assert_eq!(
            handle("ZRANGE z 0 -1 WITHSCORES", &db).await,
            "*4\r\n$1\r\nb\r\n$1\r\n2\r\n$1\r\na\r\n$1\r\n6\r\n"
        );
        assert_eq!(
            handle("ZRANGEBYSCORE z 2 5", &db).await,
            "*1\r\n$1\r\nb\r\n"
        );
    }

    #[tokio::test]
    async fn spop_removes_and_returns_a_member() {
        let db = Database::new();
        handle("SADD s only", &db).await;
        assert_eq!(handle("SPOP s", &db).await, "$4\r\nonly\r\n");
        // The set is now empty and the key is dropped.
        assert_eq!(handle("SPOP s", &db).await, "$-1\r\n");
        assert_eq!(handle("EXISTS s", &db).await, ":0\r\n");
    }

    #[tokio::test]
    async fn nan_scores_are_rejected() {
        let db = Database::new();
        assert!(handle("ZADD z nan m", &db).await.starts_with("-ERR"));
        assert!(handle("ZINCRBY z nan m", &db).await.starts_with("-ERR"));
        assert!(
            handle("ZRANGEBYSCORE z nan 5", &db)
                .await
                .starts_with("-ERR")
        );
        // inf remains valid, as in Redis.
        assert_eq!(handle("ZADD z inf m", &db).await, ":1\r\n");
    }

    #[tokio::test]
    async fn generic_key_commands() {
        let db = Database::new();
        handle("SET a 1", &db).await;
        handle("RPUSH b x", &db).await;
        assert_eq!(handle("EXISTS a b missing", &db).await, ":2\r\n");
        assert_eq!(handle("TYPE a", &db).await, "+string\r\n");
        assert_eq!(handle("TYPE b", &db).await, "+list\r\n");
        assert_eq!(handle("TYPE missing", &db).await, "+none\r\n");
        assert_eq!(handle("DBSIZE", &db).await, ":2\r\n");
        assert_eq!(handle("PING", &db).await, "+PONG\r\n");
        assert_eq!(handle("PING hey", &db).await, "$3\r\nhey\r\n");
        assert_eq!(handle("ECHO hi", &db).await, "$2\r\nhi\r\n");
        assert_eq!(
            handle("RENAME missing x", &db).await,
            "-ERR no such key\r\n"
        );
        assert_eq!(handle("RENAME a c", &db).await, "+OK\r\n");
        assert_eq!(handle("GET c", &db).await, "$1\r\n1\r\n");
        assert_eq!(handle("FLUSHALL", &db).await, "+OK\r\n");
        assert_eq!(handle("DBSIZE", &db).await, ":0\r\n");
    }

    #[tokio::test]
    async fn expire_ttl_and_persist() {
        let db = Database::new();
        handle("SET k v", &db).await;
        assert_eq!(handle("TTL k", &db).await, ":-1\r\n"); // no expiry
        assert_eq!(handle("EXPIRE k 100", &db).await, ":1\r\n");
        // Remaining TTL is 99 or 100 depending on timing; assert it is positive.
        let ttl = handle("TTL k", &db).await;
        assert!(
            ttl == ":100\r\n" || ttl == ":99\r\n",
            "unexpected ttl {ttl:?}"
        );
        assert_eq!(handle("PERSIST k", &db).await, ":1\r\n");
        assert_eq!(handle("PERSIST k", &db).await, ":0\r\n"); // nothing to remove
        assert_eq!(handle("TTL missing", &db).await, ":-2\r\n");
    }

    #[tokio::test]
    async fn keys_matches_glob_patterns() {
        let db = Database::new();
        for key in ["user:1", "user:2", "post:1"] {
            handle(&format!("SET {key} v"), &db).await;
        }
        let reply = handle("KEYS user:*", &db).await;
        assert!(reply.starts_with("*2\r\n"), "unexpected reply {reply:?}");
        assert!(reply.contains("user:1"));
        assert!(reply.contains("user:2"));
        assert!(!reply.contains("post:1"));
        // `*` alone matches every key.
        assert!(handle("KEYS *", &db).await.starts_with("*3\r\n"));
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
