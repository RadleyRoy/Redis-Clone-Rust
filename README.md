# RustKV ⚡

A lightweight **Redis-inspired in-memory key-value database** written in **Rust** using **Tokio async networking**.  
RustKV supports multiple data structures, TTL expiration, and a simple TCP command protocol.

This project demonstrates building a **high-performance async database server from scratch**. It aims for a clean, well-tested, easy-to-read codebase rather than feature completeness.

> 📖 For the complete command reference with worked examples, connection methods, and reply formats, see **[USAGE.md](USAGE.md)**.

---

# Features

- Async TCP server using Tokio, with concurrent per-connection tasks
- In-memory key-value storage guarded by `Arc<RwLock<_>>`
- Multiple Redis-like data structures (strings, lists, sets, sorted sets, hashes)
- Over 50 commands, including variadic forms (`DEL k1 k2`, `RPUSH k a b c`)
- Key expiration with `EXPIRE`/`TTL`/`PERSIST`, evicted lazily **and** by a background sweeper
- Generic key commands (`EXISTS`, `TYPE`, `KEYS` with glob matching, `RENAME`, ...)
- Configurable host/port/log-level via CLI flags or environment variables
- Structured logging with `tracing`, `INFO` introspection, and graceful Ctrl+C shutdown
- Proper RESP replies, including `WRONGTYPE` errors for type mismatches
- Robust input handling: malformed commands return an error reply, never a crash

### Supported Data Types

| Data Structure | Description |
|---|---|
| Strings | Basic key → value storage, counters, append |
| Lists | Push/pop at either end, range and index queries |
| Sets | Unique unordered members, set algebra |
| Sorted Sets | Score-based ordered members, rank and score ranges |
| Hashes | Field → value maps stored under one key |

---

# Architecture

```
client
   │
   ▼
TCP Server (Tokio)          server/mod.rs
   │
   ▼
Parse → Execute → Encode    command/mod.rs + resp.rs
   │
   ▼
Database (one store)        database/db.rs
   │
   ▼
Value = Str | List | Set | SortedSet | Hash
```

A request flows in a straight line: the **server** reads a line and hands it to `command::handle`, which **parses** it into a typed `Command`, **executes** it against the `Database`, and asks the `resp` module to **encode** the reply. Parsing is kept separate from execution, and the wire format lives in one module.

### Modules

```
src
│
├── main.rs                 # entry point; parses config, starts the server
│
├── config.rs               # CLI/env configuration (clap)
│
├── resp.rs                 # RESP reply encoders (the wire format)
│
├── server
│   └── mod.rs              # TCP accept loop + per-connection I/O
│
├── command
│   └── mod.rs              # Command enum: parse, execute, encode
│
└── database
    ├── db.rs               # thread-safe key/value store (expiry + type checks)
    └── data_structure.rs   # RList, RSet, RSortedSet, RHash value types
```

---

# How It Works

### 1. Server

`main.rs` starts a Tokio async TCP server. Each client connection is handled concurrently with `tokio::spawn`, so multiple clients interact with the database simultaneously. Every I/O operation is handled (not unwrapped): a misbehaving client ends only its own task.

### 2. Command Parsing

Incoming inline commands are parsed inside `command/mod.rs` into a typed `Command`. Command names are case-insensitive. Anything malformed (unknown command, wrong argument count, non-numeric index) produces an error reply instead of panicking.

### 3. Database

All keys live in a **single** `Arc<RwLock<HashMap<String, Entry>>>`, where each `Entry` holds one `Value` (string, list, set, sorted set, or hash) plus an optional expiry deadline.

Storing one value per key means:

| Property | Result |
|---|---|
| One type per key | Using the wrong command returns a `WRONGTYPE` error |
| Uniform expiry | TTLs apply to every type, not just strings |
| Uniform `DEL` | `DEL` removes a key of any type and reports a correct count |

---

# Installation

### 1. Clone the repository

```bash
git clone https://github.com/RadleyRoy/Redis-Clone-Rust.git
cd Redis-Clone-Rust
```

### 2. Build the project

```bash
cargo build --release
```

### 3. Run the server

```bash
cargo run --release
```

Server starts at `127.0.0.1:7335` and logs to stderr. Stop it with `Ctrl+C`,
which triggers a graceful shutdown (it stops accepting new connections and lets
in-flight ones finish).

---

# Configuration

Every setting is a CLI flag with an environment-variable fallback:

| Flag | Env var | Default | Description |
|---|---|---|---|
| `--host` | `REDIS_CLONE_HOST` | `127.0.0.1` | Address to bind. |
| `--port` / `-p` | `REDIS_CLONE_PORT` | `7335` | Port to listen on. |
| `--log-level` | `REDIS_CLONE_LOG` | `info` | `error`/`warn`/`info`/`debug`/`trace`. |
| `--sweep-secs` | `REDIS_CLONE_SWEEP_SECS` | `10` | Background expiry sweep interval. |

```bash
cargo run --release -- --port 6400 --log-level debug
# or
REDIS_CLONE_PORT=6400 cargo run --release
```

Run `cargo run -- --help` for the full generated usage.

---

# Connecting to the Server

RustKV accepts two request framings and detects them per line:

- **RESP arrays** — the multi-bulk format `redis-cli` sends, so it works out of the box and values may contain spaces.
- **Inline** — one whitespace-separated command per line, handy for `telnet`/`netcat`.

```bash
redis-cli -p 7335              # RESP: SET note "hello world" works
# or
nc 127.0.0.1 7335             # inline: type commands directly
```

> With the inline framing, arguments are split on whitespace, so a value cannot contain spaces — use `redis-cli` (RESP) when you need spaces in a value.

---

# Supported Commands

Command names are case-insensitive. Replies use RESP (`+` simple string, `-` error, `:` integer, `$` bulk string, `*` array). See **[USAGE.md](USAGE.md)** for full examples.

## Generic key commands

These work on a key of any type.

| Command | Description |
|---|---|
| `DEL key [key ...]` | Delete keys of any type. Returns the number removed. |
| `EXISTS key [key ...]` | Return how many of the named keys exist (duplicates counted). |
| `TYPE key` | Return the key's type (`string`/`list`/`set`/`zset`/`hash`/`none`). |
| `KEYS pattern` | Return keys matching a glob (`*`, `?`, `[abc]`, `[a-z]`, `\` escape). |
| `EXPIRE key seconds` | Set a TTL. A non-positive TTL deletes the key. Returns `1` if the key exists. |
| `TTL key` | Remaining TTL in seconds: `-2` if missing, `-1` if no expiry. |
| `PERSIST key` | Remove a key's TTL. Returns `1` if one was removed. |
| `RENAME key newkey` | Rename a key (moving its value and TTL); errors if it is missing. |
| `DBSIZE` | Return the number of live keys. |
| `FLUSHALL` | Remove every key. |
| `INFO` | Return server stats (uptime, connected clients, key count). |
| `PING [message]` | Reply `+PONG`, or echo `message` if given. |
| `ECHO message` | Reply with `message`. |

## Strings

| Command | Description |
|---|---|
| `SET key value [EX seconds]` | Store a value, optionally expiring after `seconds`. Replies `+OK`. (`EXP` is accepted as an alias for `EX`.) |
| `GET key` | Return the string, or nil if missing/expired. |
| `SETNX key value` | Set only if the key does not exist. Returns `1` if set, else `0`. |
| `GETSET key value` | Set a new value and return the old one (clears any TTL). |
| `MSET key value [key value ...]` | Set several keys at once. Replies `+OK`. |
| `MGET key [key ...]` | Return each key's value, nil for missing/non-string keys. |
| `INCR key` / `DECR key` | Increment / decrement an integer value (starting from 0). Returns the new value. |
| `INCRBY key n` / `DECRBY key n` | Increment / decrement by `n`. |
| `APPEND key value` | Append to the string (creating it if absent). Returns the new length. |
| `STRLEN key` | Return the string length (0 if missing). |

## Lists

| Command | Description |
|---|---|
| `LPUSH key value [value ...]` | Prepend one or more elements. Returns the new length. |
| `RPUSH key value [value ...]` | Append one or more elements. Returns the new length. |
| `LPOP key` / `RPOP key` | Remove and return the first / last element. |
| `LLEN key` | Return the list length. |
| `LINDEX key index` | Return the element at `index` (negative counts from the end). |
| `LSET key index value` | Set the element at `index`; errors if the key or index is out of range. |
| `LRANGE key start stop` | Return the inclusive range. Negative indices count from the end (`-1` is last). |

> **Note:** the argument order is `LRANGE key start stop` (the key comes first, matching Redis).

## Sets

| Command | Description |
|---|---|
| `SADD key member [member ...]` | Add members. Returns the number newly added. |
| `SREM key member [member ...]` | Remove members. Returns the number removed. |
| `SMEMBERS key` | Return all members. |
| `SISMEMBER key member` | Return `1` if a member, else `0`. |
| `SCARD key` | Return the number of members. |
| `SPOP key` | Remove and return an arbitrary member. |
| `SINTER key [key ...]` | Intersection of the given sets. |
| `SUNION key [key ...]` | Union of the given sets. |
| `SDIFF key [key ...]` | Members of the first set not in the rest. |

## Sorted Sets

Sorted sets store **members ordered by score**, implemented with a `HashMap` (for O(1) score lookup) kept in sync with a `BTreeSet` (for ordered range queries).

| Command | Description |
|---|---|
| `ZADD key score member [score member ...]` | Add or update members. Returns the number newly added. |
| `ZREM key member` | Remove a member. Returns `1` if present, else `0`. |
| `ZRANGE key start stop [WITHSCORES]` | Return members by ascending score. Supports negative indices; `WITHSCORES` interleaves each score. |
| `ZRANGEBYSCORE key min max` | Return members with score in `[min, max]`. Supports `(` for exclusive bounds and `+inf`/`-inf`. |
| `ZSCORE key member` | Return the member's score, or nil. |
| `ZCARD key` | Return the number of members. |
| `ZRANK key member` | Return the 0-based ascending rank of a member, or nil. |
| `ZINCRBY key increment member` | Add `increment` to a member's score. Returns the new score. |

## Hashes

Hashes map string fields to string values under a single key.

| Command | Description |
|---|---|
| `HSET key field value` | Set a field. Returns `1` if the field is new, `0` if overwritten. |
| `HGET key field` | Return a field's value, or nil. |
| `HDEL key field` | Delete a field. Returns `1` if present, else `0`. |
| `HGETALL key` | Return all fields and values (flat `[field, value, ...]` array). |
| `HKEYS key` / `HVALS key` | Return all field names / all values. |
| `HLEN key` | Return the number of fields. |
| `HEXISTS key field` | Return `1` if the field exists, else `0`. |
| `HINCRBY key field n` | Increment a field's integer value by `n`. Returns the new value. |

---

# TTL Expiration

A key set with `EX seconds` (or given a TTL via `EXPIRE`) stores an expiry deadline alongside its value. Expiration is **lazy** — on the next access after the deadline passes, the key is treated as missing and evicted — **and** a background sweeper periodically removes expired keys even if they are never touched again (interval set by `--sweep-secs`).

---

# Concurrency Model

RustKV serves multiple clients simultaneously:

- `tokio::spawn` → one task per connection
- `Arc` → the store is shared across tasks
- `RwLock` → safe concurrent reads and exclusive writes

---

# Example Session

```
SET name Alice          -> +OK
GET name                -> $5 / Alice
RPUSH mylist a          -> :1
RPUSH mylist b          -> :2
LRANGE mylist 0 -1      -> *2 / a / b
GET mylist              -> -WRONGTYPE Operation against a key holding the wrong kind of value
ZADD board 5 alice      -> :1
ZADD board 3 bob        -> :1
ZRANGE board 0 -1       -> *2 / bob / alice
DEL name                -> :1
```

---

# Testing & CI

```bash
cargo test        # unit + end-to-end TCP tests (protocol, ranges, expiry, WRONGTYPE, ...)
cargo clippy      # lint
cargo fmt --check # verify formatting
```

A manually-triggered **Verify** GitHub Actions workflow runs each check — format, build, clippy, tests — as a separate, individually-visible step. Launch it from the repository's **Actions → Verify → Run workflow**. It is read-only and does not modify the repo.

---

# Dependencies

```toml
tokio               # async runtime
ordered-float       # totally-ordered f64 for sorted-set scores
clap                # CLI argument / environment parsing
tracing             # structured, levelled logging
tracing-subscriber  # log formatting and filtering
```

---

# Limitations

This is a learning project, not a drop-in Redis replacement:

- Only the commands above are implemented (no scripting, streams, or bitmaps).
- Requests are parsed as RESP arrays or inline commands, but replies are always RESP2; there is no RESP3, and inline values cannot contain spaces (use `redis-cli`).
- No persistence (RDB/AOF), replication, pub/sub, transactions, or clustering.
- A single database (no `SELECT`).
