# RustKV ⚡

A lightweight **Redis-inspired in-memory key-value database** written in **Rust** using **Tokio async networking**.  
RustKV supports multiple data structures, TTL expiration, and a simple TCP command protocol.

This project demonstrates building a **high-performance async database server from scratch**. It aims for a clean, well-tested, easy-to-read codebase rather than feature completeness.

> 📖 For the complete command reference with worked examples, connection methods, and reply formats, see **[USAGE.md](USAGE.md)**.

---

# Features

- Async TCP server using Tokio, with concurrent per-connection tasks
- In-memory key-value storage guarded by `Arc<RwLock<_>>`
- Multiple Redis-like data structures (strings, lists, sets, sorted sets)
- Key expiration (TTL), evicted lazily on access
- Proper RESP replies, including `WRONGTYPE` errors for type mismatches
- Robust input handling: malformed commands return an error reply, never a crash

### Supported Data Types

| Data Structure | Description |
|---|---|
| Strings | Basic key → value storage |
| Lists | Push/pop at either end, range queries |
| Sets | Unique unordered members |
| Sorted Sets | Score-based ordered members |

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
Value = Str | List | Set | SortedSet
```

A request flows in a straight line: the **server** reads a line and hands it to `command::handle`, which **parses** it into a typed `Command`, **executes** it against the `Database`, and asks the `resp` module to **encode** the reply. Parsing is kept separate from execution, and the wire format lives in one module.

### Modules

```
src
│
├── main.rs                 # entry point; starts the server
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
    └── data_structure.rs   # RList, RSet, RSortedSet value types
```

---

# How It Works

### 1. Server

`main.rs` starts a Tokio async TCP server. Each client connection is handled concurrently with `tokio::spawn`, so multiple clients interact with the database simultaneously. Every I/O operation is handled (not unwrapped): a misbehaving client ends only its own task.

### 2. Command Parsing

Incoming inline commands are parsed inside `command/mod.rs` into a typed `Command`. Command names are case-insensitive. Anything malformed (unknown command, wrong argument count, non-numeric index) produces an error reply instead of panicking.

### 3. Database

All keys live in a **single** `Arc<RwLock<HashMap<String, Entry>>>`, where each `Entry` holds one `Value` (string, list, set, or sorted set) plus an optional expiry deadline.

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

Server starts at:

```
127.0.0.1:7335
```

---

# Connecting to the Server

RustKV understands the **inline** protocol: one command per line, arguments separated by spaces. Use `redis-cli`, `telnet`, or `netcat`:

```bash
redis-cli -p 7335      # inline mode
# or
nc 127.0.0.1 7335
```

> Because arguments are split on whitespace, values cannot contain spaces.

---

# Supported Commands

Command names are case-insensitive. Replies use RESP (`+` simple string, `-` error, `:` integer, `$` bulk string, `*` array). See **[USAGE.md](USAGE.md)** for full examples.

## Strings

| Command | Description |
|---|---|
| `SET key value [EX seconds]` | Store a value, optionally expiring after `seconds`. Replies `+OK`. (`EXP` is accepted as an alias for `EX`.) |
| `GET key` | Return the string, or nil if missing/expired. |
| `DEL key` | Delete a key of any type. Returns `1` if removed, else `0`. |

## Lists

| Command | Description |
|---|---|
| `LPUSH key value` | Prepend an element. Returns the new length. |
| `RPUSH key value` | Append an element. Returns the new length. |
| `LPOP key` / `RPOP key` | Remove and return the first / last element. |
| `LRANGE key start stop` | Return the inclusive range. Negative indices count from the end (`-1` is last). |

> **Note:** the argument order is `LRANGE key start stop` (the key comes first, matching Redis).

## Sets

| Command | Description |
|---|---|
| `SADD key value` | Add a member. Returns `1` if added, `0` if already present. |
| `SREM key value` | Remove a member. Returns `1` if present, else `0`. |
| `SMEMBERS key` | Return all members. |
| `SISMEMBER key value` | Return `1` if a member, else `0`. |

## Sorted Sets

Sorted sets store **members ordered by score**, implemented with a `HashMap` (for O(1) score lookup) kept in sync with a `BTreeSet` (for ordered range queries).

| Command | Description |
|---|---|
| `ZADD key score member` | Add or update a member. Returns `1` only when newly added, `0` on a score update. |
| `ZREM key member` | Remove a member. Returns `1` if present, else `0`. |
| `ZRANGE key start stop` | Return members by ascending score. Supports negative indices. |
| `ZSCORE key member` | Return the member's score, or nil. |

---

# TTL Expiration

A key set with `EX seconds` stores an expiry deadline alongside its value. Expiration is **lazy**: on the next access after the deadline passes, the key is treated as missing and evicted. There is no background sweeper.

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
cargo test        # run the unit tests (16 covering protocol, ranges, expiry, WRONGTYPE, ...)
cargo clippy      # lint
cargo fmt         # format
```

A manually-triggered **Verify** GitHub Actions workflow runs the full pass (format, build, clippy, tests) and commits any formatting fixes. Launch it from the repository's **Actions → Verify → Run workflow**.

---

# Dependencies

```toml
tokio          # async runtime
ordered-float  # totally-ordered f64 for sorted-set scores
```

---

# Limitations

This is a learning project, not a drop-in Redis replacement:

- Only the commands above are implemented.
- Requests use the **inline** protocol, so values cannot contain spaces, and full RESP array / pipelined requests are not parsed.
- No persistence (RDB/AOF), replication, pub/sub, or clustering.
- TTLs are evicted lazily (on access), not by a background sweeper.
