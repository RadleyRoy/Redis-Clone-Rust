# RustKV ⚡

A lightweight **Redis-inspired in-memory key-value database** written in **Rust** using **Tokio async networking**.  
RustKV supports multiple data structures, TTL expiration, and a simple TCP command protocol.

This project demonstrates building a **high-performance async database server from scratch**.

---

# Features

- Async TCP server using Tokio
- In-memory key-value storage
- Multiple Redis-like data structures
- Concurrent client handling
- Key expiration (TTL)
- Thread-safe database using `Arc<RwLock<_>>`

### Supported Data Types

| Data Structure | Description |
|---|---|
| Strings | Basic key → value storage |
| Lists | Push/pop operations |
| Sets | Unique unordered members |
| Sorted Sets | Score-based ordered members |

---

# Architecture

```
client
   │
   ▼
TCP Server (Tokio)
   │
   ▼
Command Parser
   │
   ▼
Database Layer
   │
   ├── String KV Store
   ├── Lists
   ├── Sets
   └── Sorted Sets
```

### Modules

```
src
│
├── main.rs
│
├── server
│   └── mod.rs        # TCP server + client handling
│
├── command
│   └── mod.rs        # command parser
│
└── database
    ├── db.rs         # main database implementation
    └── datastructure.rs
```

---

# How It Works

### 1. Server

`main.rs` starts a Tokio async TCP server.

Each client connection is handled concurrently using:

```rust
tokio::spawn(...)
```

This allows multiple clients to interact with the database simultaneously.

---

### 2. Command Parsing

Incoming commands are parsed inside:

```
command/mod.rs
```

Example:

```
SET key value
GET key
```

Commands are converted into database operations.

---

### 3. Database

The database uses:

```rust
Arc<RwLock<HashMap<...>>>
```

This allows:

- Multiple concurrent readers
- Safe mutable writes

Database components:

| Storage | Description |
|---|---|
| `db` | String key-value store |
| `expiry` | TTL expiration tracking |
| `list` | List data structure |
| `set` | Set data structure |
| `sorted_set` | Sorted set |

---

# Installation

### 1. Clone the repository

```bash
git clone https://github.com/yourusername/rustkv.git
cd rustkv
```

### 2. Build the project

```bash
cargo build
```

### 3. Run the server

```bash
cargo run
```

Server starts at:

```
127.0.0.1:7335
```

---

# Connecting to the Server

You can use **telnet** or **netcat**.

Example:

```bash
nc 127.0.0.1 7335
```

---

# Supported Commands

## Strings

### SET

```
SET key value
```

Example:

```
SET name alice
```

Response:

```
+OK
```

---

### SET with Expiry

```
SET key value EXP seconds
```

Example:

```
SET token abc123 EXP 10
```

Key expires after **10 seconds**.

---

### GET

```
GET key
```

Example:

```
GET name
```

Response:

```
$5
alice
```

---

### DEL

```
DEL key
```

Deletes a key.

---

# Lists

### LPUSH

```
LPUSH key value
```

Adds element to the **head**.

---

### RPUSH

```
RPUSH key value
```

Adds element to the **tail**.

---

### LPOP

```
LPOP key
```

Removes from **head**.

---

### RPOP

```
RPOP key
```

Removes from **tail**.

---

### LRANGE

```
LRANGE start end key
```

Example:

```
LRANGE 0 2 mylist
```

---

# Sets

### SADD

```
SADD key value
```

Adds a member to a set.

---

### SREM

```
SREM key value
```

Removes a member.

---

### SMEMBERS

```
SMEMBERS key
```

Returns all members.

---

### SISMEMBER

```
SISMEMBER key value
```

Checks membership.

---

# Sorted Sets

Sorted sets store **members ordered by score**.

Internally implemented using:

```
HashMap + BTreeSet
```

---

### ZADD

```
ZADD key score member
```

Example:

```
ZADD leaderboard 100 player1
```

---

### ZREM

```
ZREM key member
```

---

### ZRANGE

```
ZRANGE key start end
```

Returns members ordered by score.

---

### ZSCORE

```
ZSCORE key member
```

Returns score.

---

# TTL Expiration

Keys set with expiration are stored in:

```
expiry: HashMap<String, Instant>
```

On `GET`, the server checks:

```
Instant::now() > expiry
```

Expired keys are automatically deleted.

---

# Concurrency Model

RustKV supports **multiple clients simultaneously**.

Key mechanisms:

- `tokio::spawn` → concurrent client handling
- `Arc` → shared ownership
- `RwLock` → safe read/write concurrency

---

# Example Session

```
SET name Alice
+OK

GET name
$5
Alice

LPUSH mylist a
+OK

LPUSH mylist b
+OK

LRANGE 0 1 mylist
*2
$1
b
$1
a
```

---

# Dependencies

```toml
tokio
ordered-float
```

- Tokio → async runtime
- ordered-float → ordered floating point values for sorted sets

---
