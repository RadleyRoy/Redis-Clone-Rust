# Usage Guide

This guide explains how to run the server, how to talk to it, and documents
every supported command with examples.

## 1. Running the server

Build and start the server (it listens on `127.0.0.1:7335` by default):

```sh
cargo run --release
```

You should see a log line like:

```
2026-07-06T17:38:13Z  INFO redis_clone listening address=127.0.0.1:7335
```

Stop it with `Ctrl+C` — the server stops accepting connections and lets
in-flight requests finish before exiting.

### Configuration

Each setting is a CLI flag with an environment-variable fallback:

| Flag | Env var | Default | Description |
| --- | --- | --- | --- |
| `--host` | `REDIS_CLONE_HOST` | `127.0.0.1` | Address to bind. |
| `--port` / `-p` | `REDIS_CLONE_PORT` | `7335` | Port to listen on. |
| `--log-level` | `REDIS_CLONE_LOG` | `info` | `error`/`warn`/`info`/`debug`/`trace`. |
| `--sweep-secs` | `REDIS_CLONE_SWEEP_SECS` | `10` | Background expiry sweep interval. |

```sh
cargo run --release -- --port 6400 --log-level debug
```

Run `cargo run -- --help` for the generated usage text.

## 2. Connecting

The server accepts two request framings and picks one per line automatically:

- **RESP arrays** — the multi-bulk format `redis-cli` sends. Values may contain
  spaces (quote them). Recommended.
- **Inline** — one whitespace-separated command per line, terminated by a
  newline. Convenient for `telnet`/`netcat`, but values cannot contain spaces.

### Option A — `redis-cli` (RESP)

```sh
redis-cli -p 7335
127.0.0.1:7335> SET greeting hello
OK
127.0.0.1:7335> SET note "hello world"   # spaces are fine over RESP
OK
127.0.0.1:7335> GET note
"hello world"
```

### Option B — `telnet` / `netcat`

```sh
telnet 127.0.0.1 7335
```

Then type commands directly:

```
SET greeting hello
GET greeting
```

### Option C — a raw socket from a script

Anything that can open a TCP socket and write lines works. For example, with
PowerShell on Windows:

```powershell
$client = [System.Net.Sockets.TcpClient]::new("127.0.0.1", 7335)
$stream = $client.GetStream()
$writer = [System.IO.StreamWriter]::new($stream); $writer.NewLine = "`r`n"
$reader = [System.IO.StreamReader]::new($stream)

$writer.WriteLine("SET greeting hello"); $writer.Flush()
$reader.ReadLine()   # +OK
$client.Close()
```

> **Note:** With the **inline** framing (telnet/netcat/raw socket above),
> arguments are split on whitespace, so a value cannot contain spaces
> (`SET note hello world` is parsed as extra arguments and rejected). To store a
> value with spaces, use `redis-cli` (or send a RESP array), which frames each
> argument explicitly.

## 3. Understanding the replies

Replies use RESP. You will see these forms:

| Prefix     | Meaning        | Example                         |
| ---------- | -------------- | ------------------------------- |
| `+`        | Simple string  | `+OK\r\n`                       |
| `-`        | Error          | `-ERR unknown command 'FOO'\r\n`|
| `:`        | Integer        | `:1\r\n`                        |
| `$`        | Bulk string    | `$5\r\nhello\r\n`               |
| `$-1`      | Null (no value)| `$-1\r\n`                       |
| `*`        | Array          | `*2\r\n$1\r\na\r\n$1\r\nb\r\n`  |

`redis-cli` renders these for you; with `telnet` you see the raw bytes.

## 4. Command reference

Command names are **case-insensitive** (`SET`, `set` and `Set` are equivalent).

### Strings

#### `SET key value [EX seconds]`

Stores `value` at `key`, optionally expiring after `seconds`. Overwrites any
existing value (of any type). Replies `+OK`.

```
SET user:1 radley           -> +OK
SET session abc123 EX 60    -> +OK   (expires in 60 seconds)
```

> `EXP` is accepted as an alias for `EX`.

#### `GET key`

Returns the string at `key`, or null if it does not exist / has expired.
Returns `WRONGTYPE` if the key holds a non-string value.

```
GET user:1     -> "radley"
GET missing    -> (nil)
```

#### `SETNX key value` / `GETSET key value`

`SETNX` sets `key` only if it does not already exist (returns `1` if set, else
`0`). `GETSET` sets a new value and returns the previous one (clearing any TTL).

```
SETNX user:1 alice   -> (integer) 0   (already exists)
GETSET user:1 bob    -> "radley"
```

#### `MSET key value [key value ...]` / `MGET key [key ...]`

Set or get several keys in one call. `MGET` returns nil for keys that are
missing or hold a non-string value.

```
MSET a 1 b 2         -> +OK
MGET a missing b     -> [1, (nil), 2]
```

#### `INCR` / `DECR` / `INCRBY` / `DECRBY`

Treat the string as a 64-bit integer (a missing key starts at 0) and return the
new value. A non-integer value gives an error.

```
INCR visits          -> (integer) 1
INCRBY visits 9      -> (integer) 10
DECR visits          -> (integer) 9
```

#### `APPEND key value` / `STRLEN key`

`APPEND` appends to the string (creating it if absent) and returns the new
length; `STRLEN` returns the current length.

```
APPEND log hello     -> (integer) 5
STRLEN log           -> (integer) 5
```

### Generic key commands

These apply to a key of any type.

#### `DEL key [key ...]`

Deletes keys no matter what type they hold. Returns the number removed.

```
DEL user:1 user:2    -> (integer) 2
DEL user:1           -> (integer) 0
```

#### `EXISTS key [key ...]` / `TYPE key`

`EXISTS` returns how many of the named keys exist (duplicates counted). `TYPE`
returns the key's type: `string`, `list`, `set`, `zset`, `hash`, or `none`.

```
EXISTS user:1 nope   -> (integer) 1
TYPE user:1          -> string
```

#### `KEYS pattern`

Returns keys matching a glob pattern (`*`, `?`, `[abc]`, `[a-z]`, `\` to escape).

```
KEYS user:*          -> [user:1, user:2]
KEYS *               -> (every key)
```

#### `EXPIRE key seconds` / `TTL key` / `PERSIST key`

`EXPIRE` sets a TTL (a non-positive value deletes the key). `TTL` returns the
seconds remaining (`-1` if no expiry, `-2` if the key is missing). `PERSIST`
removes a TTL.

```
EXPIRE session 100   -> (integer) 1
TTL session          -> (integer) 99
PERSIST session      -> (integer) 1
TTL session          -> (integer) -1
```

#### `RENAME key newkey` / `DBSIZE` / `FLUSHALL` / `PING` / `ECHO`

```
RENAME a c           -> +OK      (errors with "no such key" if a is missing)
DBSIZE               -> (integer) 3
FLUSHALL             -> +OK
PING                 -> +PONG
PING hello           -> "hello"
ECHO hi              -> "hi"
```

#### `INFO`

Returns a Redis-style block of server statistics: uptime, connected clients, and
key count.

```
INFO                 -> # Server
                        server_name:redis_clone
                        uptime_in_seconds:42
                        # Clients
                        connected_clients:1
                        # Keyspace
                        keys:3
```

### Lists

Lists are ordered sequences of strings.

#### `LPUSH key value [value ...]` / `RPUSH key value [value ...]`

Prepends (`LPUSH`) or appends (`RPUSH`) one or more values, creating the list if
needed. Returns the new list length.

```
RPUSH tasks a b   -> (integer) 2
LPUSH tasks z     -> (integer) 3   (list is now: z, a, b)
```

#### `LPOP key` / `RPOP key`

Removes and returns the first (`LPOP`) or last (`RPOP`) element, or null if the
list is empty/missing. When the last element is removed, the key is deleted.

```
LPOP tasks     -> "z"
RPOP tasks     -> "b"
```

#### `LRANGE key start stop`

Returns the elements between `start` and `stop`, **inclusive**. Indices are
zero-based, and negative indices count from the end (`-1` is the last element).
Out-of-range indices are clamped.

```
RPUSH nums a
RPUSH nums b
RPUSH nums c
LRANGE nums 0 -1   -> [a, b, c]
LRANGE nums 0 0    -> [a]
LRANGE nums -2 -1  -> [b, c]
```

#### `LLEN key` / `LINDEX key index` / `LSET key index value`

`LLEN` returns the length; `LINDEX` returns the element at `index` (negative
counts from the end); `LSET` overwrites it. `LSET` errors with "no such key" or
"index out of range".

```
LLEN nums          -> (integer) 3
LINDEX nums -1     -> "c"
LSET nums 0 z      -> +OK
```

### Sets

Sets are unordered collections of unique strings.

#### `SADD key member [member ...]`

Adds members. Returns the number that were newly added.

```
SADD tags rust go rust   -> (integer) 2
SADD tags rust           -> (integer) 0
```

#### `SREM key member [member ...]`

Removes members. Returns the number that were present.

```
SREM tags rust missing   -> (integer) 1
```

#### `SMEMBERS key` / `SCARD key` / `SPOP key`

`SMEMBERS` returns all members, `SCARD` their count, and `SPOP` removes and
returns an arbitrary one.

```
SMEMBERS tags    -> [rust, go]
SCARD tags       -> (integer) 2
SPOP tags        -> "go"
```

#### `SISMEMBER key member`

Returns `1` if `member` is in the set, `0` otherwise.

```
SISMEMBER tags rust  -> (integer) 1
```

#### `SINTER` / `SUNION` / `SDIFF key [key ...]`

Set algebra across one or more sets (missing keys are empty). `SINTER` returns
the intersection, `SUNION` the union, and `SDIFF` the members of the first set
not present in the rest.

```
SADD a x y
SADD b y z
SINTER a b       -> [y]
SUNION a b       -> [x, y, z]
SDIFF a b        -> [x]
```

### Sorted sets

Sorted sets pair each unique member with a floating-point score and keep members
ordered by score (ties broken lexicographically by member).

#### `ZADD key score member [score member ...]`

Adds members with scores, updating any that already exist. Returns the number of
members that were newly added (not updated).

```
ZADD board 5 alice 3 bob   -> (integer) 2
ZADD board 8 alice         -> (integer) 0   (score updated, not added)
```

#### `ZREM key member`

Removes `member`. Returns `1` if it was present, `0` otherwise.

```
ZREM board bob   -> (integer) 1
```

#### `ZRANGE key start stop [WITHSCORES]`

Returns members ranked by ascending score, between `start` and `stop`
inclusive. Supports negative indices, like `LRANGE`. With `WITHSCORES`, each
member is followed by its score.

```
ZADD board 5 alice
ZADD board 3 bob
ZRANGE board 0 -1              -> [bob, alice]   (bob's score 3 < alice's 5)
ZRANGE board 0 -1 WITHSCORES   -> [bob, 3, alice, 5]
```

#### `ZRANGEBYSCORE key min max`

Returns members whose score falls within `[min, max]`. Prefix a bound with `(`
for exclusive, and use `+inf`/`-inf` for open ranges.

```
ZRANGEBYSCORE board 3 5     -> [bob, alice]
ZRANGEBYSCORE board (3 +inf -> [alice]         (bob's 3 is excluded)
```

#### `ZSCORE` / `ZCARD` / `ZRANK` / `ZINCRBY`

`ZSCORE` returns a member's score (or nil); `ZCARD` the member count; `ZRANK`
the 0-based ascending rank (or nil); `ZINCRBY` adds to a member's score and
returns the new value.

```
ZSCORE board alice  -> "5"
ZCARD board         -> (integer) 2
ZRANK board alice   -> (integer) 1
ZINCRBY board 2 bob -> "5"
```

### Hashes

Hashes map string fields to string values under a single key.

#### `HSET` / `HGET` / `HDEL` / `HEXISTS`

```
HSET user:1 name radley   -> (integer) 1   (new field)
HSET user:1 name alice    -> (integer) 0   (overwritten)
HGET user:1 name          -> "alice"
HEXISTS user:1 name       -> (integer) 1
HDEL user:1 name          -> (integer) 1
```

#### `HGETALL` / `HKEYS` / `HVALS` / `HLEN`

```
HSET user:1 name radley
HSET user:1 visits 3
HGETALL user:1   -> [name, radley, visits, 3]
HKEYS user:1     -> [name, visits]
HVALS user:1     -> [radley, 3]
HLEN user:1      -> (integer) 2
```

#### `HINCRBY key field n`

Increments a field's integer value by `n` (a missing field starts at 0).

```
HINCRBY user:1 visits 5   -> (integer) 8
```

## 5. Errors you may encounter

| Reply                                          | Cause                                          |
| ---------------------------------------------- | ---------------------------------------------- |
| `-ERR unknown command '...'`                   | The command name is not recognised.            |
| `-ERR wrong number of arguments for '...'`     | Too few/many arguments for the command.        |
| `-ERR value '...' is not a valid integer`      | A numeric argument (TTL/index) was not a number.|
| `-ERR value '...' is not a valid float`        | A `ZADD`/`ZINCRBY` score was not a number.     |
| `-ERR value is not an integer or out of range` | `INCR`/`DECR` on a non-integer string.         |
| `-ERR hash value is not an integer`            | `HINCRBY` on a non-integer field.              |
| `-ERR no such key`                             | `RENAME`/`LSET` on a missing key.              |
| `-ERR index out of range`                      | `LSET` with an out-of-bounds index.            |
| `-WRONGTYPE Operation against a key ...`       | The command does not apply to the key's type.  |

Malformed input never crashes the server — it always produces an error reply.

## 6. A complete session

```
SET user:1 radley           -> +OK
GET user:1                  -> "radley"
SET session abc EX 30       -> +OK
RPUSH tasks buy-milk        -> (integer) 1
RPUSH tasks walk-dog        -> (integer) 2
LRANGE tasks 0 -1           -> [buy-milk, walk-dog]
SADD tags rust go           -> -ERR wrong number of arguments for 'sadd' command
SADD tags rust              -> (integer) 1
ZADD scores 10 alice        -> (integer) 1
ZADD scores 7 bob           -> (integer) 1
ZRANGE scores 0 -1          -> [bob, alice]
DEL user:1                  -> (integer) 1
GET user:1                  -> (nil)
```
