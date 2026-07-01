# Usage Guide

This guide explains how to run the server, how to talk to it, and documents
every supported command with examples.

## 1. Running the server

Build and start the server (it listens on `127.0.0.1:7335`):

```sh
cargo run --release
```

You should see:

```
Redis clone listening on 127.0.0.1:7335
```

Stop it with `Ctrl+C`.

## 2. Connecting

The server understands the **inline** command protocol: send one command per
line, with arguments separated by spaces, terminated by a newline. Any
line-based TCP client works.

### Option A — `redis-cli`

```sh
redis-cli -p 7335
127.0.0.1:7335> SET greeting hello
OK
127.0.0.1:7335> GET greeting
"hello"
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

> **Note:** Because arguments are split on whitespace, values themselves cannot
> contain spaces (e.g. `SET note hello world` is parsed as three arguments and
> rejected). Use a single token such as `hello_world`.

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

#### `DEL key`

Deletes `key` no matter what type it holds. Returns `1` if a key was removed,
otherwise `0`.

```
DEL user:1     -> (integer) 1
DEL user:1     -> (integer) 0
```

### Lists

Lists are ordered sequences of strings.

#### `LPUSH key value` / `RPUSH key value`

Prepends (`LPUSH`) or appends (`RPUSH`) `value`, creating the list if needed.
Returns the new list length.

```
RPUSH tasks a  -> (integer) 1
RPUSH tasks b  -> (integer) 2
LPUSH tasks z  -> (integer) 3   (list is now: z, a, b)
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

### Sets

Sets are unordered collections of unique strings.

#### `SADD key value`

Adds `value`. Returns `1` if it was added, `0` if it was already a member.

```
SADD tags rust   -> (integer) 1
SADD tags rust   -> (integer) 0
```

#### `SREM key value`

Removes `value`. Returns `1` if it was present, `0` otherwise.

```
SREM tags rust   -> (integer) 1
```

#### `SMEMBERS key`

Returns all members (in no particular order), or an empty array if the key does
not exist.

```
SMEMBERS tags    -> [rust, go]
```

#### `SISMEMBER key value`

Returns `1` if `value` is a member, `0` otherwise.

```
SISMEMBER tags rust  -> (integer) 1
```

### Sorted sets

Sorted sets pair each unique member with a floating-point score and keep members
ordered by score (ties broken lexicographically by member).

#### `ZADD key score member`

Adds `member` with `score`, or updates its score if it already exists. Returns
`1` only when the member is newly added, `0` when it already existed.

```
ZADD board 5 alice   -> (integer) 1
ZADD board 3 bob     -> (integer) 1
ZADD board 8 alice   -> (integer) 0   (score updated, not added)
```

#### `ZREM key member`

Removes `member`. Returns `1` if it was present, `0` otherwise.

```
ZREM board bob   -> (integer) 1
```

#### `ZRANGE key start stop`

Returns members ranked by ascending score, between `start` and `stop`
inclusive. Supports negative indices, like `LRANGE`.

```
ZADD board 5 alice
ZADD board 3 bob
ZRANGE board 0 -1   -> [bob, alice]   (bob's score 3 < alice's 5)
```

#### `ZSCORE key member`

Returns the score of `member` as a string, or null if it is not a member.

```
ZSCORE board alice  -> "5"
ZSCORE board nobody -> (nil)
```

## 5. Errors you may encounter

| Reply                                          | Cause                                          |
| ---------------------------------------------- | ---------------------------------------------- |
| `-ERR unknown command '...'`                   | The command name is not recognised.            |
| `-ERR wrong number of arguments for '...'`     | Too few/many arguments for the command.        |
| `-ERR value '...' is not a valid integer`      | A numeric argument (TTL/index) was not a number.|
| `-ERR value '...' is not a valid float`        | A `ZADD` score was not a number.               |
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
