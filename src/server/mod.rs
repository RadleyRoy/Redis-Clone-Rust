//! The TCP front end. It accepts connections and, for each one, reads requests,
//! hands the tokens to [`command::dispatch`], and writes the reply back.
//!
//! Two request framings are supported, chosen per line by the first byte:
//!   * **inline** — a whitespace-separated command line (telnet-friendly);
//!   * **RESP arrays** — `*N` multi-bulk requests as sent by `redis-cli`, which
//!     allow arguments that contain spaces.
//!
//! Every fallible I/O operation is handled rather than unwrapped: a misbehaving
//! or disconnecting client ends only its own task, and a failed `accept` is
//! logged without bringing the whole server down.

use std::io::{Error, ErrorKind, Result};
use std::path::PathBuf;
use std::time::Duration;

use tokio::io::{AsyncBufRead, AsyncBufReadExt, AsyncReadExt, AsyncWriteExt, BufReader};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::mpsc;
use tokio::{select, signal, spawn};
use tracing::{debug, info, warn};

use crate::command;
use crate::config::Config;
use crate::database::aof::{Aof, FsyncPolicy};
use crate::database::db::Database;
use crate::session::Session;

/// Largest number of arguments accepted in a single RESP array request.
const MAX_MULTIBULK: usize = 1024 * 1024;
/// Largest byte length accepted for a single RESP bulk-string argument.
const MAX_BULK_BYTES: usize = 512 * 1024 * 1024;

/// Binds to the configured address and serves clients until interrupted with
/// Ctrl+C. Returns an error only if the initial bind fails.
pub async fn run(config: &Config) -> Result<()> {
    let listener = TcpListener::bind(config.address()).await?;
    info!(address = %listener.local_addr()?, "redis_clone listening");

    let snapshot_path = PathBuf::from(&config.snapshot_file);
    let (db, aof_policy) = init_persistence(config, snapshot_path).await?;

    spawn_sweeper(db.clone(), config.sweep_interval());
    if aof_policy == Some(FsyncPolicy::EverySec) {
        spawn_aof_flusher(db.clone());
    }

    let result = accept_loop(listener, db.clone()).await;

    // Persist on a clean shutdown: a final snapshot, and a final AOF flush.
    match db.save().await {
        Ok(()) => info!("snapshot saved on shutdown"),
        Err(cause) => warn!(%cause, "failed to save snapshot on shutdown"),
    }
    if let Err(cause) = db.sync_aof().await {
        warn!(%cause, "failed to sync AOF on shutdown");
    }
    result
}

/// Builds the database with the configured persistence backend and recovers
/// prior state: the AOF (replayed) when append-only mode is on, otherwise the
/// snapshot (loaded). Returns the database and the AOF's fsync policy, if any.
async fn init_persistence(
    config: &Config,
    snapshot_path: PathBuf,
) -> Result<(Database, Option<FsyncPolicy>)> {
    if config.appendonly {
        let policy: FsyncPolicy = config
            .appendfsync
            .parse()
            .map_err(|cause| Error::new(ErrorKind::InvalidInput, cause))?;
        let aof_path = PathBuf::from(&config.aof_file);
        let aof = Aof::open(&aof_path, policy).await?;
        let db = Database::with_persistence(Some(snapshot_path), Some(aof));

        match replay_aof(&aof_path, &db).await {
            Ok(count) if count > 0 => {
                info!(commands = count, path = %aof_path.display(), "replayed AOF")
            }
            Ok(_) => {}
            Err(cause) => warn!(%cause, path = %aof_path.display(), "failed to replay AOF"),
        }
        Ok((db, Some(policy)))
    } else {
        let db = Database::with_persistence(Some(snapshot_path.clone()), None);
        match db.load_from(&snapshot_path).await {
            Ok(true) => info!(
                path = %snapshot_path.display(),
                keys = db.dbsize().await,
                "loaded snapshot"
            ),
            Ok(false) => {}
            Err(cause) => warn!(%cause, path = %snapshot_path.display(), "failed to load snapshot"),
        }
        Ok((db, None))
    }
}

/// Replays every command in the AOF into `db`, suppressing re-logging while it
/// runs. Returns the number of commands applied.
async fn replay_aof(path: &std::path::Path, db: &Database) -> Result<u64> {
    let bytes = match tokio::fs::read(path).await {
        Ok(bytes) => bytes,
        Err(cause) if cause.kind() == ErrorKind::NotFound => return Ok(0),
        Err(cause) => return Err(cause),
    };

    let mut reader = BufReader::new(&bytes[..]);
    db.begin_replay();
    let mut count = 0;
    // Drive the loop by hand rather than with `?` so a malformed trailing entry
    // (a write interrupted by a crash) does not skip `end_replay`: that would
    // leave the replay flag stuck on and silently drop every future AOF append.
    // A truncated tail is tolerated, as Redis does, by stopping replay there.
    loop {
        match read_request(&mut reader).await {
            Ok(Some(tokens)) => {
                let _ = command::dispatch(&tokens, db).await;
                count += 1;
            }
            Ok(None) => break,
            Err(cause) => {
                warn!(%cause, "stopped AOF replay at a malformed entry");
                break;
            }
        }
    }
    db.end_replay();
    Ok(count)
}

/// Periodically flushes the AOF to disk (the `everysec` policy).
fn spawn_aof_flusher(db: Database) {
    spawn(async move {
        let mut ticker = tokio::time::interval(Duration::from_secs(1));
        ticker.tick().await; // the first tick fires immediately; skip it
        loop {
            ticker.tick().await;
            if let Err(cause) = db.sync_aof().await {
                warn!(%cause, "AOF flush failed");
            }
        }
    });
}

/// Periodically evicts expired keys in the background, complementing the lazy
/// eviction that happens on access.
fn spawn_sweeper(db: Database, interval: Duration) {
    spawn(async move {
        let mut ticker = tokio::time::interval(interval);
        ticker.tick().await; // the first tick fires immediately; skip it
        loop {
            ticker.tick().await;
            let removed = db.sweep_expired().await;
            if removed > 0 {
                debug!(removed, "swept expired keys");
            }
        }
    });
}

/// Accepts connections until Ctrl+C, spawning a task to serve each one. On the
/// shutdown signal it stops accepting and returns; in-flight connection tasks
/// are left to finish on their own.
async fn accept_loop(listener: TcpListener, db: Database) -> Result<()> {
    loop {
        let (socket, peer) = select! {
            result = listener.accept() => match result {
                Ok(connection) => connection,
                Err(cause) => {
                    warn!(%cause, "failed to accept connection");
                    continue;
                }
            },
            _ = signal::ctrl_c() => {
                info!("shutdown signal received; no longer accepting connections");
                return Ok(());
            }
        };

        let db = db.clone();
        spawn(async move {
            let clients = db.client_connected();
            debug!(%peer, clients, "client connected");
            if let Err(cause) = handle_connection(socket, db.clone()).await {
                debug!(%peer, %cause, "connection ended");
            }
            db.client_disconnected();
        });
    }
}

/// Serves a single client until it disconnects.
///
/// Requests are read in a dedicated task and delivered over a channel, so the
/// main loop can wait on *either* the next request *or* a pub/sub message
/// without a message ever cancelling a half-finished read.
async fn handle_connection(socket: TcpStream, db: Database) -> Result<()> {
    let (reader, mut writer) = socket.into_split();
    let (mut session, mut mailbox) = Session::new(db);

    let (requests_tx, mut requests) = mpsc::channel::<Vec<String>>(16);
    let reader_task = spawn(async move {
        let mut reader = BufReader::new(reader);
        while let Ok(Some(tokens)) = read_request(&mut reader).await {
            if requests_tx.send(tokens).await.is_err() {
                break;
            }
        }
    });

    // A write error must not skip cleanup: it ends the loop with the error
    // instead of returning early, so the session's pub/sub subscriptions are
    // always torn down and the reader task is always aborted.
    let result = loop {
        select! {
            request = requests.recv() => match request {
                Some(tokens) => {
                    let reply = session.handle(&tokens).await;
                    if !reply.is_empty()
                        && let Err(cause) = writer.write_all(reply.as_bytes()).await
                    {
                        break Err(cause);
                    }
                }
                None => break Ok(()), // the reader task ended (EOF or protocol error)
            },
            Some(message) = mailbox.recv() => {
                if let Err(cause) = writer.write_all(message.as_bytes()).await {
                    break Err(cause);
                }
            }
        }
    };

    session.cleanup();
    reader_task.abort();
    result
}

/// Reads one request as a list of tokens, choosing the framing from the first
/// byte. Returns `Ok(None)` at end of stream. A framing violation is an
/// `InvalidData` error, which closes the connection; malformed *commands* (as
/// opposed to malformed framing) are handled later and merely produce an error
/// reply.
async fn read_request<R>(reader: &mut R) -> Result<Option<Vec<String>>>
where
    R: AsyncBufRead + Unpin,
{
    let mut line = String::new();
    if reader.read_line(&mut line).await? == 0 {
        return Ok(None); // clean EOF
    }

    let line = line.trim_end();
    match line.strip_prefix('*') {
        Some(count) => read_multibulk(reader, count).await.map(Some),
        None => Ok(Some(line.split_whitespace().map(str::to_string).collect())),
    }
}

/// Reads the `count` bulk-string arguments of a RESP array whose `*<count>`
/// header line has already been consumed.
async fn read_multibulk<R>(reader: &mut R, count: &str) -> Result<Vec<String>>
where
    R: AsyncBufRead + Unpin,
{
    let count: usize = count
        .parse()
        .map_err(|_| protocol_error("invalid multibulk length"))?;
    if count > MAX_MULTIBULK {
        return Err(protocol_error("multibulk length out of range"));
    }

    // Reserve modestly rather than trusting `count`, which is attacker-supplied.
    let mut tokens = Vec::with_capacity(count.min(16));
    for _ in 0..count {
        tokens.push(read_bulk_string(reader).await?);
    }
    Ok(tokens)
}

/// Reads a single `$<len>\r\n<bytes>\r\n` bulk string.
async fn read_bulk_string<R>(reader: &mut R) -> Result<String>
where
    R: AsyncBufRead + Unpin,
{
    let mut header = String::new();
    if reader.read_line(&mut header).await? == 0 {
        return Err(protocol_error("unexpected end of input"));
    }
    let length: usize = header
        .trim_end()
        .strip_prefix('$')
        .ok_or_else(|| protocol_error("expected a bulk string"))?
        .parse()
        .map_err(|_| protocol_error("invalid bulk length"))?;
    if length > MAX_BULK_BYTES {
        return Err(protocol_error("bulk length out of range"));
    }

    // `length` data bytes followed by a trailing CRLF.
    let mut buffer = vec![0u8; length + 2];
    reader.read_exact(&mut buffer).await?;
    buffer.truncate(length);
    String::from_utf8(buffer).map_err(|_| protocol_error("invalid UTF-8 in bulk string"))
}

fn protocol_error(message: &str) -> Error {
    Error::new(ErrorKind::InvalidData, format!("protocol error: {message}"))
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::net::SocketAddr;
    use tokio::net::TcpStream;
    use tokio::net::tcp::OwnedReadHalf;

    /// Starts the server on an ephemeral port and returns its address.
    async fn start_server() -> SocketAddr {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        spawn(accept_loop(listener, Database::new()));
        address
    }

    async fn next_line(reader: &mut BufReader<OwnedReadHalf>) -> String {
        let mut buffer = String::new();
        reader.read_line(&mut buffer).await.unwrap();
        buffer
    }

    #[tokio::test]
    async fn truncated_aof_replay_still_allows_future_writes_to_be_logged() {
        // A valid `SET k1 v1` followed by an entry truncated mid-bulk-string,
        // as a crash during an append would leave behind.
        let unique = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        let path = std::env::temp_dir().join(format!("redis_clone_replay_{unique}.aof"));
        std::fs::write(
            &path,
            b"*3\r\n$3\r\nSET\r\n$2\r\nk1\r\n$2\r\nv1\r\n*3\r\n$3\r\nSET\r\n$2\r\nk2\r\n$5\r\nval",
        )
        .unwrap();

        let aof = Aof::open(&path, FsyncPolicy::Always).await.unwrap();
        let db = Database::with_persistence(None, Some(aof));
        replay_aof(&path, &db).await.unwrap();

        // The valid entry was applied, and replay stopped at the truncation.
        assert_eq!(db.get("k1").await.unwrap(), Some("v1".to_string()));

        // The replay flag must have been cleared despite the error, so a new
        // write is still appended to the AOF (the bug left it stuck on).
        db.log_write(&["SET".to_string(), "k3".to_string(), "v3".to_string()])
            .await;
        db.sync_aof().await.unwrap();
        let contents = std::fs::read_to_string(&path).unwrap();
        assert!(
            contents.contains("k3"),
            "new write was not logged: {contents:?}"
        );

        let _ = std::fs::remove_file(&path);
    }

    #[tokio::test]
    async fn serves_inline_and_resp_requests_over_tcp() {
        let address = start_server().await;
        let stream = TcpStream::connect(address).await.unwrap();
        let (reader, mut writer) = stream.into_split();
        let mut reader = BufReader::new(reader);

        // Inline request.
        writer.write_all(b"SET greeting hello\r\n").await.unwrap();
        assert_eq!(next_line(&mut reader).await, "+OK\r\n");

        // RESP array request whose value contains a space (impossible inline).
        writer
            .write_all(b"*3\r\n$3\r\nSET\r\n$4\r\nnote\r\n$11\r\nhello world\r\n")
            .await
            .unwrap();
        assert_eq!(next_line(&mut reader).await, "+OK\r\n");

        // The space-containing value round-trips.
        writer.write_all(b"GET note\r\n").await.unwrap();
        assert_eq!(next_line(&mut reader).await, "$11\r\n");
        assert_eq!(next_line(&mut reader).await, "hello world\r\n");
    }

    #[tokio::test]
    async fn multibulk_length_out_of_range_is_rejected() {
        let mut input = BufReader::new(&b"*99999999999999\r\n"[..]);
        assert!(read_request(&mut input).await.is_err());
    }

    #[tokio::test]
    async fn transaction_queues_and_execs() {
        let address = start_server().await;
        let stream = TcpStream::connect(address).await.unwrap();
        let (reader, mut writer) = stream.into_split();
        let mut reader = BufReader::new(reader);

        writer.write_all(b"MULTI\r\n").await.unwrap();
        assert_eq!(next_line(&mut reader).await, "+OK\r\n");
        writer.write_all(b"SET k 1\r\n").await.unwrap();
        assert_eq!(next_line(&mut reader).await, "+QUEUED\r\n");
        writer.write_all(b"INCR k\r\n").await.unwrap();
        assert_eq!(next_line(&mut reader).await, "+QUEUED\r\n");

        // EXEC returns an array of each queued command's reply.
        writer.write_all(b"EXEC\r\n").await.unwrap();
        assert_eq!(next_line(&mut reader).await, "*2\r\n");
        assert_eq!(next_line(&mut reader).await, "+OK\r\n");
        assert_eq!(next_line(&mut reader).await, ":2\r\n");
    }

    #[tokio::test]
    async fn subscribe_inside_multi_aborts_the_transaction() {
        let address = start_server().await;
        let stream = TcpStream::connect(address).await.unwrap();
        let (reader, mut writer) = stream.into_split();
        let mut reader = BufReader::new(reader);

        writer.write_all(b"MULTI\r\n").await.unwrap();
        assert_eq!(next_line(&mut reader).await, "+OK\r\n");
        writer.write_all(b"SET a 1\r\n").await.unwrap();
        assert_eq!(next_line(&mut reader).await, "+QUEUED\r\n");
        // SUBSCRIBE cannot be queued; it is rejected and dirties the transaction.
        writer.write_all(b"SUBSCRIBE ch\r\n").await.unwrap();
        assert!(next_line(&mut reader).await.starts_with("-ERR"));
        // EXEC therefore aborts rather than half-applying the queue.
        writer.write_all(b"EXEC\r\n").await.unwrap();
        assert!(next_line(&mut reader).await.starts_with("-EXECABORT"));
    }

    #[tokio::test]
    async fn multi_is_rejected_while_subscribed() {
        let address = start_server().await;
        let stream = TcpStream::connect(address).await.unwrap();
        let (reader, mut writer) = stream.into_split();
        let mut reader = BufReader::new(reader);

        writer.write_all(b"SUBSCRIBE ch\r\n").await.unwrap();
        // Drain the subscribe confirmation: *3 / $9 subscribe / $2 ch / :1.
        assert_eq!(next_line(&mut reader).await, "*3\r\n");
        for _ in 0..5 {
            next_line(&mut reader).await;
        }
        // MULTI is not one of the commands allowed in subscribe mode.
        writer.write_all(b"MULTI\r\n").await.unwrap();
        assert!(next_line(&mut reader).await.starts_with("-ERR"));
    }

    #[tokio::test]
    async fn exec_without_multi_is_an_error() {
        let address = start_server().await;
        let stream = TcpStream::connect(address).await.unwrap();
        let (reader, mut writer) = stream.into_split();
        let mut reader = BufReader::new(reader);

        writer.write_all(b"EXEC\r\n").await.unwrap();
        assert!(
            next_line(&mut reader)
                .await
                .starts_with("-ERR EXEC without MULTI")
        );
    }

    #[tokio::test]
    async fn publish_reaches_a_subscriber() {
        let address = start_server().await;

        // Subscriber connection.
        let sub = TcpStream::connect(address).await.unwrap();
        let (sub_reader, mut sub_writer) = sub.into_split();
        let mut sub_reader = BufReader::new(sub_reader);
        sub_writer.write_all(b"SUBSCRIBE news\r\n").await.unwrap();
        // Confirmation: *3 / $9 subscribe / $4 news / :1
        assert_eq!(next_line(&mut sub_reader).await, "*3\r\n");
        assert_eq!(next_line(&mut sub_reader).await, "$9\r\n");
        assert_eq!(next_line(&mut sub_reader).await, "subscribe\r\n");
        assert_eq!(next_line(&mut sub_reader).await, "$4\r\n");
        assert_eq!(next_line(&mut sub_reader).await, "news\r\n");
        assert_eq!(next_line(&mut sub_reader).await, ":1\r\n");

        // Publisher connection.
        let pub_stream = TcpStream::connect(address).await.unwrap();
        let (pub_reader, mut pub_writer) = pub_stream.into_split();
        let mut pub_reader = BufReader::new(pub_reader);
        pub_writer
            .write_all(b"PUBLISH news hello\r\n")
            .await
            .unwrap();
        assert_eq!(next_line(&mut pub_reader).await, ":1\r\n"); // one subscriber

        // The subscriber receives the message: *3 / $7 message / $4 news / $5 hello
        assert_eq!(next_line(&mut sub_reader).await, "*3\r\n");
        assert_eq!(next_line(&mut sub_reader).await, "$7\r\n");
        assert_eq!(next_line(&mut sub_reader).await, "message\r\n");
        assert_eq!(next_line(&mut sub_reader).await, "$4\r\n");
        assert_eq!(next_line(&mut sub_reader).await, "news\r\n");
        assert_eq!(next_line(&mut sub_reader).await, "$5\r\n");
        assert_eq!(next_line(&mut sub_reader).await, "hello\r\n");
    }
}
