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
use std::time::Duration;

use tokio::io::{AsyncBufRead, AsyncBufReadExt, AsyncReadExt, AsyncWriteExt, BufReader};
use tokio::net::{TcpListener, TcpStream};
use tokio::{select, signal, spawn};
use tracing::{debug, info, warn};

use crate::command;
use crate::config::Config;
use crate::database::db::Database;

/// Largest number of arguments accepted in a single RESP array request.
const MAX_MULTIBULK: usize = 1024 * 1024;
/// Largest byte length accepted for a single RESP bulk-string argument.
const MAX_BULK_BYTES: usize = 512 * 1024 * 1024;

/// Binds to the configured address and serves clients until interrupted with
/// Ctrl+C. Returns an error only if the initial bind fails.
pub async fn run(config: &Config) -> Result<()> {
    let listener = TcpListener::bind(config.address()).await?;
    info!(address = %listener.local_addr()?, "redis_clone listening");

    let db = Database::new();
    spawn_sweeper(db.clone(), config.sweep_interval());
    accept_loop(listener, db).await
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

/// Serves a single client: one request per iteration until the peer disconnects.
async fn handle_connection(socket: TcpStream, db: Database) -> Result<()> {
    let (reader, mut writer) = socket.into_split();
    let mut reader = BufReader::new(reader);

    while let Some(tokens) = read_request(&mut reader).await? {
        let response = command::dispatch(&tokens, &db).await;
        if !response.is_empty() {
            writer.write_all(response.as_bytes()).await?;
        }
    }
    Ok(())
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
}
