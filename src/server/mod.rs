//! The TCP front end. It accepts connections and, for each one, reads inline
//! command lines, hands them to [`command::handle`], and writes the reply back.
//!
//! Every fallible I/O operation is handled rather than unwrapped: a misbehaving
//! or disconnecting client ends only its own task, and a failed `accept` is
//! logged without bringing the whole server down.

use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};
use tokio::net::{TcpListener, TcpStream};
use tokio::spawn;

use crate::command;
use crate::database::db::Database;

/// Binds to `address` and serves clients until the process is stopped. Returns
/// an error only if the initial bind fails.
pub async fn run(address: &str) -> std::io::Result<()> {
    let listener = TcpListener::bind(address).await?;
    let db = Database::new();
    eprintln!("Redis clone listening on {address}");

    loop {
        let (socket, peer) = match listener.accept().await {
            Ok(connection) => connection,
            Err(error) => {
                eprintln!("failed to accept connection: {error}");
                continue;
            }
        };

        let db = db.clone();
        spawn(async move {
            if let Err(error) = handle_connection(socket, db).await {
                eprintln!("connection {peer} ended: {error}");
            }
        });
    }
}

/// Serves a single client: one command per line until the peer disconnects.
async fn handle_connection(socket: TcpStream, db: Database) -> std::io::Result<()> {
    let (reader, mut writer) = socket.into_split();
    let mut reader = BufReader::new(reader);
    let mut line = String::new();

    loop {
        line.clear();
        // `read_line` fails on invalid UTF-8; propagating ends this task only.
        if reader.read_line(&mut line).await? == 0 {
            return Ok(()); // clean EOF: the client closed the connection
        }

        let response = command::handle(&line, &db).await;
        if !response.is_empty() {
            writer.write_all(response.as_bytes()).await?;
        }
    }
}
