mod command;
mod database;
mod resp;
mod server;

use server::run;

/// Address the server listens on. Kept here as the single place to change it.
const LISTEN_ADDRESS: &str = "127.0.0.1:7335";

#[tokio::main]
async fn main() {
    if let Err(error) = run(LISTEN_ADDRESS).await {
        eprintln!("fatal: could not start server on {LISTEN_ADDRESS}: {error}");
        std::process::exit(1);
    }
}
