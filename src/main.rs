use self::server::create_server;

mod command;
mod database;
mod server;

#[tokio::main]
async fn main() 
{
    let add = "127.0.0.1:7335";
    eprintln!("Starting server at {:?}", add);
    create_server(add).await;
}
