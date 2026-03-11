use std::net::SocketAddr;

use db_rs::server;
use tokio::net::TcpListener;
use tokio::task::JoinHandle;

pub async fn start_server() -> (SocketAddr, JoinHandle<()>) {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let handle = tokio::spawn(async move { server::run(listener, tokio::signal::ctrl_c()).await });
    (addr, handle)
}
