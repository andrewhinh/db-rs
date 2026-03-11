use std::net::SocketAddr;

use db_rs::server;
use tokio::net::TcpListener;
use tokio::task::JoinHandle;

pub async fn start_server() -> (SocketAddr, JoinHandle<()>) {
    start_server_with_config(server::ServerConfig::default()).await
}

pub async fn start_server_with_config(
    config: server::ServerConfig,
) -> (SocketAddr, JoinHandle<()>) {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let handle = tokio::spawn(async move {
        let _ = server::run_with_config(listener, std::future::pending::<()>(), config).await;
    });
    (addr, handle)
}
