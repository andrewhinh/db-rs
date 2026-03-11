#![warn(rust_2018_idioms)]

use std::time::{SystemTime, UNIX_EPOCH};

use db_rs::{
    Result,
    clients::Client,
    server::{self, ServerConfig},
};
use tokio::net::TcpListener;
use tokio::sync::oneshot;

#[tokio::main]
pub async fn main() -> Result<()> {
    let aof_path = std::env::temp_dir().join(format!("db-rs-replay-{}.aof", unix_nanos()));

    {
        let server = start_server(aof_path.clone()).await?;
        let mut client = Client::connect(server.addr).await?;
        client.set("user:1", "alice".into()).await?;
        let _ = client.expire("user:1", 120).await?;
        server.shutdown().await?;
    }

    {
        let server = start_server(aof_path.clone()).await?;
        let mut client = Client::connect(server.addr).await?;
        let value = client.get("user:1").await?;
        let ttl = client.ttl("user:1").await?;
        println!("replayed value: {value:?}");
        println!("replayed ttl: {ttl}");
        server.shutdown().await?;
    }

    let _ = tokio::fs::remove_file(aof_path).await;
    Ok(())
}

struct ExampleServer {
    addr: std::net::SocketAddr,
    shutdown_tx: oneshot::Sender<()>,
    handle: tokio::task::JoinHandle<Result<()>>,
}

impl ExampleServer {
    async fn shutdown(self) -> Result<()> {
        let _ = self.shutdown_tx.send(());
        self.handle.await??;
        Ok(())
    }
}

async fn start_server(aof_path: std::path::PathBuf) -> Result<ExampleServer> {
    let listener = TcpListener::bind("127.0.0.1:0").await?;
    let addr = listener.local_addr()?;
    let (shutdown_tx, shutdown_rx) = oneshot::channel::<()>();

    let config = ServerConfig {
        aof_path: Some(aof_path),
    };

    let handle = tokio::spawn(async move {
        server::run_with_config(
            listener,
            async move {
                let _ = shutdown_rx.await;
            },
            config,
        )
        .await
    });

    Ok(ExampleServer {
        addr,
        shutdown_tx,
        handle,
    })
}

fn unix_nanos() -> u128 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_nanos()
}
