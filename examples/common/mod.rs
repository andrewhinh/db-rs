use std::net::SocketAddr;
use std::path::PathBuf;
use std::time::{SystemTime, UNIX_EPOCH};

use db_rs::{
    Result,
    server::{self, ServerConfig},
};
use tokio::net::TcpListener;
use tokio::sync::oneshot;

pub struct ExampleServer {
    pub addr: SocketAddr,
    shutdown_tx: oneshot::Sender<()>,
    handle: tokio::task::JoinHandle<Result<()>>,
}

impl ExampleServer {
    pub async fn shutdown(self) -> Result<()> {
        let _ = self.shutdown_tx.send(());
        self.handle.await??;
        Ok(())
    }
}

pub async fn start_server(config: ServerConfig) -> Result<ExampleServer> {
    let listener = TcpListener::bind("127.0.0.1:0").await?;
    let addr = listener.local_addr()?;
    let (shutdown_tx, shutdown_rx) = oneshot::channel::<()>();

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

pub fn temp_path(prefix: &str, ext: &str) -> PathBuf {
    std::env::temp_dir().join(format!("{prefix}-{}.{}", unix_nanos(), ext))
}

fn unix_nanos() -> u128 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_nanos()
}
