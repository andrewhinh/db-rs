//! AOF append example.
//!
//! This example starts an in-process server with `--aof-path` equivalent
//! config, issues mutating and non-mutating commands, then prints the AOF.
//!
//! Run with:
//!
//!     cargo run --example aof_append

#![warn(rust_2018_idioms)]

use std::time::{SystemTime, UNIX_EPOCH};

use db_rs::{
    Result,
    clients::Client,
    server::{self, ServerConfig},
};
use tokio::net::TcpListener;
use tokio::sync::oneshot;
use tokio::time::{Duration, sleep};

#[tokio::main]
pub async fn main() -> Result<()> {
    let aof_path = std::env::temp_dir().join(format!("db-rs-example-{}.aof", unix_nanos()));
    let listener = TcpListener::bind("127.0.0.1:0").await?;
    let addr = listener.local_addr()?;
    let (shutdown_tx, shutdown_rx) = oneshot::channel::<()>();

    let config = ServerConfig {
        aof_path: Some(aof_path.clone()),
    };
    let server_task = tokio::spawn(async move {
        server::run_with_config(
            listener,
            async move {
                let _ = shutdown_rx.await;
            },
            config,
        )
        .await
    });

    let mut client = Client::connect(addr).await?;
    let _ = client.ping(None).await?;
    client.set("k", "v".into()).await?;
    let _ = client.get("k").await?;
    let _ = client.expire("k", 60).await?;
    let _ = client.del(&["k".into()]).await?;

    sleep(Duration::from_millis(20)).await;

    let aof = tokio::fs::read_to_string(&aof_path).await?;
    println!("aof_path: {}", aof_path.display());
    println!("aof_contents:\n{aof}");

    let _ = shutdown_tx.send(());
    server_task.await??;

    let _ = tokio::fs::remove_file(&aof_path).await;
    Ok(())
}

fn unix_nanos() -> u128 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_nanos()
}
