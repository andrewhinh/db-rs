//! AOF append example.
//!
//! This example starts an in-process server with `--aof-path` equivalent
//! config, issues mutating and non-mutating commands, then prints the AOF.
//!
//! Run with:
//!
//!     cargo run --example aof_append

#![warn(rust_2018_idioms)]

mod common;

use db_rs::{Result, clients::Client, server::ServerConfig};
use tokio::time::{Duration, sleep};

#[tokio::main]
pub async fn main() -> Result<()> {
    let aof_path = common::temp_path("db-rs-example", "aof");

    let config = ServerConfig {
        aof_path: Some(aof_path.clone()),
        snapshot_path: None,
    };
    let server = common::start_server(config).await?;

    let mut client = Client::connect(server.addr).await?;
    let _ = client.ping(None).await?;
    client.set("k", "v".into()).await?;
    let _ = client.get("k").await?;
    let _ = client.expire("k", 60).await?;
    let _ = client.del(&["k".into()]).await?;

    sleep(Duration::from_millis(20)).await;

    let aof = tokio::fs::read_to_string(&aof_path).await?;
    println!("aof_path: {}", aof_path.display());
    println!("aof_contents:\n{aof}");

    server.shutdown().await?;

    let _ = tokio::fs::remove_file(&aof_path).await;
    Ok(())
}
