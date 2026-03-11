//! Snapshot restore example.
//!
//! This example starts an in-process server with `--snapshot-path` equivalent
//! config, writes data, restarts, then prints restored value and ttl.
//!
//! Run with:
//!
//!     cargo run --example snapshot_restore

#![warn(rust_2018_idioms)]

mod common;

use db_rs::{Result, clients::Client, server::ServerConfig};

#[tokio::main]
pub async fn main() -> Result<()> {
    let snapshot_path = common::temp_path("db-rs-snapshot-restore", "snap");

    {
        let server = common::start_server(ServerConfig {
            aof_path: None,
            snapshot_path: Some(snapshot_path.clone()),
            ..Default::default()
        })
        .await?;
        let mut client = Client::connect(server.addr).await?;
        client.set("user:1", "alice".into()).await?;
        let _ = client.expire("user:1", 120).await?;
        server.shutdown().await?;
    }

    {
        let server = common::start_server(ServerConfig {
            aof_path: None,
            snapshot_path: Some(snapshot_path.clone()),
            ..Default::default()
        })
        .await?;
        let mut client = Client::connect(server.addr).await?;
        let value = client.get("user:1").await?;
        let ttl = client.ttl("user:1").await?;
        println!("restored value: {value:?}");
        println!("restored ttl: {ttl}");
        server.shutdown().await?;
    }

    let _ = tokio::fs::remove_file(snapshot_path).await;
    Ok(())
}
