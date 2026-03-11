//! AOF replay example.
//!
//! This example starts an in-process server with `--aof-path` equivalent
//! config, writes data, restarts, then prints restored value and ttl.
//!
//! Run with:
//!
//!     cargo run --example aof_replay

#![warn(rust_2018_idioms)]

mod common;

use db_rs::{Result, clients::Client, server::ServerConfig};

#[tokio::main]
pub async fn main() -> Result<()> {
    let aof_path = common::temp_path("db-rs-replay", "aof");

    {
        let server = common::start_server(ServerConfig {
            aof_path: Some(aof_path.clone()),
            snapshot_path: None,
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
            aof_path: Some(aof_path.clone()),
            snapshot_path: None,
            ..Default::default()
        })
        .await?;
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
