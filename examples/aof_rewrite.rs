//! AOF rewrite example.
//!
//! Run with:
//!
//!     cargo run --example aof_rewrite

#![warn(rust_2018_idioms)]

mod common;

use db_rs::{Result, clients::Client, server::ServerConfig};

#[tokio::main]
pub async fn main() -> Result<()> {
    let aof_path = common::temp_path("db-rs-rewrite", "aof");

    {
        let server = common::start_server(ServerConfig {
            aof_path: Some(aof_path.clone()),
            snapshot_path: None,
            ..Default::default()
        })
        .await?;
        let mut client = Client::connect(server.addr).await?;

        client.set("live", "1".into()).await?;
        client.set("live", "2".into()).await?;
        client.set("gone", "x".into()).await?;
        let _ = client.del(&["gone".to_string()]).await?;

        server.shutdown().await?;
    }

    let compacted = tokio::fs::read_to_string(&aof_path).await?;
    println!("compacted_aof_path: {}", aof_path.display());
    println!("compacted_aof_contents:\n{compacted}");

    {
        let server = common::start_server(ServerConfig {
            aof_path: Some(aof_path.clone()),
            snapshot_path: None,
            ..Default::default()
        })
        .await?;
        let mut client = Client::connect(server.addr).await?;

        println!("live => {:?}", client.get("live").await?);
        println!("gone => {:?}", client.get("gone").await?);

        server.shutdown().await?;
    }

    let _ = tokio::fs::remove_file(&aof_path).await;
    Ok(())
}
