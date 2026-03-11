//! Leader/follower replication demo.
//!
//! Start leader: cargo run --bin db-rs-server -- --port 6379
//! Start follower: cargo run --bin db-rs-server -- --port 6380 --replicaof
//! 127.0.0.1:6379 Then: SET on leader (port 6379), GET on follower (port 6380)

#![warn(rust_2018_idioms)]

use db_rs::Result;
use db_rs::clients::Client;

#[tokio::main]
async fn main() -> Result<()> {
    let mut leader = Client::connect("127.0.0.1:6379").await?;
    let mut follower = Client::connect("127.0.0.1:6380").await?;

    leader.set("demo", "replicated".into()).await?;
    println!("SET demo replicated (on leader)");

    tokio::time::sleep(tokio::time::Duration::from_millis(100)).await;
    let v = follower.get("demo").await?;
    println!(
        "GET demo = {:?} (on follower)",
        v.as_deref().map(|b| String::from_utf8_lossy(b))
    );

    Ok(())
}
