//! EXPIRE/TTL/PTTL example.
//!
//! You can test this out by running:
//!
//!     cargo run --bin db-rs-server
//!
//! And then in another terminal run:
//!
//!     cargo run --example ttl

#![warn(rust_2018_idioms)]

use db_rs::{Result, clients::Client};

#[tokio::main]
pub async fn main() -> Result<()> {
    let mut client = Client::connect("127.0.0.1:6379").await?;

    client.set("session", "value".into()).await?;

    let ttl = client.ttl("session").await?;
    println!("ttl before expire: {ttl}");

    let updated = client.expire("session", 5).await?;
    println!("expire updated: {updated}");

    let ttl = client.ttl("session").await?;
    let pttl = client.pttl("session").await?;
    println!("ttl after expire: {ttl}s");
    println!("pttl after expire: {pttl}ms");

    Ok(())
}
