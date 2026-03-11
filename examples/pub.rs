//! Publish to a redis channel example.
//!
//! A simple client that connects to a db-rs server and publishes a message on
//! `foo` once at least one subscriber is active.
//!
//! You can test this out by running:
//!
//!     cargo run --bin db-rs-server
//!
//! Then in another terminal run:
//!
//!     cargo run --example sub
//!
//! And then in another terminal run:
//!
//!     cargo run --example pub

#![warn(rust_2018_idioms)]

use std::time::Duration;

use db_rs::{Result, clients::Client};
use tokio::time::sleep;

#[tokio::main]
async fn main() -> Result<()> {
    // Open a connection to the db-rs address.
    let mut client = Client::connect("127.0.0.1:6379").await?;

    // Wait for at least one subscriber to avoid racing startup order.
    loop {
        let subscribers = client.publish("foo", "bar".into()).await?;
        if subscribers > 0 {
            println!("published to {subscribers} subscriber(s)");
            break;
        }
        sleep(Duration::from_millis(100)).await;
    }

    Ok(())
}
