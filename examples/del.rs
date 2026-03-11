//! DEL example.
//!
//! You can test this out by running:
//!
//!     cargo run --bin db-rs-server
//!
//! And then in another terminal run:
//!
//!     cargo run --example del

#![warn(rust_2018_idioms)]

use db_rs::{Result, clients::Client};

#[tokio::main]
pub async fn main() -> Result<()> {
    let mut client = Client::connect("127.0.0.1:6379").await?;

    client.set("alpha", "1".into()).await?;
    client.set("beta", "2".into()).await?;

    let deleted = client
        .del(&["alpha".into(), "missing".into(), "alpha".into()])
        .await?;

    println!("deleted keys: {deleted}");

    Ok(())
}
