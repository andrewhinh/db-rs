//! EXISTS example.
//!
//! You can test this out by running:
//!
//!     cargo run --bin db-rs-server
//!
//! And then in another terminal run:
//!
//!     cargo run --example exists

#![warn(rust_2018_idioms)]

use db_rs::{Result, clients::Client};

#[tokio::main]
pub async fn main() -> Result<()> {
    let mut client = Client::connect("127.0.0.1:6379").await?;

    client.set("alpha", "1".into()).await?;
    client.set("beta", "2".into()).await?;

    let count = client
        .exists(&["alpha".into(), "beta".into(), "missing".into()])
        .await?;

    println!("existing keys: {count}");

    Ok(())
}
