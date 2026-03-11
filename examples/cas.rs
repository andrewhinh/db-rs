//! CAS example: increment loop using compare-and-set with retry on :0.
//!
//! Run server: cargo run --bin db-rs-server
//! Then: cargo run --example cas

#![warn(rust_2018_idioms)]

use bytes::Bytes;
use db_rs::{Result, clients::Client};

#[tokio::main]
async fn main() -> Result<()> {
    let mut client = Client::connect("127.0.0.1:6379").await?;

    client.set("counter", "0".into()).await?;

    for _ in 0..20 {
        loop {
            let old = client
                .get("counter")
                .await?
                .unwrap_or_else(|| Bytes::from("0"));
            let n: u32 = std::str::from_utf8(&old)
                .unwrap_or("0")
                .parse()
                .unwrap_or(0);
            let new = Bytes::from((n + 1).to_string());

            if client.cas("counter", Some(old), new).await? {
                break;
            }
        }
    }

    let val = client.get("counter").await?.unwrap();
    println!("counter = {}", std::str::from_utf8(&val).unwrap());

    Ok(())
}
