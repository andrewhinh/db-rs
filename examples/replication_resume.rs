//! Resume from persisted offset demo.
//!
//! Use mprocs replication_resume or:
//! Leader: cargo run --bin db-rs-server -- --port 6379
//! Follower: cargo run --bin db-rs-server -- --port 6381 --replicaof
//! 127.0.0.1:6379 --repl-offset-path /tmp/db-rs-repl-offset Then run this
//! example. Restart the follower; state is preserved via persisted offset.

#![warn(rust_2018_idioms)]

use db_rs::Result;
use db_rs::clients::Client;

#[tokio::main]
async fn main() -> Result<()> {
    let mut follower = Client::connect("127.0.0.1:6381").await?;

    let keys = ["demo", "resume", "state"];
    for k in keys {
        let v = follower.get(k).await?;
        println!(
            "GET {} = {:?}",
            k,
            v.as_deref().map(|b| String::from_utf8_lossy(b))
        );
    }

    Ok(())
}
