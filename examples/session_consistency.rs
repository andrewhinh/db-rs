//! Read-your-writes demo: write on leader, read from follower with GET
//! READOFFSET.
//!
//! Start: mprocs session_consistency (starts leader, follower, runs this)
//! Or: leader on 6379, follower on 6382 --replicaof 127.0.0.1:6379

#![warn(rust_2018_idioms)]

use db_rs::Result;
use db_rs::clients::Client;

#[tokio::main]
async fn main() -> Result<()> {
    let mut leader = Client::connect("127.0.0.1:6379").await?;
    let mut follower = Client::connect("127.0.0.1:6382").await?;

    leader.set("demo", "read-your-writes".into()).await?;
    let offset = leader.offset().await?;
    println!("SET demo (offset {})", offset);

    let v = follower.get_with_offset("demo", Some(offset)).await?;
    println!(
        "GET demo READOFFSET {} = {:?}",
        offset,
        v.as_deref().map(|b| String::from_utf8_lossy(b))
    );

    Ok(())
}
