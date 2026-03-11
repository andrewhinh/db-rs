//! Subscribe to the ordered change stream.
//!
//! Connects to a db-rs server, subscribes to __changes__, and prints
//! change events (offset, op, key) as mutations occur.
//!
//! Run with:
//!
//!     cargo run --bin db-rs-server
//!
//! Then:
//!
//!     cargo run --example change_stream

#![warn(rust_2018_idioms)]

use std::io::Cursor;

use db_rs::{Frame, Result, clients::Client};
use tokio::time::Duration;

fn parse_change(content: &[u8]) -> Option<(i64, String, String)> {
    let mut cur = Cursor::new(content);
    let frame = Frame::parse(&mut cur).ok()?;
    let Frame::Array(arr) = frame else {
        return None;
    };
    let offset = match &arr[0] {
        Frame::Integer(n) => *n,
        _ => return None,
    };
    let op = match &arr[1] {
        Frame::Simple(s) => s.clone(),
        _ => return None,
    };
    let key = match &arr[2] {
        Frame::Bulk(b) => String::from_utf8_lossy(b).into_owned(),
        _ => return None,
    };
    Some((offset, op, key))
}

#[tokio::main]
async fn main() -> Result<()> {
    let mut subscriber = Client::connect("127.0.0.1:6379")
        .await?
        .subscribe(vec!["__changes__".into()])
        .await?;

    tokio::spawn(async {
        let mut client = Client::connect("127.0.0.1:6379").await.unwrap();
        tokio::time::sleep(Duration::from_millis(200)).await;
        client.set("a", "1".into()).await.unwrap();
        tokio::time::sleep(Duration::from_millis(100)).await;
        client.set("b", "2".into()).await.unwrap();
        tokio::time::sleep(Duration::from_millis(100)).await;
        client.del(&["a".into()]).await.unwrap();
    });

    for _ in 0..3 {
        if let Some(msg) = subscriber.next_message().await? {
            if let Some((offset, op, key)) = parse_change(&msg.content) {
                println!("{} {} {}", offset, op, key);
            }
        }
    }

    Ok(())
}
