//! Bootstrap CDC: snapshot then tail.
//!
//! Connects, fetches current offset, subscribes to __changes__@boot,
//! prints snapshot entries then live changes.
//!
//! Run server: cargo run --bin db-rs-server
//! Then: cargo run --example change_stream_bootstrap

#![warn(rust_2018_idioms)]

use std::io::Cursor;

use db_rs::{Frame, Result, clients::Client};

fn parse_change(content: &[u8]) -> Option<(i64, String, String, i64)> {
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
    let ts_ms = arr
        .get(4)
        .and_then(|f| match f {
            Frame::Integer(n) => Some(*n),
            _ => None,
        })
        .unwrap_or(0);
    Some((offset, op, key, ts_ms))
}

#[tokio::main]
async fn main() -> Result<()> {
    let mut client = Client::connect("127.0.0.1:6379").await?;
    let offset = client.offset().await?;
    println!("current offset: {}", offset);

    let mut subscriber = Client::connect("127.0.0.1:6379")
        .await?
        .subscribe(vec!["__changes__@boot".into()])
        .await?;

    println!("snapshot then tail (Ctrl+C to stop):");
    loop {
        if let Some(msg) = subscriber.next_message().await? {
            if let Some((off, op, key, _ts)) = parse_change(&msg.content) {
                println!("{} {} {}", off, op, key);
            }
        }
    }
}
