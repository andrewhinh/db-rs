//! Event-time tumbling windows with late-event corrections.
//!
//! Subscribes to __changes__, aggregates set ops by tumbling window
//! (event-time), emits corrections when late events arrive.
//!
//! Run server: cargo run --bin db-rs-server
//! Then: cargo run --example event_time_windows

#![warn(rust_2018_idioms)]

use db_rs::{Result, clients::Client, stream};
use tokio::time::Duration;

const WINDOW_MS: i64 = 1000;
const ALLOWED_LATENESS_MS: i64 = 200;

#[tokio::main]
async fn main() -> Result<()> {
    let mut subscriber = Client::connect("127.0.0.1:6379")
        .await?
        .subscribe(vec!["__changes__".into()])
        .await?;

    tokio::spawn(async {
        let mut client = Client::connect("127.0.0.1:6379").await.unwrap();
        tokio::time::sleep(Duration::from_millis(100)).await;
        client.set("a", "1".into()).await.unwrap();
        tokio::time::sleep(Duration::from_millis(100)).await;
        client.set("b", "2".into()).await.unwrap();
        tokio::time::sleep(Duration::from_millis(100)).await;
        client.set("c", "3".into()).await.unwrap();
        tokio::time::sleep(Duration::from_millis(1500)).await;
        client.set("d", "4".into()).await.unwrap();
    });

    let mut proc = stream::TumblingWindowProcessor::new(WINDOW_MS, ALLOWED_LATENESS_MS);
    for _ in 0..4 {
        if let Some(msg) = subscriber.next_message().await? {
            if let Some((_off, op, _key, _val, ts_ms)) = stream::parse_change(&msg.content) {
                for (window_end, count, is_correction) in proc.process(ts_ms, &op) {
                    let tag = if is_correction {
                        "correction"
                    } else {
                        "window"
                    };
                    println!("{} [{}] count={}", tag, window_end, count);
                }
            }
        }
    }

    Ok(())
}
