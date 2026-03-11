mod common;

use std::io::Cursor;

use db_rs::{Frame, clients::Client};
use tokio::time::{self, Duration};

fn parse_change_event(bytes: &[u8]) -> (i64, String, String, Option<Vec<u8>>) {
    let mut cur = Cursor::new(bytes);
    let frame = Frame::parse(&mut cur).unwrap();
    let Frame::Array(arr) = frame else {
        panic!("expected array");
    };
    let offset = match &arr[0] {
        Frame::Integer(n) => *n,
        _ => panic!("expected integer offset"),
    };
    let op = match &arr[1] {
        Frame::Simple(s) => s.clone(),
        _ => panic!("expected simple op"),
    };
    let key = match &arr[2] {
        Frame::Bulk(b) => String::from_utf8_lossy(b).into_owned(),
        _ => panic!("expected bulk key"),
    };
    let value = match &arr[3] {
        Frame::Null => None,
        Frame::Bulk(b) => Some(b.to_vec()),
        _ => panic!("expected null or bulk value"),
    };
    (offset, op, key, value)
}

#[tokio::test]
async fn cdc_emits_set_del_expire() {
    let (addr, _) = common::start_server().await;

    let mut subscriber = Client::connect(addr)
        .await
        .unwrap()
        .subscribe(vec!["__changes__".into()])
        .await
        .unwrap();

    let mut client = Client::connect(addr).await.unwrap();

    client.set("k1", "v1".into()).await.unwrap();
    let msg = subscriber.next_message().await.unwrap().unwrap();
    assert_eq!(msg.channel, "__changes__");
    let (_offset, op, key, value) = parse_change_event(&msg.content);
    assert_eq!(op, "set");
    assert_eq!(key, "k1");
    assert_eq!(value.as_deref(), Some(b"v1" as &[u8]));

    client.del(&["k1".into()]).await.unwrap();
    let msg = subscriber.next_message().await.unwrap().unwrap();
    let (_, op, key, value) = parse_change_event(&msg.content);
    assert_eq!(op, "del");
    assert_eq!(key, "k1");
    assert!(value.is_none());

    client.set("k2", "v2".into()).await.unwrap();
    let _ = subscriber.next_message().await.unwrap().unwrap();

    client.expire("k2", 10).await.unwrap();
    let msg = subscriber.next_message().await.unwrap().unwrap();
    let (_, op, key, _) = parse_change_event(&msg.content);
    assert_eq!(op, "expire");
    assert_eq!(key, "k2");
}

#[tokio::test]
async fn cdc_offsets_monotonic() {
    let (addr, _) = common::start_server().await;

    let mut subscriber = Client::connect(addr)
        .await
        .unwrap()
        .subscribe(vec!["__changes__".into()])
        .await
        .unwrap();

    let mut client = Client::connect(addr).await.unwrap();

    for i in 0..5 {
        client
            .set(&format!("k{i}"), format!("v{i}").into())
            .await
            .unwrap();
    }

    let mut prev_offset = -1i64;
    for _ in 0..5 {
        let msg = subscriber.next_message().await.unwrap().unwrap();
        let (offset, _, _, _) = parse_change_event(&msg.content);
        assert!(offset > prev_offset);
        prev_offset = offset;
    }
}

#[tokio::test]
async fn cdc_expired_key_emits_del() {
    let (addr, _) = common::start_server().await;

    let mut subscriber = Client::connect(addr)
        .await
        .unwrap()
        .subscribe(vec!["__changes__".into()])
        .await
        .unwrap();

    let mut client = Client::connect(addr).await.unwrap();
    client
        .set_expires("exp_k", "val".into(), Duration::from_millis(100))
        .await
        .unwrap();

    let _ = subscriber.next_message().await.unwrap().unwrap();

    time::sleep(Duration::from_millis(150)).await;

    let msg = subscriber.next_message().await.unwrap().unwrap();
    let (_, op, key, value) = parse_change_event(&msg.content);
    assert_eq!(op, "del");
    assert_eq!(key, "exp_k");
    assert!(value.is_none());
}
