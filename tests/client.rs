mod common;

use std::net::SocketAddr;

use db_rs::clients::Client;
use tokio::time::{self, Duration};

/// A PING PONG test without message provided.
/// It should return "PONG".
#[tokio::test]
async fn ping_pong_without_message() {
    let (addr, _) = common::start_server().await;
    let mut client = Client::connect(addr).await.unwrap();

    let pong = client.ping(None).await.unwrap();
    assert_eq!(b"PONG", &pong[..]);
}

/// A PING PONG test with message provided.
/// It should return the message.
#[tokio::test]
async fn ping_pong_with_message() {
    let (addr, _) = common::start_server().await;
    let mut client = Client::connect(addr).await.unwrap();

    let pong = client.ping(Some("你好世界".into())).await.unwrap();
    assert_eq!("你好世界".as_bytes(), &pong[..]);
}

/// A basic "hello world" style test. A server instance is started in a
/// background task. A client instance is then established and set and get
/// commands are sent to the server. The response is then evaluated
#[tokio::test]
async fn key_value_get_set() {
    let (addr, _) = common::start_server().await;

    let mut client = Client::connect(addr).await.unwrap();
    client.set("hello", "world".into()).await.unwrap();

    let value = client.get("hello").await.unwrap().unwrap();
    assert_eq!(b"world", &value[..])
}

#[tokio::test]
async fn key_value_exists_del() {
    let (addr, _) = common::start_server().await;

    let mut client = Client::connect(addr).await.unwrap();
    client.set("hello", "world".into()).await.unwrap();
    client.set("foo", "bar".into()).await.unwrap();

    let exists = client
        .exists(&["hello".into(), "foo".into(), "missing".into()])
        .await
        .unwrap();
    assert_eq!(2, exists);

    let removed = client
        .del(&["hello".into(), "missing".into(), "hello".into()])
        .await
        .unwrap();
    assert_eq!(1, removed);

    let value = client.get("hello").await.unwrap();
    assert!(value.is_none());

    let exists = client.exists(&["foo".into(), "foo".into()]).await.unwrap();
    assert_eq!(2, exists);
}

#[tokio::test]
async fn key_value_expire_ttl_pttl() {
    let (addr, _) = common::start_server().await;

    let mut client = Client::connect(addr).await.unwrap();

    let ttl = client.ttl("hello").await.unwrap();
    assert_eq!(-2, ttl);

    client.set("hello", "world".into()).await.unwrap();
    let ttl = client.ttl("hello").await.unwrap();
    assert_eq!(-1, ttl);

    let updated = client.expire("hello", 2).await.unwrap();
    assert_eq!(1, updated);

    let ttl = client.ttl("hello").await.unwrap();
    assert!((0..=2).contains(&ttl));

    let pttl = client.pttl("hello").await.unwrap();
    assert!((1..=2000).contains(&pttl));

    time::sleep(Duration::from_millis(2200)).await;

    let ttl = client.ttl("hello").await.unwrap();
    assert_eq!(-2, ttl);

    let pttl = client.pttl("hello").await.unwrap();
    assert_eq!(-2, pttl);

    let value = client.get("hello").await.unwrap();
    assert!(value.is_none());
}

/// similar to the "hello world" style test, But this time
/// a single channel subscription will be tested instead
#[tokio::test]
async fn receive_message_subscribed_channel() {
    let (addr, _) = common::start_server().await;

    let client = Client::connect(addr).await.unwrap();
    let mut subscriber = client.subscribe(vec!["hello".into()]).await.unwrap();

    tokio::spawn(async move {
        let mut client = Client::connect(addr).await.unwrap();
        client.publish("hello", "world".into()).await.unwrap()
    });

    let message = subscriber.next_message().await.unwrap().unwrap();
    assert_eq!("hello", &message.channel);
    assert_eq!(b"world", &message.content[..])
}

/// test that a client gets messages from multiple subscribed channels
#[tokio::test]
async fn receive_message_multiple_subscribed_channels() {
    let (addr, _) = common::start_server().await;

    let client = Client::connect(addr).await.unwrap();
    let mut subscriber = client
        .subscribe(vec!["hello".into(), "world".into()])
        .await
        .unwrap();

    tokio::spawn(async move {
        let mut client = Client::connect(addr).await.unwrap();
        client.publish("hello", "world".into()).await.unwrap()
    });

    let message1 = subscriber.next_message().await.unwrap().unwrap();
    assert_eq!("hello", &message1.channel);
    assert_eq!(b"world", &message1.content[..]);

    tokio::spawn(async move {
        let mut client = Client::connect(addr).await.unwrap();
        client.publish("world", "howdy?".into()).await.unwrap()
    });

    let message2 = subscriber.next_message().await.unwrap().unwrap();
    assert_eq!("world", &message2.channel);
    assert_eq!(b"howdy?", &message2.content[..])
}

/// test that a client accurately removes its own subscribed channel list
/// when unsubscribing to all subscribed channels by submitting an empty vec
#[tokio::test]
async fn unsubscribes_from_channels() {
    let (addr, _) = common::start_server().await;

    let client = Client::connect(addr).await.unwrap();
    let mut subscriber = client
        .subscribe(vec!["hello".into(), "world".into()])
        .await
        .unwrap();

    subscriber.unsubscribe(&[]).await.unwrap();
    assert_eq!(subscriber.get_subscribed().len(), 0);
}
