mod common;

use db_rs::clients::{BufferedClient, Client};
use tokio::time::{self, Duration};

/// A basic "hello world" style test. A server instance is started in a
/// background task. A client instance is then established and used to
/// initialize the buffer. Set and get commands are sent to the server. The
/// response is then evaluated.
#[tokio::test]
async fn pool_key_value_get_set() {
    let (addr, _) = common::start_server().await;

    let client = Client::connect(addr).await.unwrap();
    let mut client = BufferedClient::buffer(client);

    client.set("hello", "world".into()).await.unwrap();

    let value = client.get("hello").await.unwrap().unwrap();
    assert_eq!(b"world", &value[..])
}

#[tokio::test]
async fn pool_key_value_exists_del() {
    let (addr, _) = common::start_server().await;

    let client = Client::connect(addr).await.unwrap();
    let mut client = BufferedClient::buffer(client);

    client.set("hello", "world".into()).await.unwrap();
    client.set("foo", "bar".into()).await.unwrap();

    let exists = client
        .exists(&["hello".into(), "foo".into(), "missing".into()])
        .await
        .unwrap();
    assert_eq!(2, exists);

    let removed = client
        .del(&["hello".into(), "missing".into()])
        .await
        .unwrap();
    assert_eq!(1, removed);
}

#[tokio::test]
async fn pool_key_value_expire_ttl_pttl() {
    let (addr, _) = common::start_server().await;

    let client = Client::connect(addr).await.unwrap();
    let mut client = BufferedClient::buffer(client);

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
}
