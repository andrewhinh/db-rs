mod common;

use db_rs::clients::Client;
use tokio::time::{Duration, sleep};

#[tokio::test]
async fn read_your_writes() {
    let (leader_addr, _leader) = common::start_server().await;
    let config = db_rs::server::ServerConfig {
        replicaof: Some((leader_addr.ip().to_string(), leader_addr.port())),
        ..Default::default()
    };
    let (follower_addr, _follower) = common::start_server_with_config(config).await;

    let mut leader_client = Client::connect(leader_addr).await.unwrap();
    leader_client.set("k", "v1".into()).await.unwrap();
    let offset = leader_client.offset().await.unwrap();

    let mut follower_client = Client::connect(follower_addr).await.unwrap();
    let v = follower_client
        .get_with_offset("k", Some(offset))
        .await
        .unwrap();
    assert_eq!(v.as_deref(), Some(b"v1" as &[u8]));
}

#[tokio::test]
async fn monotonic_reads() {
    let (leader_addr, _leader) = common::start_server().await;
    let config = db_rs::server::ServerConfig {
        replicaof: Some((leader_addr.ip().to_string(), leader_addr.port())),
        ..Default::default()
    };
    let (follower_addr, _follower) = common::start_server_with_config(config).await;

    let mut leader_client = Client::connect(leader_addr).await.unwrap();
    leader_client.set("k", "v1".into()).await.unwrap();
    sleep(Duration::from_millis(100)).await;

    let mut follower_client = Client::connect(follower_addr).await.unwrap();
    let v1 = follower_client.get("k").await.unwrap();
    assert_eq!(v1.as_deref(), Some(b"v1" as &[u8]));
    let offset_after_first = follower_client.offset().await.unwrap();

    leader_client.set("k", "v2".into()).await.unwrap();
    sleep(Duration::from_millis(50)).await;

    let v2 = follower_client
        .get_with_offset("k", Some(offset_after_first))
        .await
        .unwrap();
    assert_eq!(v2.as_deref(), Some(b"v2" as &[u8]));
}
