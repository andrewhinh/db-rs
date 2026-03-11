mod common;

use db_rs::clients::Client;
use tokio::time::{Duration, sleep};

#[tokio::test]
async fn follower_copies_leader_state() {
    let (leader_addr, _leader) = common::start_server().await;
    let leader_host = leader_addr.ip().to_string();
    let leader_port = leader_addr.port();

    let config = db_rs::server::ServerConfig {
        replicaof: Some((leader_host, leader_port)),
        ..Default::default()
    };
    let (follower_addr, _follower) = common::start_server_with_config(config).await;

    let mut leader_client = Client::connect(leader_addr).await.unwrap();
    leader_client.set("k1", "v1".into()).await.unwrap();

    sleep(Duration::from_millis(200)).await;

    let mut follower_client = Client::connect(follower_addr).await.unwrap();
    let v = follower_client.get("k1").await.unwrap();
    assert_eq!(v.as_deref(), Some(b"v1" as &[u8]));
}

#[tokio::test]
async fn follower_rejects_writes() {
    let (leader_addr, _leader) = common::start_server().await;
    let config = db_rs::server::ServerConfig {
        replicaof: Some((leader_addr.ip().to_string(), leader_addr.port())),
        ..Default::default()
    };
    let (follower_addr, _follower) = common::start_server_with_config(config).await;

    sleep(Duration::from_millis(200)).await;

    let mut follower_client = Client::connect(follower_addr).await.unwrap();
    let err = follower_client.set("k1", "v1".into()).await.unwrap_err();
    let msg = err.to_string();
    assert!(msg.contains("READONLY"), "expected READONLY, got: {}", msg);
}

#[tokio::test]
async fn replication_lag_visible() {
    let (leader_addr, _leader) = common::start_server().await;
    let config = db_rs::server::ServerConfig {
        replicaof: Some((leader_addr.ip().to_string(), leader_addr.port())),
        ..Default::default()
    };
    let (follower_addr, _follower) = common::start_server_with_config(config).await;

    let mut leader_client = Client::connect(leader_addr).await.unwrap();
    let mut follower_client = Client::connect(follower_addr).await.unwrap();

    leader_client.set("k1", "v1".into()).await.unwrap();
    sleep(Duration::from_millis(200)).await;
    let v = follower_client.get("k1").await.unwrap();
    assert_eq!(v.as_deref(), Some(b"v1" as &[u8]));
}
