mod common;

use db_rs::clients::Client;
use tempfile::tempdir;
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

#[tokio::test]
async fn follower_restart_resumes() {
    let (leader_addr, _leader) = common::start_server().await;
    let leader_host = leader_addr.ip().to_string();
    let leader_port = leader_addr.port();

    let tmp = tempdir().unwrap();
    let repl_offset_path = tmp.path().join("repl-offset");

    let config = db_rs::server::ServerConfig {
        replicaof: Some((leader_host.clone(), leader_port)),
        repl_offset_path: Some(repl_offset_path.clone()),
        ..Default::default()
    };
    let (_, follower_handle) = common::start_server_with_config(config).await;

    let mut leader_client = Client::connect(leader_addr).await.unwrap();
    leader_client.set("k1", "v1".into()).await.unwrap();
    leader_client.set("k2", "v2".into()).await.unwrap();
    sleep(Duration::from_millis(200)).await;

    follower_handle.abort();
    sleep(Duration::from_millis(100)).await;

    leader_client.set("k3", "v3".into()).await.unwrap();
    sleep(Duration::from_millis(100)).await;

    let config2 = db_rs::server::ServerConfig {
        replicaof: Some((leader_host, leader_port)),
        repl_offset_path: Some(repl_offset_path),
        ..Default::default()
    };
    let (follower_addr2, _follower2) = common::start_server_with_config(config2).await;
    sleep(Duration::from_millis(200)).await;

    let mut follower_client = Client::connect(follower_addr2).await.unwrap();
    assert_eq!(
        follower_client.get("k1").await.unwrap().as_deref(),
        Some(b"v1" as &[u8])
    );
    assert_eq!(
        follower_client.get("k2").await.unwrap().as_deref(),
        Some(b"v2" as &[u8])
    );
    assert_eq!(
        follower_client.get("k3").await.unwrap().as_deref(),
        Some(b"v3" as &[u8])
    );
}

#[tokio::test]
async fn no_duplicate_apply_on_resume() {
    let (leader_addr, _leader) = common::start_server().await;
    let leader_host = leader_addr.ip().to_string();
    let leader_port = leader_addr.port();

    let tmp = tempdir().unwrap();
    let repl_offset_path = tmp.path().join("repl-offset");

    let config = db_rs::server::ServerConfig {
        replicaof: Some((leader_host.clone(), leader_port)),
        repl_offset_path: Some(repl_offset_path.clone()),
        ..Default::default()
    };
    let (_, follower_handle) = common::start_server_with_config(config).await;

    let mut leader_client = Client::connect(leader_addr).await.unwrap();
    leader_client.set("k", "1".into()).await.unwrap();
    leader_client.set("k", "2".into()).await.unwrap();
    sleep(Duration::from_millis(200)).await;

    follower_handle.abort();
    sleep(Duration::from_millis(100)).await;

    let config2 = db_rs::server::ServerConfig {
        replicaof: Some((leader_host, leader_port)),
        repl_offset_path: Some(repl_offset_path),
        ..Default::default()
    };
    let (follower_addr2, _follower2) = common::start_server_with_config(config2).await;
    sleep(Duration::from_millis(200)).await;

    let mut follower_client = Client::connect(follower_addr2).await.unwrap();
    assert_eq!(
        follower_client.get("k").await.unwrap().as_deref(),
        Some(b"2" as &[u8])
    );
}
