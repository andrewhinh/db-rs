use std::net::SocketAddr;
use std::path::PathBuf;
use std::time::{SystemTime, UNIX_EPOCH};

use bytes::Bytes;
use db_rs::{
    clients::Client,
    server::{self, ServerConfig},
};
use tokio::net::TcpListener;
use tokio::sync::oneshot;
use tokio::time::{Duration, sleep};

#[tokio::test]
async fn snapshot_restore_restores_values_and_ttl_after_restart() {
    let snapshot_path = temp_snapshot_path("restore-values-ttl");

    {
        let server = start_server_with_snapshot(snapshot_path.clone()).await;
        let mut client = Client::connect(server.addr).await.unwrap();

        client.set("k1", "v1".into()).await.unwrap();
        client.set("k2", "v2".into()).await.unwrap();
        assert_eq!(1, client.expire("k1", 30).await.unwrap());
        assert_eq!(1, client.del(&["k2".to_string()]).await.unwrap());

        server.shutdown().await.unwrap();
    }

    {
        let server = start_server_with_snapshot(snapshot_path.clone()).await;
        let mut client = Client::connect(server.addr).await.unwrap();

        assert_eq!(Some(Bytes::from("v1")), client.get("k1").await.unwrap());
        assert_eq!(None, client.get("k2").await.unwrap());
        let ttl = client.ttl("k1").await.unwrap();
        assert!(ttl > 0 && ttl <= 30, "ttl out of expected range: {ttl}");

        server.shutdown().await.unwrap();
    }

    cleanup(&snapshot_path).await;
}

#[tokio::test]
async fn snapshot_restore_ignores_expired_entries() {
    let snapshot_path = temp_snapshot_path("ignores-expired");

    {
        let server = start_server_with_snapshot(snapshot_path.clone()).await;
        let mut client = Client::connect(server.addr).await.unwrap();

        client.set("alive", "yes".into()).await.unwrap();
        client.set("gone", "soon".into()).await.unwrap();
        assert_eq!(1, client.expire("gone", 1).await.unwrap());
        sleep(Duration::from_millis(1200)).await;

        server.shutdown().await.unwrap();
    }

    {
        let server = start_server_with_snapshot(snapshot_path.clone()).await;
        let mut client = Client::connect(server.addr).await.unwrap();

        assert_eq!(Some(Bytes::from("yes")), client.get("alive").await.unwrap());
        assert_eq!(None, client.get("gone").await.unwrap());
        assert_eq!(-2, client.ttl("gone").await.unwrap());

        server.shutdown().await.unwrap();
    }

    cleanup(&snapshot_path).await;
}

#[tokio::test]
async fn snapshot_restore_persists_latest_state_across_restarts() {
    let snapshot_path = temp_snapshot_path("latest-state");

    {
        let server = start_server_with_snapshot(snapshot_path.clone()).await;
        let mut client = Client::connect(server.addr).await.unwrap();
        client.set("k", "one".into()).await.unwrap();
        server.shutdown().await.unwrap();
    }

    {
        let server = start_server_with_snapshot(snapshot_path.clone()).await;
        let mut client = Client::connect(server.addr).await.unwrap();
        client.set("k", "two".into()).await.unwrap();
        server.shutdown().await.unwrap();
    }

    {
        let server = start_server_with_snapshot(snapshot_path.clone()).await;
        let mut client = Client::connect(server.addr).await.unwrap();
        assert_eq!(Some(Bytes::from("two")), client.get("k").await.unwrap());
        server.shutdown().await.unwrap();
    }

    cleanup(&snapshot_path).await;
}

async fn start_server_with_snapshot(snapshot_path: PathBuf) -> TestServer {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let config = ServerConfig {
        aof_path: None,
        snapshot_path: Some(snapshot_path),
    };
    let (shutdown_tx, shutdown_rx) = oneshot::channel::<()>();

    let handle = tokio::spawn(async move {
        server::run_with_config(
            listener,
            async move {
                let _ = shutdown_rx.await;
            },
            config,
        )
        .await
    });

    TestServer {
        addr,
        shutdown_tx: Some(shutdown_tx),
        handle,
    }
}

fn temp_snapshot_path(test_name: &str) -> PathBuf {
    let now = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_nanos();
    std::env::temp_dir().join(format!("db-rs-{test_name}-{now}.snapshot"))
}

async fn cleanup(path: &PathBuf) {
    let _ = tokio::fs::remove_file(path).await;
}

struct TestServer {
    addr: SocketAddr,
    shutdown_tx: Option<oneshot::Sender<()>>,
    handle: tokio::task::JoinHandle<db_rs::Result<()>>,
}

impl TestServer {
    async fn shutdown(mut self) -> db_rs::Result<()> {
        if let Some(shutdown_tx) = self.shutdown_tx.take() {
            let _ = shutdown_tx.send(());
        }
        self.handle.await??;
        Ok(())
    }
}
