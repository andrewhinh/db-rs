use std::net::SocketAddr;
use std::path::PathBuf;
use std::time::{SystemTime, UNIX_EPOCH};

use bytes::Bytes;
use db_rs::{
    clients::Client,
    server::{self, ServerConfig},
};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::oneshot;
use tokio::time::{Duration, sleep};

#[tokio::test]
async fn aof_appends_only_mutating_commands() {
    let aof_path = temp_aof_path("mutating-only");
    let server = start_server_with_aof(aof_path.clone()).await;

    let mut stream = TcpStream::connect(server.addr).await.unwrap();

    stream
        .write_all(b"*2\r\n$3\r\nGET\r\n$3\r\nkey\r\n")
        .await
        .unwrap();
    let mut nil = [0; 5];
    stream.read_exact(&mut nil).await.unwrap();
    assert_eq!(b"$-1\r\n", &nil);

    stream.write_all(b"*1\r\n$4\r\nPING\r\n").await.unwrap();
    let mut pong = [0; 7];
    stream.read_exact(&mut pong).await.unwrap();
    assert_eq!(b"+PONG\r\n", &pong);

    stream
        .write_all(b"*3\r\n$3\r\nSET\r\n$3\r\nkey\r\n$3\r\nval\r\n")
        .await
        .unwrap();
    let mut ok = [0; 5];
    stream.read_exact(&mut ok).await.unwrap();
    assert_eq!(b"+OK\r\n", &ok);

    stream
        .write_all(b"*2\r\n$6\r\nEXISTS\r\n$3\r\nkey\r\n")
        .await
        .unwrap();
    assert_eq!(1, read_integer_response(&mut stream).await);

    stream
        .write_all(b"*3\r\n$6\r\nEXPIRE\r\n$3\r\nkey\r\n$2\r\n10\r\n")
        .await
        .unwrap();
    assert_eq!(1, read_integer_response(&mut stream).await);

    stream
        .write_all(b"*2\r\n$3\r\nDEL\r\n$3\r\nkey\r\n")
        .await
        .unwrap();
    assert_eq!(1, read_integer_response(&mut stream).await);

    sleep(Duration::from_millis(20)).await;

    let aof = tokio::fs::read(&aof_path).await.unwrap();
    let expected = [
        &b"*3\r\n$3\r\nSET\r\n$3\r\nkey\r\n$3\r\nval\r\n"[..],
        &b"*3\r\n$6\r\nEXPIRE\r\n$3\r\nkey\r\n$2\r\n10\r\n"[..],
        &b"*2\r\n$3\r\nDEL\r\n$3\r\nkey\r\n"[..],
    ]
    .concat();
    assert_eq!(expected, aof);

    server.shutdown().await.unwrap();
    cleanup(&aof_path).await;
}

#[tokio::test]
async fn aof_appends_noop_mutating_commands() {
    let aof_path = temp_aof_path("noop-mutating");
    let server = start_server_with_aof(aof_path.clone()).await;

    let mut stream = TcpStream::connect(server.addr).await.unwrap();

    stream
        .write_all(b"*2\r\n$3\r\nDEL\r\n$7\r\nmissing\r\n")
        .await
        .unwrap();
    assert_eq!(0, read_integer_response(&mut stream).await);

    stream
        .write_all(b"*3\r\n$6\r\nEXPIRE\r\n$7\r\nmissing\r\n$2\r\n60\r\n")
        .await
        .unwrap();
    assert_eq!(0, read_integer_response(&mut stream).await);

    sleep(Duration::from_millis(20)).await;

    let aof = tokio::fs::read(&aof_path).await.unwrap();
    let expected = [
        &b"*2\r\n$3\r\nDEL\r\n$7\r\nmissing\r\n"[..],
        &b"*3\r\n$6\r\nEXPIRE\r\n$7\r\nmissing\r\n$2\r\n60\r\n"[..],
    ]
    .concat();
    assert_eq!(expected, aof);

    server.shutdown().await.unwrap();
    cleanup(&aof_path).await;
}

#[tokio::test]
async fn aof_preserves_command_order() {
    let aof_path = temp_aof_path("order");
    let server = start_server_with_aof(aof_path.clone()).await;

    let mut stream = TcpStream::connect(server.addr).await.unwrap();

    stream
        .write_all(b"*3\r\n$3\r\nSET\r\n$1\r\na\r\n$1\r\n1\r\n")
        .await
        .unwrap();
    let mut ok = [0; 5];
    stream.read_exact(&mut ok).await.unwrap();
    assert_eq!(b"+OK\r\n", &ok);

    stream
        .write_all(b"*3\r\n$3\r\nSET\r\n$1\r\na\r\n$1\r\n2\r\n")
        .await
        .unwrap();
    stream.read_exact(&mut ok).await.unwrap();
    assert_eq!(b"+OK\r\n", &ok);

    stream
        .write_all(b"*2\r\n$3\r\nDEL\r\n$1\r\na\r\n")
        .await
        .unwrap();
    assert_eq!(1, read_integer_response(&mut stream).await);

    sleep(Duration::from_millis(20)).await;

    let aof = tokio::fs::read(&aof_path).await.unwrap();
    let expected = [
        &b"*3\r\n$3\r\nSET\r\n$1\r\na\r\n$1\r\n1\r\n"[..],
        &b"*3\r\n$3\r\nSET\r\n$1\r\na\r\n$1\r\n2\r\n"[..],
        &b"*2\r\n$3\r\nDEL\r\n$1\r\na\r\n"[..],
    ]
    .concat();
    assert_eq!(expected, aof);

    server.shutdown().await.unwrap();
    cleanup(&aof_path).await;
}

#[tokio::test]
async fn aof_replay_restores_mutations_after_restart() {
    let aof_path = temp_aof_path("replay-restore");

    {
        let server = start_server_with_aof(aof_path.clone()).await;
        let mut client = Client::connect(server.addr).await.unwrap();

        client.set("k1", "v1".into()).await.unwrap();
        client.set("k2", "v2".into()).await.unwrap();
        assert_eq!(1, client.expire("k1", 30).await.unwrap());
        assert_eq!(1, client.del(&["k2".to_string()]).await.unwrap());

        server.shutdown().await.unwrap();
    }

    {
        let server = start_server_with_aof(aof_path.clone()).await;
        let mut client = Client::connect(server.addr).await.unwrap();

        assert_eq!(Some(Bytes::from("v1")), client.get("k1").await.unwrap());
        assert_eq!(None, client.get("k2").await.unwrap());

        let ttl = client.ttl("k1").await.unwrap();
        assert!(ttl > 0 && ttl <= 30, "ttl out of expected range: {ttl}");

        server.shutdown().await.unwrap();
    }

    cleanup(&aof_path).await;
}

#[tokio::test]
async fn aof_replay_preserves_last_write_wins_order() {
    let aof_path = temp_aof_path("replay-order");

    {
        let server = start_server_with_aof(aof_path.clone()).await;
        let mut client = Client::connect(server.addr).await.unwrap();

        client.set("k", "1".into()).await.unwrap();
        client.set("k", "2".into()).await.unwrap();
        assert_eq!(1, client.del(&["k".to_string()]).await.unwrap());
        client.set("k", "3".into()).await.unwrap();

        server.shutdown().await.unwrap();
    }

    {
        let server = start_server_with_aof(aof_path.clone()).await;
        let mut client = Client::connect(server.addr).await.unwrap();

        assert_eq!(Some(Bytes::from("3")), client.get("k").await.unwrap());

        server.shutdown().await.unwrap();
    }

    cleanup(&aof_path).await;
}

#[tokio::test]
async fn aof_replay_ignores_truncated_tail_frame() {
    let aof_path = temp_aof_path("replay-truncated-tail");
    let contents = [
        &b"*3\r\n$3\r\nSET\r\n$1\r\nk\r\n$1\r\nv\r\n"[..],
        &b"*3\r\n$3\r\nSET\r\n$1\r\nb"[..],
    ]
    .concat();
    tokio::fs::write(&aof_path, contents).await.unwrap();

    let server = start_server_with_aof(aof_path.clone()).await;
    let mut client = Client::connect(server.addr).await.unwrap();
    assert_eq!(Some(Bytes::from("v")), client.get("k").await.unwrap());
    assert_eq!(None, client.get("b").await.unwrap());

    server.shutdown().await.unwrap();
    cleanup(&aof_path).await;
}

#[tokio::test]
async fn aof_replay_rejects_unknown_command() {
    let aof_path = temp_aof_path("replay-unknown-command");
    tokio::fs::write(&aof_path, b"*1\r\n$3\r\nFOO\r\n")
        .await
        .unwrap();

    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let config = ServerConfig {
        aof_path: Some(aof_path.clone()),
        snapshot_path: None,
    };

    let err = server::run_with_config(listener, async {}, config)
        .await
        .unwrap_err();
    assert!(
        err.to_string()
            .contains("unsupported command in AOF replay")
    );

    cleanup(&aof_path).await;
}

async fn start_server_with_aof(aof_path: PathBuf) -> TestServer {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let config = ServerConfig {
        aof_path: Some(aof_path),
        snapshot_path: None,
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

fn temp_aof_path(test_name: &str) -> PathBuf {
    let now = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_nanos();
    std::env::temp_dir().join(format!("db-rs-{test_name}-{now}.aof"))
}

async fn read_integer_response(stream: &mut TcpStream) -> i64 {
    let mut line = Vec::new();

    loop {
        let mut byte = [0; 1];
        stream.read_exact(&mut byte).await.unwrap();
        line.push(byte[0]);
        if line.ends_with(b"\r\n") {
            break;
        }
    }

    assert_eq!(line[0], b':');
    std::str::from_utf8(&line[1..line.len() - 2])
        .unwrap()
        .parse::<i64>()
        .unwrap()
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
