use std::net::SocketAddr;
use std::path::PathBuf;
use std::time::{SystemTime, UNIX_EPOCH};

use db_rs::server::{self, ServerConfig};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use tokio::time::{Duration, sleep};

#[tokio::test]
async fn aof_appends_only_mutating_commands() {
    let aof_path = temp_aof_path("mutating-only");
    let addr = start_server_with_aof(aof_path.clone()).await;

    let mut stream = TcpStream::connect(addr).await.unwrap();

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

    let _ = tokio::fs::remove_file(&aof_path).await;
}

#[tokio::test]
async fn aof_appends_noop_mutating_commands() {
    let aof_path = temp_aof_path("noop-mutating");
    let addr = start_server_with_aof(aof_path.clone()).await;

    let mut stream = TcpStream::connect(addr).await.unwrap();

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

    let _ = tokio::fs::remove_file(&aof_path).await;
}

#[tokio::test]
async fn aof_preserves_command_order() {
    let aof_path = temp_aof_path("order");
    let addr = start_server_with_aof(aof_path.clone()).await;

    let mut stream = TcpStream::connect(addr).await.unwrap();

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

    let _ = tokio::fs::remove_file(&aof_path).await;
}

async fn start_server_with_aof(aof_path: PathBuf) -> SocketAddr {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let config = ServerConfig {
        aof_path: Some(aof_path),
    };

    tokio::spawn(async move {
        let _ = server::run_with_config(listener, tokio::signal::ctrl_c(), config).await;
    });

    addr
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
