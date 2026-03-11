mod common;

use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;

#[tokio::test]
async fn cas_set_if_not_exists() {
    let addr = common::start_server().await.0;
    let mut stream = TcpStream::connect(addr).await.unwrap();

    // CAS key null value when key missing -> :1
    stream
        .write_all(b"*4\r\n$3\r\ncas\r\n$3\r\nfoo\r\n$-1\r\n$3\r\nbar\r\n")
        .await
        .unwrap();

    let mut response = [0; 4];
    stream.read_exact(&mut response).await.unwrap();
    assert_eq!(b":1\r\n", &response);

    // GET returns value
    stream
        .write_all(b"*2\r\n$3\r\nget\r\n$3\r\nfoo\r\n")
        .await
        .unwrap();

    let mut response = [0; 9];
    stream.read_exact(&mut response).await.unwrap();
    assert_eq!(b"$3\r\nbar\r\n", &response);
}

#[tokio::test]
async fn cas_set_if_matches() {
    let addr = common::start_server().await.0;
    let mut stream = TcpStream::connect(addr).await.unwrap();

    stream
        .write_all(b"*3\r\n$3\r\nset\r\n$3\r\nfoo\r\n$2\r\nv1\r\n")
        .await
        .unwrap();
    let mut response = [0; 5];
    stream.read_exact(&mut response).await.unwrap();
    assert_eq!(b"+OK\r\n", &response);

    // CAS key v1 v2 -> :1
    stream
        .write_all(b"*4\r\n$3\r\ncas\r\n$3\r\nfoo\r\n$2\r\nv1\r\n$2\r\nv2\r\n")
        .await
        .unwrap();

    let mut response = [0; 4];
    stream.read_exact(&mut response).await.unwrap();
    assert_eq!(b":1\r\n", &response);

    // GET returns v2
    stream
        .write_all(b"*2\r\n$3\r\nget\r\n$3\r\nfoo\r\n")
        .await
        .unwrap();

    let mut response = [0; 8];
    stream.read_exact(&mut response).await.unwrap();
    assert_eq!(b"$2\r\nv2\r\n", &response);
}

#[tokio::test]
async fn cas_fails_on_mismatch() {
    let addr = common::start_server().await.0;
    let mut stream = TcpStream::connect(addr).await.unwrap();

    stream
        .write_all(b"*3\r\n$3\r\nset\r\n$3\r\nfoo\r\n$2\r\nv1\r\n")
        .await
        .unwrap();
    let mut response = [0; 5];
    stream.read_exact(&mut response).await.unwrap();

    // CAS key v2 v3 -> :0 (mismatch)
    stream
        .write_all(b"*4\r\n$3\r\ncas\r\n$3\r\nfoo\r\n$2\r\nv2\r\n$2\r\nv3\r\n")
        .await
        .unwrap();

    let mut response = [0; 4];
    stream.read_exact(&mut response).await.unwrap();
    assert_eq!(b":0\r\n", &response);

    // GET still v1
    stream
        .write_all(b"*2\r\n$3\r\nget\r\n$3\r\nfoo\r\n")
        .await
        .unwrap();

    let mut response = [0; 8];
    stream.read_exact(&mut response).await.unwrap();
    assert_eq!(b"$2\r\nv1\r\n", &response);
}

#[tokio::test]
async fn cas_fails_when_expect_nil_but_exists() {
    let addr = common::start_server().await.0;
    let mut stream = TcpStream::connect(addr).await.unwrap();

    stream
        .write_all(b"*3\r\n$3\r\nset\r\n$3\r\nfoo\r\n$2\r\nv1\r\n")
        .await
        .unwrap();
    let mut response = [0; 5];
    stream.read_exact(&mut response).await.unwrap();

    // CAS key null v2 -> :0 (key exists)
    stream
        .write_all(b"*4\r\n$3\r\ncas\r\n$3\r\nfoo\r\n$-1\r\n$2\r\nv2\r\n")
        .await
        .unwrap();

    let mut response = [0; 4];
    stream.read_exact(&mut response).await.unwrap();
    assert_eq!(b":0\r\n", &response);

    // GET still v1
    stream
        .write_all(b"*2\r\n$3\r\nget\r\n$3\r\nfoo\r\n")
        .await
        .unwrap();

    let mut response = [0; 8];
    stream.read_exact(&mut response).await.unwrap();
    assert_eq!(b"$2\r\nv1\r\n", &response);
}

#[tokio::test]
async fn cas_lost_update_prevention() {
    let addr = common::start_server().await.0;

    let mut client = db_rs::clients::Client::connect(addr).await.unwrap();
    client.set("ctr", "0".into()).await.unwrap();

    let mut handles = vec![];
    for _ in 0..4 {
        let addr = addr;
        handles.push(tokio::spawn(async move {
            let mut client = db_rs::clients::Client::connect(addr).await.unwrap();
            let mut success = 0u32;
            for _ in 0..10 {
                loop {
                    let old = client.get("ctr").await.unwrap().unwrap_or_default();
                    let n: u32 = std::str::from_utf8(&old)
                        .unwrap_or("0")
                        .parse()
                        .unwrap_or(0);
                    let new = format!("{}", n + 1);
                    if client.cas("ctr", Some(old), new.into()).await.unwrap() {
                        success += 1;
                        break;
                    }
                    tokio::task::yield_now().await;
                }
            }
            success
        }));
    }

    let mut results = Vec::with_capacity(4);
    for h in handles {
        results.push(h.await.unwrap());
    }

    let total: u32 = results.iter().sum();
    assert_eq!(
        total, 40,
        "each of 4 tasks did 10 increments; exactly 40 should succeed"
    );

    let mut client = db_rs::clients::Client::connect(addr).await.unwrap();
    let val = client.get("ctr").await.unwrap().unwrap();
    assert_eq!(std::str::from_utf8(&val).unwrap(), "40");
}
