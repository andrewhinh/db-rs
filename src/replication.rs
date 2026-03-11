use std::io::Cursor;

use bytes::Bytes;
use tokio::sync::broadcast;
use tracing::{debug, error, info};

use crate::clients::Client;
use crate::{Db, Frame};

const CHANGES_BOOTSTRAP_CHANNEL: &str = "__changes__@boot";

fn parse_change(bytes: &[u8]) -> Option<(String, String, Option<Bytes>)> {
    let mut cur = Cursor::new(bytes);
    let frame = Frame::parse(&mut cur).ok()?;
    let Frame::Array(arr) = frame else {
        return None;
    };
    if arr.len() < 4 {
        return None;
    }
    let op = match &arr[1] {
        Frame::Simple(s) => s.clone(),
        _ => return None,
    };
    let key = match &arr[2] {
        Frame::Bulk(b) => String::from_utf8_lossy(b).into_owned(),
        _ => return None,
    };
    let value = match &arr[3] {
        Frame::Null => None,
        Frame::Bulk(b) => Some(b.clone()),
        _ => return None,
    };
    Some((op, key, value))
}

pub(crate) async fn run_follower(
    leader_addr: (String, u16),
    db: Db,
    mut shutdown: broadcast::Receiver<()>,
) {
    let addr = format!("{}:{}", leader_addr.0, leader_addr.1);
    info!(addr = %addr, "replication starting");

    loop {
        let client = match Client::connect(&addr).await {
            Ok(c) => c,
            Err(e) => {
                error!(cause = %e, "replication connect failed, retrying");
                tokio::select! {
                    _ = tokio::time::sleep(tokio::time::Duration::from_secs(1)) => {}
                    _ = shutdown.recv() => return,
                }
                continue;
            }
        };

        let mut subscriber = match client
            .subscribe(vec![CHANGES_BOOTSTRAP_CHANNEL.to_string()])
            .await
        {
            Ok(s) => s,
            Err(e) => {
                error!(cause = %e, "replication subscribe failed, retrying");
                tokio::select! {
                    _ = tokio::time::sleep(tokio::time::Duration::from_secs(1)) => {}
                    _ = shutdown.recv() => return,
                }
                continue;
            }
        };

        loop {
            tokio::select! {
                msg = subscriber.next_message() => {
                    let Some(msg) = msg.ok().flatten() else {
                        debug!("replication stream ended");
                        break;
                    };
                    if let Some((op, key, value)) = parse_change(&msg.content) {
                        db.apply_change_quiet(&op, &key, value);
                    }
                }
                _ = shutdown.recv() => {
                    info!("replication shutdown");
                    return;
                }
            }
        }
    }
}
