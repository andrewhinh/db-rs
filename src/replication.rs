use std::io::Cursor;
use std::path::PathBuf;

use bytes::Bytes;
use tokio::sync::broadcast;
use tracing::{debug, error, info};

use crate::clients::Client;
use crate::{Db, Frame};

const CHANGES_BOOTSTRAP_CHANNEL: &str = "__changes__@boot";

fn parse_change(bytes: &[u8]) -> Option<(i64, String, String, Option<Bytes>)> {
    let mut cur = Cursor::new(bytes);
    let frame = Frame::parse(&mut cur).ok()?;
    let Frame::Array(arr) = frame else {
        return None;
    };
    if arr.len() < 4 {
        return None;
    }
    let offset = match &arr[0] {
        Frame::Integer(n) => *n,
        _ => return None,
    };
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
    Some((offset, op, key, value))
}

pub(crate) async fn run_follower(
    leader_addr: (String, u16),
    db: Db,
    mut shutdown: broadcast::Receiver<()>,
    repl_offset_path: Option<PathBuf>,
) {
    let addr = format!("{}:{}", leader_addr.0, leader_addr.1);
    info!(addr = %addr, "replication starting");

    let mut last_applied = repl_offset_path
        .as_ref()
        .and_then(|p| std::fs::read_to_string(p).ok())
        .and_then(|s| s.trim().parse().ok())
        .unwrap_or(-1);

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

        let mut in_snapshot = true;
        let mut first_offset: Option<i64> = None;

        loop {
            tokio::select! {
                msg = subscriber.next_message() => {
                    let Some(msg) = msg.ok().flatten() else {
                        debug!("replication stream ended");
                        break;
                    };
                    if let Some((offset, op, key, value)) = parse_change(&msg.content) {
                        let applied = if in_snapshot {
                            if first_offset.is_none() {
                                first_offset = Some(offset);
                            }
                            if offset == first_offset.unwrap() {
                                db.apply_change_quiet(&op, &key, value, offset);
                                false
                            } else {
                                in_snapshot = false;
                                if offset > last_applied {
                                    db.apply_change_quiet(&op, &key, value, offset);
                                    last_applied = offset;
                                    true
                                } else {
                                    false
                                }
                            }
                        } else if offset > last_applied {
                            db.apply_change_quiet(&op, &key, value, offset);
                            last_applied = offset;
                            true
                        } else {
                            false
                        };

                        if applied && let Some(ref p) = repl_offset_path {
                            let _ = std::fs::write(p, last_applied.to_string());
                        }
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
