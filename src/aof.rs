use std::io::{self, Cursor, ErrorKind, Write as _};
use std::path::{Path, PathBuf};
use std::sync::Arc;

use tokio::fs::{self, File, OpenOptions};
use tokio::io::AsyncWriteExt;
use tokio::sync::Mutex;
use tracing::warn;

use crate::cmd::Set;
use crate::{Command, Db, Frame, frame};

#[derive(Debug, Clone)]
pub(crate) struct AofAppender {
    file: Arc<Mutex<File>>,
    path: PathBuf,
}

impl AofAppender {
    pub(crate) async fn open(path: impl AsRef<Path>) -> crate::Result<AofAppender> {
        let path = path.as_ref().to_path_buf();
        let file = OpenOptions::new()
            .create(true)
            .append(true)
            .open(&path)
            .await?;

        Ok(AofAppender {
            file: Arc::new(Mutex::new(file)),
            path,
        })
    }

    pub(crate) fn path(&self) -> &Path {
        &self.path
    }

    pub(crate) async fn append_frame(&self, frame: &Frame) -> crate::Result<()> {
        let mut encoded = Vec::new();
        encode_frame(frame, &mut encoded)?;

        let mut file = self.file.lock().await;
        file.write_all(&encoded).await?;
        file.flush().await?;

        Ok(())
    }
}

#[derive(Debug, Default, Clone, Copy)]
pub(crate) struct ReplayStats {
    pub(crate) applied_commands: usize,
    pub(crate) truncated_tail_bytes: usize,
}

#[derive(Debug, Clone, Copy, Default)]
pub(crate) struct RewriteStats {
    pub(crate) written_commands: usize,
    pub(crate) old_bytes: u64,
    pub(crate) new_bytes: u64,
}

pub(crate) async fn replay_from_path(
    path: impl AsRef<Path>,
    db: &Db,
) -> crate::Result<ReplayStats> {
    let path = path.as_ref();

    let contents = match tokio::fs::read(path).await {
        Ok(contents) => contents,
        Err(err) if err.kind() == ErrorKind::NotFound => return Ok(ReplayStats::default()),
        Err(err) => return Err(err.into()),
    };

    replay_bytes(&contents, db)
}

pub(crate) async fn rewrite_from_db(
    path: impl AsRef<Path>,
    db: &Db,
) -> crate::Result<RewriteStats> {
    let path = path.as_ref();
    let old_bytes = match fs::metadata(path).await {
        Ok(metadata) => metadata.len(),
        Err(err) if err.kind() == ErrorKind::NotFound => 0,
        Err(err) => return Err(err.into()),
    };

    let mut entries = db.snapshot_entries();
    entries.sort_by(|left, right| left.key.cmp(&right.key));

    let mut encoded = Vec::new();
    let written_commands = entries.len();
    for entry in entries {
        let frame = Set::new(entry.key, entry.value, entry.expire_in).into_frame();
        encode_frame(&frame, &mut encoded)?;
    }
    let new_bytes = encoded.len() as u64;

    let temp_path = temp_rewrite_path(path);
    fs::write(&temp_path, &encoded).await?;

    if let Err(err) = fs::rename(&temp_path, path).await {
        let _ = fs::remove_file(&temp_path).await;
        return Err(err.into());
    }

    Ok(RewriteStats {
        written_commands,
        old_bytes,
        new_bytes,
    })
}

fn temp_rewrite_path(path: &Path) -> PathBuf {
    let mut tmp = path.to_path_buf();
    let suffix = format!("rewrite-{}", std::process::id());
    if let Some(ext) = path.extension().and_then(|ext| ext.to_str())
        && !ext.is_empty()
    {
        tmp.set_extension(format!("{ext}.{suffix}"));
    } else {
        tmp.set_extension(suffix);
    }
    tmp
}

fn replay_bytes(contents: &[u8], db: &Db) -> crate::Result<ReplayStats> {
    let mut stats = ReplayStats::default();
    let mut cursor = Cursor::new(contents);

    while cursor.position() as usize != contents.len() {
        let start = cursor.position() as usize;

        match Frame::check(&mut cursor) {
            Ok(()) => {
                cursor.set_position(start as u64);
                let frame = Frame::parse(&mut cursor)?;
                let command = Command::from_frame(frame)?;
                command.apply_for_replay(db)?;
                stats.applied_commands += 1;
            }
            Err(frame::Error::Incomplete) => {
                stats.truncated_tail_bytes = contents.len() - start;
                warn!(
                    truncated_tail_bytes = stats.truncated_tail_bytes,
                    "ignoring truncated AOF tail during replay"
                );
                break;
            }
            Err(err) => return Err(err.into()),
        }
    }

    Ok(stats)
}

pub(crate) fn encode_frame(frame: &Frame, dst: &mut Vec<u8>) -> io::Result<()> {
    match frame {
        Frame::Simple(value) => {
            dst.push(b'+');
            dst.extend_from_slice(value.as_bytes());
            dst.extend_from_slice(b"\r\n");
        }
        Frame::Error(value) => {
            dst.push(b'-');
            dst.extend_from_slice(value.as_bytes());
            dst.extend_from_slice(b"\r\n");
        }
        Frame::Integer(value) => {
            dst.push(b':');
            write!(dst, "{value}\r\n")?;
        }
        Frame::Null => dst.extend_from_slice(b"$-1\r\n"),
        Frame::Bulk(value) => {
            write!(dst, "${}\r\n", value.len())?;
            dst.extend_from_slice(value);
            dst.extend_from_slice(b"\r\n");
        }
        Frame::Array(values) => {
            write!(dst, "*{}\r\n", values.len())?;
            for value in values {
                encode_frame(value, dst)?;
            }
        }
    }

    Ok(())
}
