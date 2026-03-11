use std::io::{self, Write as _};
use std::path::{Path, PathBuf};
use std::sync::Arc;

use tokio::fs::{File, OpenOptions};
use tokio::io::AsyncWriteExt;
use tokio::sync::Mutex;

use crate::Frame;

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

fn encode_frame(frame: &Frame, dst: &mut Vec<u8>) -> io::Result<()> {
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
