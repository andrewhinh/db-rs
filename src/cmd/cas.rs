use bytes::Bytes;
use tracing::{debug, instrument};

use crate::cmd::Parse;
use crate::{Connection, Db, Frame};

/// Compare-and-set: set key to value only if current value equals expected.
/// Returns :1 on success, :0 on mismatch.
#[derive(Debug)]
pub struct Cas {
    key: String,
    expected: Option<Bytes>,
    value: Bytes,
}

impl Cas {
    pub fn new(key: impl ToString, expected: Option<Bytes>, value: Bytes) -> Cas {
        Cas {
            key: key.to_string(),
            expected,
            value,
        }
    }

    pub(crate) fn parse_frames(parse: &mut Parse) -> crate::Result<Cas> {
        let key = parse.next_string()?;
        let expected = parse.next_bytes_or_null()?;
        let value = parse.next_bytes()?;
        Ok(Cas {
            key,
            expected,
            value,
        })
    }

    #[instrument(skip(self, db, dst))]
    pub(crate) async fn apply(self, db: &Db, dst: &mut Connection) -> crate::Result<bool> {
        let ok = db.cas(&self.key, self.expected.as_deref(), self.value);
        let response = Frame::Integer(ok as i64);
        debug!(?response);
        dst.write_frame(&response).await?;
        Ok(ok)
    }

    pub(crate) fn apply_for_replay(self, db: &Db) -> crate::Result<()> {
        let _ = db.cas(&self.key, self.expected.as_deref(), self.value);
        Ok(())
    }

    pub(crate) fn into_frame(self) -> Frame {
        let mut frame = Frame::array();
        frame.push_bulk(Bytes::from_static(b"cas"));
        frame.push_bulk(Bytes::from(self.key.into_bytes()));
        match &self.expected {
            Some(b) => frame.push_bulk(b.clone()),
            None => frame.push_null(),
        }
        frame.push_bulk(self.value);
        frame
    }
}
