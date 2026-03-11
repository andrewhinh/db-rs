use std::time::Duration;

use bytes::Bytes;

use crate::cmd::Parse;
use crate::{Connection, Db, Frame};

/// Set a timeout on key.
#[derive(Debug)]
pub struct Expire {
    key: String,
    seconds: u64,
}

impl Expire {
    /// Create a new `Expire` command.
    pub(crate) fn new(key: impl Into<String>, seconds: u64) -> Expire {
        Expire {
            key: key.into(),
            seconds,
        }
    }

    /// Parse an `Expire` instance from a received frame.
    ///
    /// # Format
    ///
    /// ```text
    /// EXPIRE key seconds
    /// ```
    pub(crate) fn parse_frames(parse: &mut Parse) -> crate::Result<Expire> {
        let key = parse.next_string()?;
        let seconds = parse.next_int()?;
        Ok(Expire { key, seconds })
    }

    /// Apply the `Expire` command to the specified `Db` instance.
    pub(crate) async fn apply(self, db: &Db, dst: &mut Connection) -> crate::Result<()> {
        let updated = db.expire(&self.key, Duration::from_secs(self.seconds));
        let response = Frame::Integer(if updated { 1 } else { 0 });
        dst.write_frame(&response).await?;
        Ok(())
    }

    pub(crate) fn apply_for_replay(self, db: &Db) -> crate::Result<()> {
        db.expire(&self.key, Duration::from_secs(self.seconds));
        Ok(())
    }

    /// Converts the command into an equivalent `Frame`.
    pub(crate) fn into_frame(self) -> Frame {
        let mut frame = Frame::array();
        frame.push_bulk(Bytes::from_static(b"expire"));
        frame.push_bulk(Bytes::from(self.key.into_bytes()));
        frame.push_int(self.seconds as i64);
        frame
    }
}
