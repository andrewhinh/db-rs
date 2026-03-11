use bytes::Bytes;

use crate::cmd::Parse;
use crate::{Connection, Db, Frame};

/// Return remaining time to live in seconds.
#[derive(Debug)]
pub struct Ttl {
    key: String,
}

/// Return remaining time to live in milliseconds.
#[derive(Debug)]
pub struct Pttl {
    key: String,
}

impl Ttl {
    /// Create a new `Ttl` command.
    pub(crate) fn new(key: impl Into<String>) -> Ttl {
        Ttl { key: key.into() }
    }

    /// Parse a `Ttl` instance from a received frame.
    pub(crate) fn parse_frames(parse: &mut Parse) -> crate::Result<Ttl> {
        let key = parse.next_string()?;
        Ok(Ttl { key })
    }

    /// Apply the `Ttl` command to the specified `Db` instance.
    pub(crate) async fn apply(self, db: &Db, dst: &mut Connection) -> crate::Result<()> {
        let response = Frame::Integer(db.ttl(&self.key));
        dst.write_frame(&response).await?;
        Ok(())
    }

    /// Converts the command into an equivalent `Frame`.
    pub(crate) fn into_frame(self) -> Frame {
        let mut frame = Frame::array();
        frame.push_bulk(Bytes::from_static(b"ttl"));
        frame.push_bulk(Bytes::from(self.key.into_bytes()));
        frame
    }
}

impl Pttl {
    /// Create a new `Pttl` command.
    pub(crate) fn new(key: impl Into<String>) -> Pttl {
        Pttl { key: key.into() }
    }

    /// Parse a `Pttl` instance from a received frame.
    pub(crate) fn parse_frames(parse: &mut Parse) -> crate::Result<Pttl> {
        let key = parse.next_string()?;
        Ok(Pttl { key })
    }

    /// Apply the `Pttl` command to the specified `Db` instance.
    pub(crate) async fn apply(self, db: &Db, dst: &mut Connection) -> crate::Result<()> {
        let response = Frame::Integer(db.pttl(&self.key));
        dst.write_frame(&response).await?;
        Ok(())
    }

    /// Converts the command into an equivalent `Frame`.
    pub(crate) fn into_frame(self) -> Frame {
        let mut frame = Frame::array();
        frame.push_bulk(Bytes::from_static(b"pttl"));
        frame.push_bulk(Bytes::from(self.key.into_bytes()));
        frame
    }
}
