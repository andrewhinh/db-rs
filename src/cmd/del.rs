use bytes::Bytes;

use crate::cmd::{Parse, ParseError};
use crate::{Connection, Db, Frame};

/// Delete one or more keys.
#[derive(Debug)]
pub struct Del {
    keys: Vec<String>,
}

impl Del {
    /// Create a new `Del` command.
    pub(crate) fn new(keys: &[String]) -> Del {
        Del {
            keys: keys.to_vec(),
        }
    }

    /// Parse a `Del` instance from a received frame.
    ///
    /// # Format
    ///
    /// ```text
    /// DEL key [key ...]
    /// ```
    pub(crate) fn parse_frames(parse: &mut Parse) -> crate::Result<Del> {
        use ParseError::EndOfStream;

        let mut keys = vec![parse.next_string()?];

        loop {
            match parse.next_string() {
                Ok(key) => keys.push(key),
                Err(EndOfStream) => break,
                Err(err) => return Err(err.into()),
            }
        }

        Ok(Del { keys })
    }

    /// Apply the `Del` command to the specified `Db` instance.
    pub(crate) async fn apply(self, db: &Db, dst: &mut Connection) -> crate::Result<bool> {
        let removed = db.del_many(&self.keys);
        let response = Frame::Integer(removed as i64);
        dst.write_frame(&response).await?;
        Ok(true)
    }

    pub(crate) fn apply_for_replay(self, db: &Db) -> crate::Result<()> {
        db.del_many(&self.keys);
        Ok(())
    }

    /// Converts the command into an equivalent `Frame`.
    pub(crate) fn into_frame(self) -> Frame {
        let mut frame = Frame::array();
        frame.push_bulk(Bytes::from_static(b"del"));

        for key in self.keys {
            frame.push_bulk(Bytes::from(key.into_bytes()));
        }

        frame
    }
}
