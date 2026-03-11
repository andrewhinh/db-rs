use bytes::Bytes;

use crate::cmd::{Parse, ParseError};
use crate::{Connection, Db, Frame};

/// Return the number of keys that exist.
#[derive(Debug)]
pub struct Exists {
    keys: Vec<String>,
}

impl Exists {
    /// Create a new `Exists` command.
    pub(crate) fn new(keys: &[String]) -> Exists {
        Exists {
            keys: keys.to_vec(),
        }
    }

    /// Parse an `Exists` instance from a received frame.
    ///
    /// # Format
    ///
    /// ```text
    /// EXISTS key [key ...]
    /// ```
    pub(crate) fn parse_frames(parse: &mut Parse) -> crate::Result<Exists> {
        use ParseError::EndOfStream;

        let mut keys = vec![parse.next_string()?];

        loop {
            match parse.next_string() {
                Ok(key) => keys.push(key),
                Err(EndOfStream) => break,
                Err(err) => return Err(err.into()),
            }
        }

        Ok(Exists { keys })
    }

    /// Apply the `Exists` command to the specified `Db` instance.
    pub(crate) async fn apply(self, db: &Db, dst: &mut Connection) -> crate::Result<()> {
        let count = db.exists_many(&self.keys);
        let response = Frame::Integer(count as i64);
        dst.write_frame(&response).await?;
        Ok(())
    }

    /// Converts the command into an equivalent `Frame`.
    pub(crate) fn into_frame(self) -> Frame {
        let mut frame = Frame::array();
        frame.push_bulk(Bytes::from_static(b"exists"));

        for key in self.keys {
            frame.push_bulk(Bytes::from(key.into_bytes()));
        }

        frame
    }
}
