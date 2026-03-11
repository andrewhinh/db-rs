use bytes::Bytes;
use tokio::time::{Duration, sleep};
use tracing::{debug, instrument};

use crate::ParseError;
use crate::{Connection, Db, Frame, Parse};

/// Get the value of key.
///
/// If the key does not exist the special value nil is returned. An error is
/// returned if the value stored at key is not a string, because GET only
/// handles string values.
///
/// Optional READOFFSET modifier: wait until the server has applied at least
/// the given offset before returning. Enables read-your-writes and monotonic
/// reads when reading from followers.
#[derive(Debug)]
pub struct Get {
    /// Name of the key to get
    key: String,
    read_offset: Option<i64>,
}

impl Get {
    /// Create a new `Get` command which fetches `key`.
    pub fn new(key: impl ToString) -> Get {
        Get {
            key: key.to_string(),
            read_offset: None,
        }
    }

    pub fn with_read_offset(key: impl ToString, offset: i64) -> Get {
        Get {
            key: key.to_string(),
            read_offset: Some(offset),
        }
    }

    /// Get the key
    pub fn key(&self) -> &str {
        &self.key
    }

    /// Parse a `Get` instance from a received frame.
    ///
    /// The `Parse` argument provides a cursor-like API to read fields from the
    /// `Frame`. At this point, the entire frame has already been received from
    /// the socket.
    ///
    /// The `GET` string has already been consumed.
    ///
    /// # Returns
    ///
    /// Returns the `Get` value on success. If the frame is malformed, `Err` is
    /// returned.
    ///
    /// # Format
    ///
    /// Expects an array frame containing two entries.
    ///
    /// ```text
    /// GET key [READOFFSET offset]
    /// ```
    pub(crate) fn parse_frames(parse: &mut Parse) -> crate::Result<Get> {
        // The `GET` string has already been consumed. The next value is the
        // name of the key to get. If the next value is not a string or the
        // input is fully consumed, then an error is returned.
        use ParseError::EndOfStream;

        let key = parse.next_string()?;
        let mut read_offset = None;

        match parse.next_string() {
            Ok(s) if s.to_uppercase() == "READOFFSET" => {
                let n = parse.next_int()?;
                read_offset = Some(n as i64);
            }
            Err(EndOfStream) => {}
            Ok(_) => return Err("protocol error; expected READOFFSET or end".into()),
            Err(e) => return Err(e.into()),
        }
        parse.finish()?;

        Ok(Get { key, read_offset })
    }

    /// Apply the `Get` command to the specified `Db` instance.
    ///
    /// The response is written to `dst`. This is called by the server in order
    /// to execute a received command.
    #[instrument(skip(self, db, dst))]
    pub(crate) async fn apply(self, db: &Db, dst: &mut Connection) -> crate::Result<()> {
        if let Some(required) = self.read_offset {
            let deadline = tokio::time::Instant::now() + Duration::from_secs(5);
            while db.current_offset() < required {
                if tokio::time::Instant::now() >= deadline {
                    return Err("timeout waiting for offset".into());
                }
                sleep(Duration::from_millis(10)).await;
            }
        }

        // Get the value from the shared database state
        let response = match db.get(&self.key) {
            // If a value is present, it is written to the client in "bulk"
            // format.
            Some(value) => Frame::Bulk(value),
            // If there is no value, `Null` is written.
            None => Frame::Null,
        };
        debug!(?response);

        // Write the response back to the client
        dst.write_frame(&response).await?;
        Ok(())
    }

    /// Converts the command into an equivalent `Frame`.
    ///
    /// This is called by the client when encoding a `Get` command to send to
    /// the server.
    pub(crate) fn into_frame(self) -> Frame {
        let mut frame = Frame::array();
        frame.push_bulk(Bytes::from("get".as_bytes()));
        frame.push_bulk(Bytes::from(self.key.into_bytes()));
        if let Some(off) = self.read_offset {
            frame.push_bulk(Bytes::from_static(b"readoffset"));
            frame.push_int(off);
        }
        frame
    }
}
