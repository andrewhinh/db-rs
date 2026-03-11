use bytes::Bytes;

use crate::{Connection, Db, Frame};

#[derive(Debug, Default)]
pub struct Offset;

impl Offset {
    pub(crate) fn parse_frames(_parse: &mut crate::Parse) -> crate::Result<Offset> {
        Ok(Offset)
    }

    pub(crate) async fn apply(self, db: &Db, dst: &mut Connection) -> crate::Result<()> {
        dst.write_frame(&Frame::Integer(db.current_offset()))
            .await?;
        Ok(())
    }

    pub(crate) fn into_frame(self) -> Frame {
        let mut frame = Frame::array();
        frame.push_bulk(Bytes::from_static(b"offset"));
        frame
    }
}
