use std::path::Path;

use crate::Db;
use crate::aof::encode_frame;
use crate::cmd::Set;

#[derive(Debug, Clone, Copy)]
pub(crate) struct SnapshotWriteStats {
    pub(crate) written_commands: usize,
}

pub(crate) async fn write_to_path(
    path: impl AsRef<Path>,
    db: &Db,
) -> crate::Result<SnapshotWriteStats> {
    let mut entries = db.snapshot_entries();
    entries.sort_by(|left, right| left.key.cmp(&right.key));

    let mut encoded = Vec::new();
    let written_commands = entries.len();
    for entry in entries {
        let frame = Set::new(entry.key, entry.value, entry.expire_in).into_frame();
        encode_frame(&frame, &mut encoded)?;
    }

    tokio::fs::write(path, encoded).await?;
    Ok(SnapshotWriteStats { written_commands })
}
