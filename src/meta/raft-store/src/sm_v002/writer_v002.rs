// Copyright 2021 Datafuse Labs
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use std::fs;
use std::io;
use std::io::Seek;
use std::io::Write;

use common_meta_types::LogId;

use crate::key_spaces::RaftStoreEntry;
use crate::sm_v002::snapshot_store::SnapshotStoreV002;
use crate::state_machine::MetaSnapshotId;
use crate::state_machine::StateMachineMetaKey;

pub struct WriterV002<'a> {
    /// The temp path to write to, which will be renamed to the final path.
    /// So that the readers could only see a complete snapshot.
    temp_path: String,

    inner: fs::File,

    /// The last_applied entry that has written to the snapshot.
    ///
    /// It will be used to create a snapshot id.
    last_applied: Option<LogId>,

    // Keep a mutable ref so that there could only be one writer at a time.
    snapshot_store: &'a mut SnapshotStoreV002,
}

impl<'a> io::Write for WriterV002<'a> {
    fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
        self.inner.write(buf)
    }

    fn flush(&mut self) -> io::Result<()> {
        self.inner.flush()
    }
}

impl<'a> WriterV002<'a> {
    /// Create a singleton writer for the snapshot.
    pub fn new(snapshot_store: &'a mut SnapshotStoreV002) -> Result<Self, io::Error> {
        let temp_path = snapshot_store.snapshot_temp_path();

        let f = fs::OpenOptions::new()
            .create_new(true)
            .write(true)
            .read(true)
            .open(&temp_path)?;

        let writer = WriterV002 {
            temp_path,
            inner: f,
            last_applied: None,
            snapshot_store,
        };

        Ok(writer)
    }

    /// Write entries to the snapshot, without flushing.
    ///
    /// Returns the count of entries
    pub fn write_entries<E>(
        &mut self,
        entries: impl IntoIterator<Item = RaftStoreEntry>,
    ) -> Result<usize, E>
    where
        E: std::error::Error + From<io::Error> + 'static,
    {
        self.write_entry_results(entries.into_iter().map(Ok))
    }

    /// Write `Result` of entries to the snapshot, without flushing.
    ///
    /// Returns the count of entries
    pub fn write_entry_results<E>(
        &mut self,
        entry_results: impl IntoIterator<Item = Result<RaftStoreEntry, E>>,
    ) -> Result<usize, E>
    where
        E: std::error::Error + From<io::Error> + 'static,
    {
        let mut cnt = 0;
        let data_version = self.snapshot_store.data_version();
        for ent in entry_results {
            let ent = ent?;

            tracing::debug!(entry = debug(&ent), "write {} entry", data_version);

            if let RaftStoreEntry::StateMachineMeta {
                key: StateMachineMetaKey::LastApplied,
                ref value,
            } = ent
            {
                let last: LogId = value.clone().try_into().unwrap();
                tracing::info!(last_applied = ?last, "write last applied to snapshot");

                assert!(
                    self.last_applied.is_none(),
                    "already seen a last_applied: {:?}",
                    self.last_applied
                );
                self.last_applied = Some(last);
            }

            serde_json::to_writer(&mut *self, &ent)
                .map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))?;

            self.write(b"\n")
                .map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))?;

            cnt += 1;
        }

        Ok(cnt)
    }

    /// Commit the snapshot so that it is visible to the readers.
    ///
    /// Returns the snapshot id and file size written.
    ///
    /// `uniq` is the unique number used to build snapshot id.
    /// If it is `None`, an epoch in milliseconds will be used.
    pub fn commit(&mut self, uniq: Option<u64>) -> Result<(MetaSnapshotId, u64), io::Error> {
        self.inner.flush()?;
        self.inner.sync_all()?;

        let file_size = self.inner.seek(io::SeekFrom::End(0))?;

        let snapshot_id = if let Some(u) = uniq {
            MetaSnapshotId::new(self.last_applied, u)
        } else {
            MetaSnapshotId::new_with_epoch(self.last_applied)
        };

        let path = self.snapshot_store.snapshot_path(&snapshot_id.to_string());

        fs::rename(&self.temp_path, path)?;

        tracing::info!(snapshot_id = ?snapshot_id, "snapshot committed: file_size: {}; {}", file_size, snapshot_id.to_string());

        Ok((snapshot_id, file_size))
    }
}
