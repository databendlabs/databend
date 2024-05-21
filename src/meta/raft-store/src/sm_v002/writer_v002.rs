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
use std::io::BufWriter;
use std::io::Seek;
use std::io::Write;

use databend_common_meta_types::LogId;
use log::debug;
use log::info;

use crate::key_spaces::SMEntry;
use crate::sm_v002::SnapshotStoreV002;
use crate::state_machine::MetaSnapshotId;
use crate::state_machine::StateMachineMetaKey;

/// A write entry sent to snapshot writer.
///
/// A `Finish` entry indicates the end of the data.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum WriteEntry<T> {
    Data(T),
    Finish,
}

/// Write json lines snapshot data to [`SnapshotStoreV002`].
pub struct WriterV002<'a> {
    /// The temp path to write to, which will be renamed to the final path.
    /// So that the readers could only see a complete snapshot.
    temp_path: String,

    inner: BufWriter<fs::File>,

    /// Number of entries written.
    pub(crate) cnt: u64,

    /// The count of entries to reach before next progress logging.
    next_progress_cnt: u64,

    /// The time when the writer starts to write entries.
    start_time: std::time::Instant,

    /// The last_applied entry that has written to the snapshot.
    ///
    /// It will be used to create a snapshot id.
    pub(crate) last_applied: Option<LogId>,

    // Keep a mutable ref so that there could only be one writer at a time.
    snapshot_store: &'a mut SnapshotStoreV002,
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

        let buffered_file = BufWriter::with_capacity(16 * 1024 * 1024, f);

        let writer = WriterV002 {
            temp_path,
            inner: buffered_file,
            cnt: 0,
            next_progress_cnt: 1000,
            start_time: std::time::Instant::now(),
            last_applied: None,
            snapshot_store,
        };

        Ok(writer)
    }

    /// Increase the number of entries written by one.
    fn count(&mut self) {
        self.cnt += 1;

        if self.cnt == self.next_progress_cnt {
            self.log_progress();

            // Increase the number of entries before next log by 5%,
            // but at least 50k, at most 800k.
            let step = std::cmp::min(self.next_progress_cnt / 20, 800_000);
            let step = std::cmp::max(step, 50_000);

            self.next_progress_cnt += step;
        }
    }

    fn log_progress(&self) {
        let elapsed_sec = self.start_time.elapsed().as_secs();
        // Avoid div by 0
        let avg = self.cnt / (elapsed_sec + 1);

        if self.cnt >= 10_000_000 {
            info!(
                "Snapshot Writer has written {} million entries; avg: {} kilo entries/s",
                self.cnt / 1_000_000,
                avg / 1_000,
            )
        } else {
            info!(
                "Snapshot Writer has written {} kilo entries; avg: {} kilo entries/s",
                self.cnt / 1_000,
                avg / 1_000,
            )
        }
    }

    /// Write entries to the snapshot, without flushing.
    ///
    /// Returns the count of entries
    pub fn write_entries_sync(
        mut self,
        mut entries_rx: tokio::sync::mpsc::Receiver<WriteEntry<SMEntry>>,
    ) -> Result<Self, io::Error> {
        let data_version = self.snapshot_store.data_version();

        while let Some(ent) = entries_rx.blocking_recv() {
            debug!(entry :? =(&ent); "write {} entry", data_version);

            let ent = match ent {
                WriteEntry::Data(ent) => ent,
                WriteEntry::Finish => {
                    info!("received Commit, written {} entries, quit", self.cnt);
                    return Ok(self);
                }
            };

            if let SMEntry::StateMachineMeta {
                key: StateMachineMetaKey::LastApplied,
                ref value,
            } = ent
            {
                let last: LogId = value.clone().try_into().unwrap();
                info!(last_applied :? =(last); "write last applied to snapshot");

                assert!(
                    self.last_applied.is_none(),
                    "already seen a last_applied: {:?}",
                    self.last_applied
                );
                self.last_applied = Some(last);
            }

            serde_json::to_writer(&mut self.inner, &ent)
                .map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))?;

            self.inner
                .write(b"\n")
                .map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))?;

            self.count();
        }

        Err(io::Error::new(
            io::ErrorKind::UnexpectedEof,
            "input channel is closed",
        ))
    }

    /// Commit the snapshot so that it is visible to the readers.
    ///
    /// Returns the file size written.
    ///
    /// This method consumes the writer, thus the writer will not be used after commit.
    pub fn commit(mut self, snapshot_id: MetaSnapshotId) -> Result<u64, io::Error> {
        self.inner.flush()?;
        let mut f = self.inner.into_inner()?;
        f.sync_all()?;

        let file_size = f.seek(io::SeekFrom::End(0))?;

        let id_str = snapshot_id.to_string();
        let path = self.snapshot_store.snapshot_path(&id_str);

        fs::rename(&self.temp_path, path)?;

        info!(snapshot_id :% = id_str; "snapshot committed: file_size: {}", file_size);

        Ok(file_size)
    }
}
