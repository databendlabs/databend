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

use std::fmt::Display;
use std::fs;
use std::io;
use std::io::BufWriter;
use std::io::Seek;
use std::io::Write;

use databend_common_meta_types::SnapshotData;
use databend_common_meta_types::TempSnapshotData;
use log::debug;
use log::info;
use tokio::sync::mpsc;
use tokio::task::JoinHandle;

use crate::key_spaces::SMEntry;
use crate::ondisk::DataVersion;
use crate::sm_v003::SnapshotStat;
use crate::sm_v003::WriteEntry;
use crate::snapshot_config::SnapshotConfig;

/// Write json lines snapshot data to [`SnapshotStoreV002`].
pub struct WriterV002 {
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

    /// The version of the on disk data.
    data_version: DataVersion,
}

impl WriterV002 {
    /// Create a singleton writer for the snapshot.
    pub fn new(snapshot_config: &SnapshotConfig) -> Result<Self, io::Error> {
        let temp_path = snapshot_config.snapshot_temp_path();

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
            data_version: snapshot_config.data_version(),
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
        let data_version = self.data_version;

        while let Some(ent) = entries_rx.blocking_recv() {
            debug!(entry :? =(&ent); "write {} entry", data_version);

            let ent = match ent {
                WriteEntry::Data(ent) => ent,
                WriteEntry::Finish(_) => {
                    info!("received Commit, written {} entries, quit", self.cnt);
                    return Ok(self);
                }
            };

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

    /// Flush all data to disk.
    ///
    /// Returns a **temp** [`SnapshotData`] and the file size written.
    ///
    /// This method consumes the writer, thus the writer will not be used after commit.
    pub fn flush(mut self) -> Result<(TempSnapshotData, u64), io::Error> {
        self.inner.flush()?;
        let mut f = self.inner.into_inner()?;
        f.sync_all()?;

        let file_size = f.seek(io::SeekFrom::End(0))?;

        let snapshot_data = SnapshotData::new(&self.temp_path, f, true);
        let t = TempSnapshotData::new(snapshot_data);
        Ok((t, file_size))
    }

    /// Spawn a thread to receive snapshot data [`SMEntry`] and write them to a snapshot file.
    ///
    /// It returns a sender to send entries and a handle to wait for the thread to finish.
    /// Internally it calls tokio::spawn_blocking.
    ///
    /// When a [`WritenEntry::Finish`] is received, the thread will flush the data to disk and return
    /// a [`TempSnapshotData`] and a [`SnapshotStat`].
    ///
    /// [`TempSnapshotData`] is a temporary snapshot data that will be renamed to the final path by the caller.
    #[allow(clippy::type_complexity)]
    pub fn spawn_writer_thread(
        self,
        context: impl Display + Send + Sync + 'static,
    ) -> (
        mpsc::Sender<WriteEntry<SMEntry>>,
        JoinHandle<Result<(TempSnapshotData, SnapshotStat), io::Error>>,
    ) {
        let (tx, rx) = mpsc::channel(64 * 1024);

        // Spawn another thread to write entries to disk.
        let join_handle = databend_common_base::runtime::spawn_blocking(move || {
            let with_context =
                |e: io::Error| io::Error::new(e.kind(), format!("{} while {}", e, context));

            info!("snapshot_writer_thread start writing: {}", context);
            let writer = self.write_entries_sync(rx).map_err(with_context)?;

            info!("snapshot_writer_thread committing...: {}", context);
            let cnt = writer.cnt;
            let (temp_snapshot_data, size) = writer.flush().map_err(with_context)?;

            info!(
                "snapshot writer flushed: path: {}",
                temp_snapshot_data.path()
            );

            let snapshot_stat = SnapshotStat {
                size,
                entry_cnt: cnt,
            };

            Ok::<(TempSnapshotData, SnapshotStat), io::Error>((temp_snapshot_data, snapshot_stat))
        });

        (tx, join_handle)
    }
}
