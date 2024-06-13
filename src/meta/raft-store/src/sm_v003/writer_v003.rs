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
use std::io;
use std::sync::Arc;

use databend_common_meta_types::sys_data::SysData;
use futures::Stream;
use futures_util::TryStreamExt;
use log::debug;
use log::info;
use rotbl::v001::SeqMarked;
use tokio::sync::mpsc;
use tokio::task::JoinHandle;

use crate::leveled_store::db_builder::DBBuilder;
use crate::sm_v003::temp_snapshot_data::TempSnapshotDataV003;
use crate::sm_v003::write_entry::WriteEntry;
use crate::snapshot_config::SnapshotConfig;

/// Write kv pair snapshot data to [`SnapshotStoreV002`].
pub struct WriterV003 {
    db_builder: DBBuilder,

    snapshot_config: SnapshotConfig,

    /// Number of entries written.
    pub(crate) cnt: u64,

    /// The count of entries to reach before next progress logging.
    next_progress_cnt: u64,

    /// The time when the writer starts to write entries.
    start_time: std::time::Instant,
}

impl WriterV003 {
    /// Create a singleton writer for the snapshot.
    pub fn new(snapshot_config: &SnapshotConfig) -> Result<Self, io::Error> {
        let temp_path = snapshot_config.snapshot_temp_path();

        let db_builder = DBBuilder::new(
            temp_path.clone(),
            snapshot_config.raft_config().to_rotbl_config(),
        )?;

        let writer = WriterV003 {
            db_builder,
            snapshot_config: snapshot_config.clone(),
            cnt: 0,
            next_progress_cnt: 1000,
            start_time: std::time::Instant::now(),
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

    pub async fn write_kv_stream(
        self,
        mut stream: impl Stream<Item = Result<(String, SeqMarked), io::Error>> + Unpin,
        sys_data: SysData,
    ) -> Result<TempSnapshotDataV003, io::Error> {
        let (tx, jh) = self.spawn_writer_thread("write_kv_stream");
        while let Some((k, v)) = stream.try_next().await? {
            let ent = WriteEntry::Data((k, v));
            tx.send(ent).await.map_err(|_e| {
                io::Error::new(io::ErrorKind::Other, "fail to send entry to writer thread")
            })?;
        }
        tx.send(WriteEntry::Finish(sys_data)).await.map_err(|_e| {
            io::Error::new(io::ErrorKind::Other, "fail to send entry to writer thread")
        })?;

        let temp_snapshot = jh
            .await
            .map_err(|e| io::Error::new(io::ErrorKind::Other, e))??;

        Ok(temp_snapshot)
    }

    /// Write entries to the snapshot, without flushing.
    ///
    /// Returns the count of entries
    pub fn write_kv(
        mut self,
        mut kv_rx: mpsc::Receiver<WriteEntry<(String, SeqMarked), SysData>>,
    ) -> Result<TempSnapshotDataV003, io::Error> {
        while let Some(ent) = kv_rx.blocking_recv() {
            debug!(entry :? =(&ent); "write kv");

            let (k, v) = match ent {
                WriteEntry::Data(ent) => ent,
                WriteEntry::Finish(sys_data) => {
                    info!(
                        "received Commit, written {} entries, flush with: {:?}",
                        self.cnt, sys_data
                    );
                    let temp_snapshot_data = self.flush(sys_data)?;
                    return Ok(temp_snapshot_data);
                }
            };

            self.db_builder.append_kv(k, v)?;

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
    pub fn flush(self, sys_data: SysData) -> Result<TempSnapshotDataV003, io::Error> {
        let (path, r) = self.db_builder.flush(sys_data)?;
        let t = TempSnapshotDataV003::new(path, self.snapshot_config, Arc::new(r));
        Ok(t)
    }

    /// Spawn a thread to receive snapshot data `(String, SeqMarked)`
    /// and write them to a temp snapshot file.
    ///
    /// It returns a sender to send entries and a handle to wait for the thread to finish.
    /// Internally it calls tokio::spawn_blocking.
    ///
    /// When a [`WritenEntry::Finish`] is received, the thread will flush the data to disk and return
    /// a [`TempSnapshotDataV003`] and a [`SnapshotStat`].
    ///
    /// [`TempSnapshotDataV003`] is a temporary snapshot data that will be renamed to the final path by the caller.
    #[allow(clippy::type_complexity)]
    pub fn spawn_writer_thread(
        self,
        context: impl Display + Send + Sync + 'static,
    ) -> (
        mpsc::Sender<WriteEntry<(String, SeqMarked), SysData>>,
        JoinHandle<Result<TempSnapshotDataV003, io::Error>>,
    ) {
        let (tx, rx) = mpsc::channel(64 * 1024);

        // Spawn another thread to write entries to disk.
        let join_handle = databend_common_base::runtime::spawn_blocking(move || {
            let with_context =
                |e: io::Error| io::Error::new(e.kind(), format!("{} while {}", e, context));

            info!("snapshot_writer_thread start writing: {}", context);
            let temp_snapshot_data = self.write_kv(rx).map_err(with_context)?;

            info!(
                "snapshot writer flushed: path: {}",
                temp_snapshot_data.path()
            );

            Ok::<TempSnapshotDataV003, io::Error>(temp_snapshot_data)
        });

        (tx, join_handle)
    }
}
