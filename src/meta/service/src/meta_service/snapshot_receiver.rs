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

use std::fmt;

use databend_common_base::base::tokio::io::AsyncWriteExt;
use databend_common_meta_types::protobuf::SnapshotChunkRequestV2;
use databend_common_meta_types::Snapshot;
use databend_common_meta_types::SnapshotData;
use databend_common_meta_types::SnapshotMeta;
use databend_common_meta_types::Vote;
use log::debug;
use log::info;
use tonic::Status;

use crate::metrics::raft_metrics;

pub(crate) struct Receiver {
    remote_addr: String,

    snapshot_data: Option<Box<SnapshotData>>,

    /// number of bytes received.
    n_received: usize,

    /// number of bytes received.
    size_received: usize,
}

impl Receiver {
    /// Create a new snapshot receiver with an empty snapshot.
    pub(crate) fn new(remote_addr: impl ToString, snapshot_data: Box<SnapshotData>) -> Self {
        let remote_addr = remote_addr.to_string();
        info!("Begin receiving snapshot v2 stream from: {}", remote_addr);

        Receiver {
            remote_addr,
            snapshot_data: Some(snapshot_data),
            n_received: 0,
            size_received: 0,
        }
    }

    pub(crate) fn stat_str(&self) -> impl fmt::Display {
        format!(
            "received {} chunks, {} bytes from {}",
            self.n_received, self.size_received, self.remote_addr
        )
    }

    pub(crate) async fn receive(
        &mut self,
        chunk: SnapshotChunkRequestV2,
    ) -> Result<Option<(String, Vote, Snapshot)>, Status> {
        // 1. update stat
        self.update_stat(&chunk);

        // 2. write chunk to local snapshot_data
        {
            let snapshot_data = self.snapshot_data.as_mut().ok_or_else(|| {
                Status::internal("snapshot_data is already shutdown when receiving snapshot chunk")
            })?;

            snapshot_data.write_all(&chunk.chunk).await.map_err(|e| {
                Status::internal(format!(
                    "{} when writing chunk to local temp snapshot_data",
                    e
                ))
            })?;
        }

        // 3. if it is the last chunk, finish and return the snapshot.
        {
            let end = self.load_end(&chunk).map_err(|e| {
                Status::invalid_argument(format!("{} when loading last chunk rpc_meta", e))
            })?;

            let Some((format, vote, snapshot_meta)) = end else {
                return Ok(None);
            };

            info!(
                "snapshot from {} is completely received, format: {}, vote: {:?}, meta: {:?}, size: {}",
                self.remote_addr, format, vote, snapshot_meta, self.size_received
            );

            // Safe unwrap: snapshot_data is guaranteed to be Some in the above code.
            let mut snapshot_data = self.snapshot_data.take().unwrap();

            snapshot_data.shutdown().await.map_err(|e| {
                Status::internal(format!("{} when shutdown local temp snapshot_data", e))
            })?;

            Ok(Some((format, vote, Snapshot {
                meta: snapshot_meta,
                snapshot: snapshot_data,
            })))
        }
    }

    fn update_stat(&mut self, chunk: &SnapshotChunkRequestV2) {
        let data_len = chunk.chunk.len();
        self.n_received += 1;
        self.size_received += data_len;

        debug!(
            len = data_len,
            total_len = self.size_received;
            "received {}-th snapshot chunk from {}",
            self.n_received, self.remote_addr
        );

        if self.n_received % 100 == 0 {
            info!(
                total_len = self.size_received;
                "received {}-th snapshot chunk from {}",
                self.n_received, self.remote_addr
            );
        }

        raft_metrics::network::incr_recvfrom_bytes(self.remote_addr.clone(), data_len as u64);
    }

    /// Load meta data from the last chunk.
    fn load_end(
        &self,
        chunk: &SnapshotChunkRequestV2,
    ) -> Result<Option<(String, Vote, SnapshotMeta)>, serde_json::Error> {
        let Some(meta) = &chunk.rpc_meta else {
            return Ok(None);
        };

        let (format, vote, snapshot_meta): (String, Vote, SnapshotMeta) =
            serde_json::from_str(meta)?;

        Ok(Some((format, vote, snapshot_meta)))
    }
}
