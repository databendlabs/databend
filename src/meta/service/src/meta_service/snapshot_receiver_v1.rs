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
use std::fs::File;
use std::io;
use std::io::BufWriter;
use std::io::Write;

use databend_common_meta_raft_store::sm_v003::SnapshotStoreV003;
use databend_common_meta_sled_store::openraft::error::Fatal;
use databend_common_meta_sled_store::openraft::ErrorSubject;
use databend_common_meta_sled_store::openraft::ErrorVerb;
use databend_common_meta_sled_store::openraft::SnapshotId;
use databend_common_meta_sled_store::openraft::SnapshotSegmentId;
use databend_common_meta_types::InstallSnapshotError;
use databend_common_meta_types::InstallSnapshotRequest;
use databend_common_meta_types::RaftError;
use databend_common_meta_types::SnapshotMismatch;
use databend_common_meta_types::StorageError;
use log::info;
use minitrace::func_name;

pub struct ReceiverV1 {
    /// The offset of the last byte written to the snapshot.
    offset: u64,

    /// The ID of the snapshot being written.
    snapshot_id: SnapshotId,

    temp_path: String,

    /// A handle to the snapshot writer.
    snapshot_data: BufWriter<File>,
}

impl ReceiverV1 {
    pub fn new(snapshot_id: SnapshotId, temp_path: String, f: fs::File) -> Self {
        Self {
            offset: 0,
            snapshot_id,
            temp_path,
            snapshot_data: BufWriter::with_capacity(64 * 1024 * 1024, f),
        }
    }

    /// Receive a chunk of snapshot data.
    pub fn receive(
        &mut self,
        req: InstallSnapshotRequest,
    ) -> Result<bool, RaftError<InstallSnapshotError>> {
        // Always seek to the target offset if not an exact match.
        if req.offset != self.offset {
            let mismatch = InstallSnapshotError::SnapshotMismatch(SnapshotMismatch {
                expect: SnapshotSegmentId {
                    id: req.meta.snapshot_id.clone(),
                    offset: self.offset,
                },
                got: SnapshotSegmentId {
                    id: req.meta.snapshot_id.clone(),
                    offset: req.offset,
                },
            });
            return Err(RaftError::APIError(mismatch));
        }

        // Write the next segment & update offset.
        let res = self.snapshot_data.write_all(&req.data);
        if let Err(err) = res {
            let sto_err = StorageError::from_io_error(
                ErrorSubject::Snapshot(Some(req.meta.signature())),
                ErrorVerb::Write,
                err,
            );
            return Err(RaftError::Fatal(Fatal::StorageError(sto_err)));
        }
        self.offset += req.data.len() as u64;
        Ok(req.done)
    }

    /// Flush the snapshot data and return the file handle.
    pub fn into_file(mut self) -> Result<(String, File), io::Error> {
        self.snapshot_data.flush()?;
        let f = self.snapshot_data.into_inner()?;
        f.sync_all()?;
        Ok((self.temp_path, f))
    }
}

pub(crate) async fn receive_snapshot_v1(
    receiver: &mut Option<ReceiverV1>,
    ss_store: &SnapshotStoreV003,
    req: InstallSnapshotRequest,
) -> Result<Option<String>, RaftError<InstallSnapshotError>> {
    let snapshot_id = req.meta.snapshot_id.clone();
    let snapshot_meta = req.meta.clone();
    let done = req.done;

    info!(req :? = &req; "{}", func_name!());

    let curr_id = receiver.as_ref().map(|recv_v1| recv_v1.snapshot_id.clone());

    if curr_id != Some(snapshot_id.clone()) {
        if req.offset != 0 {
            let mismatch = InstallSnapshotError::SnapshotMismatch(SnapshotMismatch {
                expect: SnapshotSegmentId {
                    id: snapshot_id.clone(),
                    offset: 0,
                },
                got: SnapshotSegmentId {
                    id: snapshot_id.clone(),
                    offset: req.offset,
                },
            });
            return Err(RaftError::APIError(mismatch));
        }

        // Changed to another stream. re-init snapshot state.
        let (temp_path, f) = ss_store.new_temp_file().map_err(|e| {
            let io_err = StorageError::write_snapshot(Some(snapshot_meta.signature()), &e);
            RaftError::Fatal(Fatal::StorageError(StorageError::from(io_err)))
        })?;

        *receiver = Some(ReceiverV1::new(snapshot_id.clone(), temp_path, f));
    }

    {
        let s = receiver.as_mut().unwrap();
        s.receive(req)?;
    }

    info!("Done received snapshot chunk");

    if done {
        let r = receiver.take().unwrap();
        let (temp_path, _f) = r.into_file().map_err(|e| {
            let io_err = StorageError::write_snapshot(Some(snapshot_meta.signature()), &e);
            StorageError::from(io_err)
        })?;

        info!("finished receiving snapshot: {:?}", snapshot_meta);
        return Ok(Some(temp_path));
    }

    Ok(None)
}
