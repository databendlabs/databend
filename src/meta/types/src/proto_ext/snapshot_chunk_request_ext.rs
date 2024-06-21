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

use anyerror::AnyError;

use crate::protobuf::SnapshotChunkRequest;
use crate::protobuf::SnapshotChunkRequestV003;
use crate::protobuf::SnapshotChunkV1;
use crate::protobuf::SnapshotResponseV003;
use crate::InstallSnapshotRequest;
use crate::NetworkError;
use crate::SnapshotMeta;
use crate::Vote;

impl SnapshotChunkRequest {
    /// Return the length of the data in the chunk.
    pub fn data_len(&self) -> u64 {
        self.chunk.as_ref().map_or(0, |x| x.data.len()) as u64
    }

    pub fn new_v1(r: InstallSnapshotRequest) -> Self {
        let meta = (r.vote, r.meta);
        let rpc_meta = serde_json::to_string(&meta).unwrap();

        let chunk_v1 = SnapshotChunkV1 {
            offset: r.offset,
            done: r.done,
            data: r.data,
        };

        SnapshotChunkRequest {
            ver: 1,
            rpc_meta,
            chunk: Some(chunk_v1),
        }
    }
}

impl SnapshotChunkRequestV003 {
    /// Build the last chunk of a snapshot stream, which contains vote and snapshot meta, without data.
    pub fn new_end_chunk(vote: Vote, snapshot_meta: SnapshotMeta) -> Self {
        let meta = ("rotbl::v001".to_string(), vote, snapshot_meta);
        let rpc_meta = serde_json::to_string(&meta).unwrap();

        SnapshotChunkRequestV003 {
            rpc_meta: Some(rpc_meta),
            chunk: vec![],
        }
    }

    /// Build a chunk item with data.
    pub fn new_chunk(chunk: Vec<u8>) -> Self {
        SnapshotChunkRequestV003 {
            rpc_meta: None,
            chunk,
        }
    }
}

impl SnapshotResponseV003 {
    pub fn new(vote: Vote) -> Self {
        Self {
            vote: serde_json::to_string(&vote).unwrap(),
        }
    }

    pub fn to_vote(&self) -> Result<Vote, NetworkError> {
        serde_json::from_str(&self.vote).map_err(|e| {
            NetworkError::new(
                &AnyError::new(&e).add_context(|| "when decoding vote from SnapshotResponseV003"),
            )
        })
    }
}
