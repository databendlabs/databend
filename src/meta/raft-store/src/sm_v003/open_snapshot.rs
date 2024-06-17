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

use openraft::SnapshotId;

use crate::config::RaftConfig;

/// A trait for opening a snapshot.
pub trait OpenSnapshot {
    fn open_snapshot(
        path: impl ToString,
        snapshot_id: SnapshotId,
        raft_config: &RaftConfig,
    ) -> Result<Self, std::io::Error>
    where
        Self: Sized;
}
