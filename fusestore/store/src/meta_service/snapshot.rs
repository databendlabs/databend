// Copyright 2020-2021 The Datafuse Authors.
//
// SPDX-License-Identifier: Apache-2.0.

use async_raft::SnapshotMeta;
use serde::Deserialize;
use serde::Serialize;

/// The application snapshot type which the `MetaStore` works with.
#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct Snapshot {
    pub meta: SnapshotMeta,

    /// The data of the state machine at the time of this snapshot.
    pub data: Vec<u8>,
}
