// Copyright 2020-2021 The Datafuse Authors.
//
// SPDX-License-Identifier: Apache-2.0.

use async_raft::NodeId;
use serde::Deserialize;
use serde::Serialize;
use thiserror::Error;

#[derive(Error, Serialize, Deserialize, Debug, Clone, PartialEq)]
pub enum RetryableError {
    /// Trying to write to a non-leader returns the latest leader the raft node knows,
    /// to indicate the client to retry.
    #[error("request must be forwarded to leader: {leader}")]
    ForwardToLeader { leader: NodeId },
}

/// Error used to trigger Raft shutdown from storage.
#[derive(Clone, Debug, Error)]
pub enum ShutdownError {
    #[error("unsafe storage error")]
    UnsafeStorageError,
}
