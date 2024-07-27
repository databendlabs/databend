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

//! This mod wraps openraft types that have generics parameter with concrete types.

use openraft::impls::OneshotResponder;
use openraft::RaftTypeConfig;
use openraft::TokioRuntime;

use crate::snapshot_db::DB;
use crate::AppliedState;
use crate::LogEntry;

pub type NodeId = u64;
pub type MembershipNode = openraft::EmptyNode;
pub type LogIndex = u64;
pub type Term = u64;

#[derive(Debug, Clone, Copy, Default, Eq, PartialEq, Ord, PartialOrd)]
pub struct TypeConfig {}
impl RaftTypeConfig for TypeConfig {
    type D = LogEntry;
    type R = AppliedState;
    type NodeId = NodeId;
    type Node = MembershipNode;
    type Entry = openraft::entry::Entry<TypeConfig>;
    type SnapshotData = DB;
    type AsyncRuntime = TokioRuntime;
    type Responder = OneshotResponder<TypeConfig>;
}

pub type CommittedLeaderId = openraft::CommittedLeaderId<NodeId>;
pub type LogId = openraft::LogId<NodeId>;
pub type Vote = openraft::Vote<NodeId>;

pub type Membership = openraft::Membership<TypeConfig>;
pub type StoredMembership = openraft::StoredMembership<TypeConfig>;

pub type EntryPayload = openraft::EntryPayload<TypeConfig>;
pub type Entry = openraft::Entry<TypeConfig>;

pub type SnapshotMeta = openraft::SnapshotMeta<TypeConfig>;
pub type Snapshot = openraft::Snapshot<TypeConfig>;
#[allow(dead_code)]
pub type SnapshotSegmentId = openraft::SnapshotSegmentId;

pub type RaftMetrics = openraft::RaftMetrics<TypeConfig>;

pub type ErrorSubject = openraft::ErrorSubject<TypeConfig>;

pub type RPCError<E = openraft::error::Infallible> = openraft::error::RPCError<TypeConfig, E>;
pub type RemoteError<E> = openraft::error::RemoteError<TypeConfig, E>;
pub type RaftError<E = openraft::error::Infallible> = openraft::error::RaftError<TypeConfig, E>;
pub type NetworkError = openraft::error::NetworkError;

pub type StorageError = openraft::StorageError<TypeConfig>;
pub type ForwardToLeader = openraft::error::ForwardToLeader<TypeConfig>;
pub type Fatal = openraft::error::Fatal<TypeConfig>;
pub type ChangeMembershipError = openraft::error::ChangeMembershipError<TypeConfig>;
pub type ClientWriteError = openraft::error::ClientWriteError<TypeConfig>;
pub type InitializeError = openraft::error::InitializeError<TypeConfig>;
pub type StreamingError<E = openraft::error::Infallible> =
    openraft::error::StreamingError<TypeConfig, E>;

pub type AppendEntriesRequest = openraft::raft::AppendEntriesRequest<TypeConfig>;
pub type AppendEntriesResponse = openraft::raft::AppendEntriesResponse<TypeConfig>;
pub type InstallSnapshotRequest = openraft::raft::InstallSnapshotRequest<TypeConfig>;
pub type InstallSnapshotResponse = openraft::raft::InstallSnapshotResponse<TypeConfig>;
pub type SnapshotResponse = openraft::raft::SnapshotResponse<TypeConfig>;
pub type InstallSnapshotError = openraft::error::InstallSnapshotError;
pub type SnapshotMismatch = openraft::error::SnapshotMismatch;
pub type VoteRequest = openraft::raft::VoteRequest<TypeConfig>;
pub type VoteResponse = openraft::raft::VoteResponse<TypeConfig>;

pub fn new_log_id(term: u64, node_id: NodeId, index: u64) -> LogId {
    LogId::new(CommittedLeaderId::new(term, node_id), index)
}
