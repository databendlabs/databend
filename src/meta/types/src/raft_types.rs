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

pub type Membership = openraft::Membership<NodeId, MembershipNode>;
pub type StoredMembership = openraft::StoredMembership<NodeId, MembershipNode>;

pub type EntryPayload = openraft::EntryPayload<TypeConfig>;
pub type Entry = openraft::Entry<TypeConfig>;

pub type SnapshotMeta = openraft::SnapshotMeta<NodeId, MembershipNode>;
pub type Snapshot = openraft::Snapshot<TypeConfig>;
#[allow(dead_code)]
pub type SnapshotSegmentId = openraft::SnapshotSegmentId;

pub type RaftMetrics = openraft::RaftMetrics<NodeId, MembershipNode>;

pub type ErrorSubject = openraft::ErrorSubject<NodeId>;

pub type RPCError<E> = openraft::error::RPCError<NodeId, MembershipNode, E>;
pub type RemoteError<E> = openraft::error::RemoteError<NodeId, MembershipNode, E>;
pub type RaftError<E = openraft::error::Infallible> = openraft::error::RaftError<NodeId, E>;
pub type NetworkError = openraft::error::NetworkError;

pub type StorageError = openraft::StorageError<NodeId>;
pub type StorageIOError = openraft::StorageIOError<NodeId>;
pub type ForwardToLeader = openraft::error::ForwardToLeader<NodeId, MembershipNode>;
pub type Fatal = openraft::error::Fatal<NodeId>;
pub type ChangeMembershipError = openraft::error::ChangeMembershipError<NodeId>;
pub type ClientWriteError = openraft::error::ClientWriteError<NodeId, MembershipNode>;
pub type InitializeError = openraft::error::InitializeError<NodeId, MembershipNode>;
pub type StreamingError<E = openraft::error::Infallible> =
    openraft::error::StreamingError<TypeConfig, E>;

pub type AppendEntriesRequest = openraft::raft::AppendEntriesRequest<TypeConfig>;
pub type AppendEntriesResponse = openraft::raft::AppendEntriesResponse<NodeId>;
pub type InstallSnapshotRequest = openraft::raft::InstallSnapshotRequest<TypeConfig>;
pub type InstallSnapshotResponse = openraft::raft::InstallSnapshotResponse<NodeId>;
pub type SnapshotResponse = openraft::raft::SnapshotResponse<NodeId>;
pub type InstallSnapshotError = openraft::error::InstallSnapshotError;
pub type SnapshotMismatch = openraft::error::SnapshotMismatch;
pub type VoteRequest = openraft::raft::VoteRequest<NodeId>;
pub type VoteResponse = openraft::raft::VoteResponse<NodeId>;

pub fn new_log_id(term: u64, node_id: NodeId, index: u64) -> LogId {
    LogId::new(CommittedLeaderId::new(term, node_id), index)
}
