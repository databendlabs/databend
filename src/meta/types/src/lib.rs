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

#![allow(clippy::uninlined_format_args)]
#![feature(no_sanitize)]
#![feature(lazy_cell)]

//! This crate defines data types used in meta data storage service.

mod applied_state;
mod change;
mod cluster;
mod endpoint;
mod eval_expire_time;
mod grpc_config;
mod grpc_helper;
mod log_entry;
mod match_seq;
mod message;
mod non_empty;
mod operation;
mod raft_snapshot_data;
mod raft_txid;
mod raft_types;
mod seq_errors;
mod seq_num;
mod seq_value;
mod time;
mod with;

mod proto_display;
mod proto_ext;

pub mod cmd;
pub mod config;
pub mod errors;
pub mod snapshot_db;
pub mod sys_data;

// reexport

pub use anyerror;

// ProtoBuf generated files.
#[allow(clippy::all)]
pub mod protobuf {
    tonic::include_proto!("meta");

    pub const FILE_DESCRIPTOR_SET: &[u8] = tonic::include_file_descriptor_set!("meta_descriptor");
}
pub use applied_state::AppliedState;
pub use change::Change;
pub use cluster::Node;
pub use cluster::NodeInfo;
pub use endpoint::Endpoint;
pub use errors::meta_api_errors::MetaAPIError;
pub use errors::meta_api_errors::MetaDataError;
pub use errors::meta_api_errors::MetaDataReadError;
pub use errors::meta_api_errors::MetaOperationError;
pub use errors::meta_client_errors::MetaClientError;
pub use errors::meta_errors::MetaError;
pub use errors::meta_handshake_errors::MetaHandshakeError;
pub use errors::meta_management_error::MetaManagementError;
pub use errors::meta_network_errors::ConnectionError;
pub use errors::meta_network_errors::InvalidArgument;
pub use errors::meta_network_errors::InvalidReply;
pub use errors::meta_network_errors::MetaNetworkError;
pub use errors::meta_network_errors::MetaNetworkResult;
pub use errors::meta_startup_errors::MetaStartupError;
pub use errors::rpc_errors::ForwardRPCError;
pub use eval_expire_time::EvalExpireTime;
pub use grpc_config::GrpcConfig;
pub use log_entry::LogEntry;
pub use match_seq::MatchSeq;
pub use match_seq::MatchSeqExt;
pub use operation::MetaId;
pub use operation::Operation;
pub use proto_display::VecDisplay;
pub use protobuf::txn_condition;
pub use protobuf::txn_condition::ConditionResult;
pub use protobuf::txn_op;
pub use protobuf::txn_op_response;
pub use protobuf::TxnCondition;
pub use protobuf::TxnDeleteByPrefixRequest;
pub use protobuf::TxnDeleteByPrefixResponse;
pub use protobuf::TxnDeleteRequest;
pub use protobuf::TxnDeleteResponse;
pub use protobuf::TxnGetRequest;
pub use protobuf::TxnGetResponse;
pub use protobuf::TxnOp;
pub use protobuf::TxnOpResponse;
pub use protobuf::TxnPutRequest;
pub use protobuf::TxnPutResponse;
pub use protobuf::TxnReply;
pub use protobuf::TxnRequest;
pub use raft_txid::RaftTxId;
pub use seq_errors::ConflictSeq;
pub use seq_num::SeqNum;
pub use seq_value::IntoSeqV;
pub use seq_value::KVMeta;
pub use seq_value::SeqV;
pub use seq_value::SeqValue;
pub use time::Interval;
pub use time::Time;
pub use with::With;

pub use crate::cmd::Cmd;
pub use crate::cmd::CmdContext;
pub use crate::cmd::MetaSpec;
pub use crate::cmd::UpsertKV;
pub use crate::grpc_helper::GrpcHelper;
pub use crate::non_empty::NonEmptyStr;
pub use crate::non_empty::NonEmptyString;
pub use crate::raft_snapshot_data::SnapshotData;
pub use crate::raft_snapshot_data::TempSnapshotData;
pub use crate::raft_types::new_log_id;
pub use crate::raft_types::AppendEntriesRequest;
pub use crate::raft_types::AppendEntriesResponse;
pub use crate::raft_types::ChangeMembershipError;
pub use crate::raft_types::ClientWriteError;
pub use crate::raft_types::CommittedLeaderId;
pub use crate::raft_types::Entry;
pub use crate::raft_types::EntryPayload;
pub use crate::raft_types::ErrorSubject;
pub use crate::raft_types::Fatal;
pub use crate::raft_types::ForwardToLeader;
pub use crate::raft_types::InstallSnapshotError;
pub use crate::raft_types::InstallSnapshotRequest;
pub use crate::raft_types::InstallSnapshotResponse;
pub use crate::raft_types::LogId;
pub use crate::raft_types::LogIndex;
pub use crate::raft_types::Membership;
pub use crate::raft_types::MembershipNode;
pub use crate::raft_types::NetworkError;
pub use crate::raft_types::NodeId;
pub use crate::raft_types::RPCError;
pub use crate::raft_types::RaftError;
pub use crate::raft_types::RaftMetrics;
pub use crate::raft_types::RemoteError;
pub use crate::raft_types::Snapshot;
pub use crate::raft_types::SnapshotMeta;
pub use crate::raft_types::SnapshotMismatch;
pub use crate::raft_types::SnapshotResponse;
pub use crate::raft_types::StorageError;
pub use crate::raft_types::StorageIOError;
pub use crate::raft_types::StoredMembership;
pub use crate::raft_types::StreamingError;
pub use crate::raft_types::Term;
pub use crate::raft_types::TypeConfig;
pub use crate::raft_types::Vote;
pub use crate::raft_types::VoteRequest;
pub use crate::raft_types::VoteResponse;
