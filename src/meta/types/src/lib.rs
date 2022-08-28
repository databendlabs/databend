// Copyright 2021 Datafuse Labs.
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

#![feature(backtrace)]

//! This crate defines data types used in meta data storage service.

mod applied_state;
mod change;
mod cluster;
mod cmd;
pub mod config;
mod endpoint;
mod errors;
mod kv_message;
mod log_entry;
mod match_seq;
mod message;
mod meta_errors;
mod meta_errors_into;
mod meta_network_errors;
mod meta_raft_errors;
mod meta_result_error;
mod meta_storage_errors;
mod operation;
mod raft_txid;
mod raft_types;
mod role_info;
mod seq_num;
mod seq_value;
mod tenant_quota;
mod user_auth;
mod user_defined_function;
mod user_grant;
mod user_identity;
mod user_info;
mod user_privilege;
mod user_quota;
mod user_setting;
mod user_stage;
mod with;

pub mod app_error;
pub mod error_context;
mod principal_identity;
mod proto_display;

// reexport

pub use anyerror;

// ProtoBuf generated files.
#[allow(clippy::all)]
pub mod protobuf {
    tonic::include_proto!("meta");

    pub const FILE_DESCRIPTOR_SET: &[u8] = tonic::include_file_descriptor_set!("meta_descriptor");
}

pub use app_error::AppError;
pub use app_error::CreateDatabaseWithDropTime;
pub use app_error::CreateTableWithDropTime;
pub use app_error::DatabaseAlreadyExists;
pub use app_error::DropDbWithDropTime;
pub use app_error::DropTableWithDropTime;
pub use app_error::ShareAlreadyExists;
pub use app_error::TableAlreadyExists;
pub use app_error::TableVersionMismatched;
pub use app_error::UndropDbHasNoHistory;
pub use app_error::UndropDbWithNoDropTime;
pub use app_error::UndropTableAlreadyExists;
pub use app_error::UndropTableHasNoHistory;
pub use app_error::UndropTableWithNoDropTime;
pub use app_error::UnknownDatabase;
pub use app_error::UnknownDatabaseId;
pub use app_error::UnknownShare;
pub use app_error::UnknownTable;
pub use app_error::UnknownTableId;
pub use app_error::WrongShareObject;
pub use applied_state::AppliedState;
pub use change::AddResult;
pub use change::Change;
pub use change::OkOrExist;
pub use cluster::Node;
pub use cluster::NodeInfo;
pub use cluster::Slot;
pub use cmd::Cmd;
pub use cmd::UpsertKV;
pub use endpoint::Endpoint;
pub use errors::ConflictSeq;
pub use kv_message::GetKVReply;
pub use kv_message::GetKVReq;
pub use kv_message::ListKVReply;
pub use kv_message::ListKVReq;
pub use kv_message::MGetKVReply;
pub use kv_message::MGetKVReq;
pub use kv_message::UpsertKVReply;
pub use kv_message::UpsertKVReq;
pub use log_entry::LogEntry;
pub use match_seq::MatchSeq;
pub use match_seq::MatchSeqExt;
pub use message::ForwardRequest;
pub use message::ForwardRequestBody;
pub use message::ForwardResponse;
pub use message::JoinRequest;
pub use message::LeaveRequest;
pub use meta_errors::MetaError;
pub use meta_errors::MetaResult;
pub use meta_errors_into::ToMetaError;
pub use meta_network_errors::ConnectionError;
pub use meta_network_errors::InvalidArgument;
pub use meta_network_errors::MetaNetworkError;
pub use meta_network_errors::MetaNetworkResult;
pub use meta_raft_errors::ForwardToLeader;
pub use meta_raft_errors::MetaRaftError;
pub use meta_raft_errors::RetryableError;
pub use meta_result_error::MetaResultError;
pub use meta_storage_errors::MetaStorageError;
pub use meta_storage_errors::MetaStorageResult;
pub use operation::GCDroppedDataReply;
pub use operation::GCDroppedDataReq;
pub use operation::MetaId;
pub use operation::Operation;
pub use principal_identity::PrincipalIdentity;
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
pub use raft_types::LogId;
pub use raft_types::LogIndex;
pub use raft_types::NodeId;
pub use raft_types::Term;
pub use role_info::RoleInfo;
pub use seq_num::SeqNum;
pub use seq_value::IntoSeqV;
pub use seq_value::KVMeta;
pub use seq_value::PbSeqV;
pub use seq_value::SeqV;
pub use tenant_quota::TenantQuota;
pub use user_auth::AuthInfo;
pub use user_auth::AuthType;
pub use user_auth::PasswordHashMethod;
pub use user_defined_function::UserDefinedFunction;
pub use user_grant::GrantEntry;
pub use user_grant::GrantObject;
pub use user_grant::UserGrantSet;
pub use user_identity::UserIdentity;
pub use user_info::UserInfo;
pub use user_info::UserOption;
pub use user_info::UserOptionFlag;
pub use user_privilege::UserPrivilegeSet;
pub use user_privilege::UserPrivilegeType;
pub use user_quota::UserQuota;
pub use user_setting::UserSetting;
pub use user_stage::*;
pub use with::With;
