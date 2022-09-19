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
pub mod errors;
mod kv_message;
mod log_entry;
mod match_seq;
mod message;
mod operation;
mod raft_txid;
mod raft_types;
mod role_info;
mod seq_errors;
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
pub use applied_state::AppliedState;
pub use change::Change;
pub use cluster::Node;
pub use cluster::NodeInfo;
pub use cluster::Slot;
pub use cmd::Cmd;
pub use cmd::UpsertKV;
pub use endpoint::Endpoint;
pub use errors::app_error::AppError;
pub use errors::app_error::CreateDatabaseWithDropTime;
pub use errors::app_error::CreateTableWithDropTime;
pub use errors::app_error::DatabaseAlreadyExists;
pub use errors::app_error::DropDbWithDropTime;
pub use errors::app_error::DropTableWithDropTime;
pub use errors::app_error::ShareAlreadyExists;
pub use errors::app_error::TableAlreadyExists;
pub use errors::app_error::TableVersionMismatched;
pub use errors::app_error::UndropDbHasNoHistory;
pub use errors::app_error::UndropDbWithNoDropTime;
pub use errors::app_error::UndropTableAlreadyExists;
pub use errors::app_error::UndropTableHasNoHistory;
pub use errors::app_error::UndropTableWithNoDropTime;
pub use errors::app_error::UnknownDatabase;
pub use errors::app_error::UnknownDatabaseId;
pub use errors::app_error::UnknownShare;
pub use errors::app_error::UnknownTable;
pub use errors::app_error::UnknownTableId;
pub use errors::app_error::WrongShareObject;
pub use errors::kv_app_errors::KVAppError;
pub use errors::meta_api_errors::MetaAPIError;
pub use errors::meta_api_errors::MetaDataError;
pub use errors::meta_api_errors::MetaDataReadError;
pub use errors::meta_api_errors::MetaOperationError;
pub use errors::meta_bytes_error::MetaBytesError;
pub use errors::meta_client_errors::MetaClientError;
pub use errors::meta_errors::MetaError;
pub use errors::meta_errors::MetaResult;
pub use errors::meta_handshake_errors::MetaHandshakeError;
pub use errors::meta_management_error::MetaManagementError;
pub use errors::meta_network_errors::ConnectionError;
pub use errors::meta_network_errors::InvalidArgument;
pub use errors::meta_network_errors::InvalidReply;
pub use errors::meta_network_errors::MetaNetworkError;
pub use errors::meta_network_errors::MetaNetworkResult;
pub use errors::meta_raft_errors::ChangeMembershipError;
pub use errors::meta_raft_errors::Fatal;
pub use errors::meta_raft_errors::ForwardToLeader;
pub use errors::meta_raft_errors::InitializeError;
pub use errors::meta_raft_errors::RaftChangeMembershipError;
pub use errors::meta_raft_errors::RaftWriteError;
pub use errors::meta_raft_errors::RetryableError;
pub use errors::meta_startup_errors::MetaStartupError;
pub use errors::meta_storage_errors::MetaStorageError;
pub use errors::meta_storage_errors::MetaStorageResult;
pub use errors::rpc_errors::ForwardRPCError;
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
pub use role_info::RoleInfoSerdeError;
pub use seq_errors::ConflictSeq;
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
pub use user_setting::UserSettingValue;
pub use user_stage::*;
pub use with::With;
