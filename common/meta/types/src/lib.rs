// Copyright 2020 Datafuse Labs.
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

//! This crate defines data types used in meta data storage service.

pub use change::AddResult;
pub use change::Change;
pub use cluster::Node;
pub use cluster::NodeInfo;
pub use cluster::Slot;
pub use cmd::Cmd;
pub use commit_table_reply::UpsertTableOptionReply;
pub use database::CreateDatabaseReply;
pub use database::CreateDatabaseReq;
pub use database::DatabaseInfo;
pub use database::DropDatabaseReply;
pub use database::DropDatabaseReq;
pub use errors::ConflictSeq;
pub use kv_message::GetKVActionReply;
pub use kv_message::MGetKVActionReply;
pub use kv_message::PrefixListReply;
pub use kv_message::UpsertKVAction;
pub use kv_message::UpsertKVActionReply;
pub use log_entry::LogEntry;
pub use match_seq::MatchSeq;
pub use match_seq::MatchSeqExt;
pub use operation::MetaId;
pub use operation::MetaVersion;
pub use operation::Operation;
pub use raft_txid::RaftTxId;
pub use raft_types::LogId;
pub use raft_types::LogIndex;
pub use raft_types::NodeId;
pub use raft_types::Term;
pub use seq_num::SeqNum;
pub use seq_value::IntoSeqV;
pub use seq_value::KVMeta;
pub use seq_value::SeqV;
pub use table::CreateTableReply;
pub use table::CreateTableReq;
pub use table::TableIdent;
pub use table::TableInfo;
pub use table::TableMeta;
pub use user_auth::AuthType;
pub use user_privilege::UserPrivilege;
pub use user_privilege::UserPrivilegeType;
pub use user_quota::UserQuota;

mod change;
mod cluster;
mod cmd;
mod commit_table_reply;
mod database;
mod errors;
mod kv_message;
mod log_entry;
mod match_seq;
mod operation;
mod raft_txid;
mod raft_types;
mod seq_num;
mod seq_value;
mod table;
mod user_auth;
mod user_privilege;
mod user_quota;
