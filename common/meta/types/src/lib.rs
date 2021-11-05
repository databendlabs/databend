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

#[cfg(test)]
mod cluster_test;
#[cfg(test)]
mod match_seq_test;

pub use change::AddResult;
pub use change::Change;
pub use cluster::Node;
pub use cluster::NodeInfo;
pub use cluster::Slot;
pub use cmd::Cmd;
pub use commit_table_reply::UpsertTableOptionReply;
pub use database_info::DatabaseInfo;
pub use database_reply::CreateDatabaseReply;
pub use errors::ConflictSeq;
pub use kv_reply::GetKVActionReply;
pub use kv_reply::MGetKVActionReply;
pub use kv_reply::PrefixListReply;
pub use kv_reply::UpsertKVActionReply;
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
pub use table_info::TableIdent;
pub use table_info::TableInfo;
pub use table_info::TableMeta;
pub use table_reply::CreateTableReply;
pub use user::AuthType;

mod change;
mod cluster;
mod cmd;
mod commit_table_reply;
mod database_info;
mod database_reply;
mod errors;
mod kv_reply;
mod log_entry;
mod match_seq;
mod operation;
mod raft_txid;
mod raft_types;
mod seq_num;
mod seq_value;
mod table_info;
mod table_reply;
mod user;
