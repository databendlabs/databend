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

pub use cluster::Node;
pub use cluster::NodeInfo;
pub use cluster::Slot;
pub use cmd::Cmd;
pub use common_meta_sled_store::ClientLastRespValue;
pub use common_meta_sled_store::KVMeta;
pub use common_meta_sled_store::KVValue;
pub use common_meta_sled_store::SeqValue;
pub use database_info::Database;
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
pub use table_info::Table;
pub use table_info::TableInfo;
pub use table_reply::CreateTableReply;

#[cfg(test)]
mod cluster_test;
#[cfg(test)]
mod match_seq_test;

mod errors;
mod match_seq;

mod cluster;
mod cmd;
mod database_info;
mod database_reply;
mod kv_reply;
mod log_entry;
mod operation;
mod raft_txid;
mod raft_types;
mod table_info;
mod table_reply;
