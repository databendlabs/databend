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

use std::cmp::Ordering;
use std::collections::HashMap;
use std::collections::HashSet;
use std::fmt;
use std::fmt::Debug;
use std::fmt::Formatter;

pub use cluster::Node;
pub use cluster::Slot;
pub use cmd::Cmd;
pub use errors::ConflictSeq;
pub use log_entry::LogEntry;
pub use match_seq::MatchSeq;
pub use match_seq::MatchSeqExt;
pub use raft_txid::RaftTxId;
pub use raft_types::LogId;
pub use raft_types::LogIndex;
pub use raft_types::NodeId;
pub use raft_types::Term;
use serde::Deserialize;
use serde::Serialize;

mod errors;
mod match_seq;

mod cluster;
mod cmd;
mod log_entry;
mod raft_txid;
mod raft_types;

#[cfg(test)]
mod match_seq_test;

/// Value with a corresponding sequence number
pub type SeqValue<T = Vec<u8>> = (u64, T);

/// The meta data of a record in meta-store kv
#[derive(Serialize, Deserialize, Debug, Default, Clone, Eq, PartialEq)]
pub struct KVMeta {
    /// expiration time in second since 1970
    pub expire_at: Option<u64>,
}

/// Value of StateMachine generic-kv
#[derive(Serialize, Deserialize, Debug, Default, Clone, Eq, PartialEq)]
pub struct KVValue<T = Vec<u8>> {
    pub meta: Option<KVMeta>,
    pub value: T,
}

/// Compare with a timestamp to check if it is expired.
impl PartialEq<u64> for KVValue {
    fn eq(&self, other: &u64) -> bool {
        match self.meta {
            None => false,
            Some(ref m) => match m.expire_at {
                None => false,
                Some(ref exp) => exp == other,
            },
        }
    }
}

/// Compare with a timestamp to check if it is expired.
impl PartialOrd<u64> for KVValue {
    fn partial_cmp(&self, other: &u64) -> Option<Ordering> {
        match self.meta {
            None => None,
            Some(ref m) => m.expire_at.as_ref().map(|exp| exp.cmp(other)),
        }
    }
}

#[derive(Serialize, Deserialize, Debug, Default, Clone, PartialEq)]
pub struct Database {
    pub database_id: u64,

    /// engine name of db
    pub database_engine: String,

    /// tables belong to this database.
    pub tables: HashMap<String, u64>,
}

impl fmt::Display for Database {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        write!(f, "database id: {}", self.database_id)
    }
}

#[derive(Serialize, Deserialize, Debug, Default, Clone, PartialEq)]
pub struct Table {
    pub table_id: u64,

    /// serialized schema
    pub schema: Vec<u8>,

    /// table engine
    pub table_engine: String,

    /// table options
    pub table_options: HashMap<String, String>,

    /// name of parts that belong to this table.
    pub parts: HashSet<String>,
}

impl fmt::Display for Table {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        write!(f, "table id: {}", self.table_id)
    }
}

pub type MetaVersion = u64;
pub type MetaId = u64;

/// An operation that updates a field, delete it, or leave it as is.
#[derive(Serialize, Deserialize, Clone, Debug, PartialEq)]
pub enum Operation<T> {
    Update(T),
    Delete,
    AsIs,
}

impl<T> From<Option<T>> for Operation<T>
where for<'x> T: Serialize + Deserialize<'x> + Debug + Clone
{
    fn from(v: Option<T>) -> Self {
        match v {
            None => Operation::Delete,
            Some(x) => Operation::Update(x),
        }
    }
}
