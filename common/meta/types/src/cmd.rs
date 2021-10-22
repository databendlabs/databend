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

use std::fmt;

use async_raft::NodeId;
use serde::Deserialize;
use serde::Serialize;

use crate::DatabaseInfo;
use crate::KVMeta;
use crate::MatchSeq;
use crate::Node;
use crate::Operation;
use crate::TableInfo;

/// A Cmd describes what a user want to do to raft state machine
/// and is the essential part of a raft log.
#[derive(Serialize, Deserialize, Debug, Clone, PartialEq)]
#[allow(clippy::large_enum_variant)]
pub enum Cmd {
    /// Increment the sequence number generator specified by `key` and returns the new value.
    IncrSeq { key: String },

    /// Add node if absent
    AddNode { node_id: NodeId, node: Node },

    /// Add a database if absent
    CreateDatabase {
        // TODO(ariesdevil): add `seq` for distinguish between the results of the execution of
        // the two commands (failed `add` and successful `delete`)
        name: String,
        db: DatabaseInfo,
    },

    /// Drop a database if absent
    DropDatabase {
        // TODO(ariesdevil): add `seq` for distinguish between the results of the execution of
        // the two commands (failed `add` and successful `delete`)
        name: String,
    },

    /// Create a table if absent
    CreateTable {
        // TODO(ariesdevil): add `seq` for distinguish between the results of the execution of
        // the two commands (failed `add` and successful `delete`)
        db_name: String,
        table_name: String,
        table: TableInfo,
    },

    /// Drop a table if absent
    DropTable {
        // TODO(ariesdevil): add `seq` for distinguish between the results of the execution of
        // the two commands (failed `add` and successful `delete`)
        db_name: String,
        table_name: String,
    },

    /// Update or insert a general purpose kv store
    UpsertKV {
        key: String,

        /// Since a sequence number is always positive, using Exact(0) to perform an add-if-absent operation.
        /// - GE(1) to perform an update-any operation.
        /// - Exact(n) to perform an update on some specified version.
        /// - Any to perform an update or insert that always takes effect.
        seq: MatchSeq,

        /// The value to set. A `None` indicates to delete it.
        value: Operation<Vec<u8>>,

        /// Meta data of a value.
        value_meta: Option<KVMeta>,
    },
}

impl fmt::Display for Cmd {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Cmd::IncrSeq { key } => {
                write!(f, "incr_seq:{}", key)
            }
            Cmd::AddNode { node_id, node } => {
                write!(f, "add_node:{}={}", node_id, node)
            }
            Cmd::CreateDatabase { name, db } => {
                write!(f, "create_db:{}={:?}", name, db)
            }
            Cmd::DropDatabase { name } => {
                write!(f, "drop_db:{}", name)
            }
            Cmd::CreateTable {
                db_name,
                table_name,
                table,
            } => {
                write!(f, "create_table:{}-{}={}", db_name, table_name, table)
            }
            Cmd::DropTable {
                db_name,
                table_name,
            } => {
                write!(
                    f,
                    "delete_table:{}-{}, if_exists:{}",
                    db_name, table_name, if_exists
                )
            }
            Cmd::UpsertKV {
                key,
                seq,
                value,
                value_meta,
            } => {
                write!(
                    f,
                    "upsert_kv: {}({:?}) = {:?} ({:?})",
                    key, seq, value, value_meta
                )
            }
        }
    }
}
