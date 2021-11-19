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

use std::collections::HashMap;
use std::fmt;

use async_raft::NodeId;
use serde::Deserialize;
use serde::Serialize;

use crate::KVMeta;
use crate::MatchSeq;
use crate::Node;
use crate::Operation;
use crate::TableMeta;

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
    CreateDatabase { name: String },

    /// Drop a database if absent
    DropDatabase { name: String },

    /// Create a table if absent
    CreateTable {
        db_name: String,
        table_name: String,
        table_meta: TableMeta,
    },

    /// Drop a table if absent
    DropTable { db_name: String, table_name: String },

    /// Update, remove or insert table options.
    ///
    /// This Cmd requires a present table to operate on.
    /// Otherwise an `UnknownTableId` is returned.
    ///
    /// With mismatched seq, it returns a unchanged state: (prev:TableMeta, prev:TableMeta)
    /// Otherwise it returns the TableMeta before and after update.
    UpsertTableOptions {
        table_id: u64,

        /// The exact version of a table to operate on.
        seq: MatchSeq,

        /// Add or remove options
        ///
        /// Some(String): add or update an option.
        /// None: delete an option.
        table_options: HashMap<String, Option<String>>,
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
            Cmd::CreateDatabase { name } => {
                write!(f, "create_db:{}", name)
            }
            Cmd::DropDatabase { name } => {
                write!(f, "drop_db:{}", name)
            }
            Cmd::CreateTable {
                db_name,
                table_name,
                table_meta,
            } => {
                write!(f, "create_table:{}-{}={}", db_name, table_name, table_meta)
            }
            Cmd::DropTable {
                db_name,
                table_name,
            } => {
                write!(f, "delete_table:{}-{}", db_name, table_name)
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
            Cmd::UpsertTableOptions {
                table_id,
                seq,
                table_options,
            } => {
                write!(
                    f,
                    "upsert-table-options: table-id:{}({:?}) = {:?}",
                    table_id, seq, table_options
                )
            }
        }
    }
}
