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

use std::fmt;

use openraft::NodeId;
use serde::Deserialize;
use serde::Serialize;

use crate::CreateDatabaseReq;
use crate::CreateShareReq;
use crate::CreateTableReq;
use crate::DropDatabaseReq;
use crate::DropShareReq;
use crate::DropTableReq;
use crate::KVMeta;
use crate::MatchSeq;
use crate::Node;
use crate::Operation;
use crate::RenameTableReq;
use crate::TxnRequest;
use crate::UpsertTableOptionReq;

/// A Cmd describes what a user want to do to raft state machine
/// and is the essential part of a raft log.
#[derive(Serialize, Deserialize, Debug, Clone, PartialEq)]
#[allow(clippy::large_enum_variant)]
pub enum Cmd {
    /// Increment the sequence number generator specified by `key` and returns the new value.
    IncrSeq {
        key: String,
    },

    /// Add node if absent
    AddNode {
        node_id: NodeId,
        node: Node,
    },

    /// Remove node
    RemoveNode {
        node_id: NodeId,
    },

    /// Add a database if absent
    CreateDatabase(CreateDatabaseReq),

    /// Drop a database if absent
    DropDatabase(DropDatabaseReq),

    /// Create a table if absent
    CreateTable(CreateTableReq),

    /// Drop a table if absent
    DropTable(DropTableReq),

    /// Rename a table
    RenameTable(RenameTableReq),

    /// Create a share if absent
    CreateShare(CreateShareReq),

    DropShare(DropShareReq),

    /// Update, remove or insert table options.
    ///
    /// This Cmd requires a present table to operate on.
    /// Otherwise an `UnknownTableId` is returned.
    ///
    /// With mismatched seq, it returns a unchanged state: (prev:TableMeta, prev:TableMeta)
    /// Otherwise it returns the TableMeta before and after update.
    UpsertTableOptions(UpsertTableOptionReq),

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

    Transaction(TxnRequest),
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
            Cmd::RemoveNode { node_id } => {
                write!(f, "remove_node:{}", node_id)
            }

            Cmd::CreateDatabase(req) => req.fmt(f),
            Cmd::DropDatabase(req) => req.fmt(f),
            Cmd::CreateTable(req) => req.fmt(f),
            Cmd::DropTable(req) => req.fmt(f),
            Cmd::RenameTable(req) => req.fmt(f),
            Cmd::UpsertTableOptions(req) => req.fmt(f),
            Cmd::CreateShare(req) => req.fmt(f),
            Cmd::DropShare(req) => req.fmt(f),
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
            Cmd::Transaction(txn) => {
                write!(f, "txn:{:?}", txn)
            }
        }
    }
}
