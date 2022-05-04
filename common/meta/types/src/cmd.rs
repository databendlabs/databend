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
use serde::de;
use serde::Deserialize;
use serde::Deserializer;
use serde::Serialize;

use crate::compatibility::cmd_00000000_20220427::Cmd as LatestVersionCmd;
use crate::CreateDatabaseReq;
use crate::CreateShareReq;
use crate::CreateTableReq;
use crate::DatabaseNameIdent;
use crate::DropDatabaseReq;
use crate::DropShareReq;
use crate::DropTableReq;
use crate::KVMeta;
use crate::MatchSeq;
use crate::Node;
use crate::Operation;
use crate::RenameTableReq;
use crate::TableNameIdent;
use crate::TxnRequest;
use crate::UpsertTableOptionReq;

/// A Cmd describes what a user want to do to raft state machine
/// and is the essential part of a raft log.
#[derive(Serialize, Debug, Clone, PartialEq)]
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

impl<'de> Deserialize<'de> for Cmd {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where D: Deserializer<'de> {
        let c: LatestVersionCmd = de::Deserialize::deserialize(deserializer)?;
        let latest = match c {
            LatestVersionCmd::IncrSeq { key } => Cmd::IncrSeq { key },
            LatestVersionCmd::AddNode { node_id, node } => Cmd::AddNode { node_id, node },
            LatestVersionCmd::RemoveNode { node_id } => Cmd::RemoveNode { node_id },
            LatestVersionCmd::CreateDatabase {
                if_not_exists,
                name_ident,
                name,
                tenant,
                meta,
            } => {
                if let Some(x) = if_not_exists {
                    // latest
                    Cmd::CreateDatabase(CreateDatabaseReq {
                        if_not_exists: x,
                        name_ident: name_ident.unwrap(),
                        meta,
                    })
                } else {
                    // 20220413
                    Cmd::CreateDatabase(CreateDatabaseReq {
                        if_not_exists: false,
                        name_ident: DatabaseNameIdent {
                            tenant: tenant.unwrap(),
                            db_name: name.unwrap(),
                        },
                        meta,
                    })
                }
            }
            LatestVersionCmd::DropDatabase {
                if_exists,
                name_ident,
                tenant,
                name,
            } => {
                if let Some(x) = if_exists {
                    // latest
                    Cmd::DropDatabase(DropDatabaseReq {
                        if_exists: x,
                        name_ident: name_ident.unwrap(),
                    })
                } else {
                    // 20220413
                    Cmd::DropDatabase(DropDatabaseReq {
                        if_exists: false,
                        name_ident: DatabaseNameIdent {
                            tenant: tenant.unwrap(),
                            db_name: name.unwrap(),
                        },
                    })
                }
            }
            LatestVersionCmd::CreateTable {
                if_not_exists,
                name_ident,
                tenant,
                db_name,
                table_name,
                table_meta,
            } => {
                // since 20220413 there is an `if_exists` field.
                let if_not_exists = if_not_exists.unwrap_or_default();

                if let Some(ni) = name_ident {
                    Cmd::CreateTable(CreateTableReq {
                        if_not_exists,
                        name_ident: ni,
                        table_meta,
                    })
                } else {
                    Cmd::CreateTable(CreateTableReq {
                        if_not_exists,
                        name_ident: TableNameIdent {
                            tenant: tenant.unwrap(),
                            db_name: db_name.unwrap(),
                            table_name: table_name.unwrap(),
                        },
                        table_meta,
                    })
                }
            }
            LatestVersionCmd::DropTable {
                if_exists,
                name_ident,
                tenant,
                db_name,
                table_name,
            } => {
                // since 20220413 there is an `if_exists` field.
                let if_exists = if_exists.unwrap_or_default();

                if let Some(ni) = name_ident {
                    Cmd::DropTable(DropTableReq {
                        if_exists,
                        name_ident: ni,
                    })
                } else {
                    Cmd::DropTable(DropTableReq {
                        if_exists,
                        name_ident: TableNameIdent {
                            tenant: tenant.unwrap(),
                            db_name: db_name.unwrap(),
                            table_name: table_name.unwrap(),
                        },
                    })
                }
            }
            LatestVersionCmd::RenameTable {
                if_exists,
                tenant,
                db_name,
                table_name,
                new_db_name,
                new_table_name,
            } => {
                // since 20220413 there is an `if_exists` field.
                let if_exists = if_exists.unwrap_or_default();

                Cmd::RenameTable(RenameTableReq {
                    if_exists,
                    tenant,
                    db_name,
                    table_name,
                    new_db_name,
                    new_table_name,
                })
            }
            LatestVersionCmd::CreateShare(x) => Cmd::CreateShare(x),
            LatestVersionCmd::DropShare(x) => Cmd::DropShare(x),
            LatestVersionCmd::UpsertTableOptions(x) => Cmd::UpsertTableOptions(x),
            LatestVersionCmd::UpsertKV {
                key,
                seq,
                value,
                value_meta,
            } => Cmd::UpsertKV {
                key,
                seq,
                value,
                value_meta,
            },
            LatestVersionCmd::Transaction(txn) => Cmd::Transaction(txn),
        };

        Ok(latest)
    }
}
