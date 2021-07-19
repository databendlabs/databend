// Copyright 2020-2021 The Datafuse Authors.
//
// SPDX-License-Identifier: Apache-2.0.

use std::fmt;

use async_raft::NodeId;
use common_metatypes::Database;
use common_metatypes::MatchSeq;
use common_metatypes::Table;
use serde::Deserialize;
use serde::Serialize;

use crate::meta_service::Node;

/// A Cmd describes what a user want to do to raft state machine
/// and is the essential part of a raft log.
#[derive(Serialize, Deserialize, Debug, Clone)]
pub enum Cmd {
    /// AKA put-if-absent. add a key-value record only when key is absent.
    AddFile {
        key: String,
        value: String,
    },

    /// Override the record with key.
    SetFile {
        key: String,
        value: String,
    },

    /// Increment the sequence number generator specified by `key` and returns the new value.
    IncrSeq {
        key: String,
    },

    /// Add node if absent
    AddNode {
        node_id: NodeId,
        node: Node,
    },

    /// Add a database if absent
    CreateDatabase {
        name: String,
        if_not_exists: bool,
        db: Database,
    },

    /// Drop a database if absent
    DropDatabase {
        name: String,
    },

    /// Create a table if absent
    CreateTable {
        db_name: String,
        table_name: String,
        if_not_exists: bool,
        table: Table,
    },

    /// Drop a table if absent
    DropTable {
        db_name: String,
        table_name: String,
        if_exists: bool,
    },

    /// Update or insert a general purpose kv store
    UpsertKV {
        key: String,
        /// Since a sequence number is always positive, using Exact(0) to perform an add-if-absent operation.
        /// GE(1) to perform an update-any operation.
        /// Exact(n) to perform an update on some specified version.
        /// Any to perform an update or insert that always takes effect.
        seq: MatchSeq,
        value: Vec<u8>,
    },
    DeleteKVByKey {
        key: String,
        seq: MatchSeq,
    },
}

impl fmt::Display for Cmd {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Cmd::AddFile { key, value } => {
                write!(f, "add_file:{}={}", key, value)
            }
            Cmd::SetFile { key, value } => {
                write!(f, "set_file:{}={}", key, value)
            }
            Cmd::IncrSeq { key } => {
                write!(f, "incr_seq:{}", key)
            }
            Cmd::AddNode { node_id, node } => {
                write!(f, "add_node:{}={}", node_id, node)
            }
            Cmd::CreateDatabase {
                name,
                if_not_exists,
                db,
            } => {
                write!(
                    f,
                    "create_db:{}={}, if_not_exists:{}",
                    name, db, if_not_exists
                )
            }
            Cmd::DropDatabase { name } => {
                write!(f, "drop_db:{}", name)
            }
            Cmd::CreateTable {
                db_name,
                table_name,
                if_not_exists,
                table,
            } => {
                write!(
                    f,
                    "create_table:{}-{}={}, if_not_exists:{}",
                    db_name, table_name, table, if_not_exists
                )
            }
            Cmd::DropTable {
                db_name,
                table_name,
                if_exists,
            } => {
                write!(
                    f,
                    "delete_table:{}-{}, if_exists:{}",
                    db_name, table_name, if_exists
                )
            }
            Cmd::UpsertKV { key, seq, value } => {
                write!(f, "upsert_kv: {}({:?}) = {:?}", key, seq, value)
            }
            Cmd::DeleteKVByKey { key, seq } => {
                write!(f, "delete_by_key_kv: {}({:?})", key, seq)
            }
        }
    }
}
