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

use openraft::NodeId;
use serde::Deserialize;
use serde::Serialize;

use crate::CreateShareReq;
use crate::DatabaseMeta;
use crate::DatabaseNameIdent;
use crate::DropShareReq;
use crate::KVMeta;
use crate::MatchSeq;
use crate::Node;
use crate::Operation;
use crate::TableMeta;
use crate::TxnRequest;
use crate::UpsertTableOptionReq;

/// Compatible with latest changes made in 34e89c99 on 20220413
/// This struct can deserialize json built by the binary before and after this commit.
///
/// - 20220413: In this commit:
///   - It replaced variant struct with standalone struct.
///   - The standalone struct has a `if_exists` or `if_not_exists`.
///
///   Compatibility:
///   - Json layout for variant struct and standalone struct is the same.
///   - `if_exist` and `if_not_exist` only affects client response, but not meta data.
///     Thus it is safe to give it a default value.
///
#[derive(Serialize, Deserialize, Debug, Clone, PartialEq)]
#[allow(clippy::large_enum_variant)]
pub enum Cmd {
    IncrSeq {
        key: String,
    },

    AddNode {
        node_id: NodeId,
        node: Node,
    },

    /// Remove node, 20220427
    RemoveNode {
        node_id: NodeId,
    },

    CreateDatabase {
        // latest add
        if_not_exists: Option<bool>,
        // latest add
        name_ident: Option<DatabaseNameIdent>,
        // latest remove
        tenant: Option<String>,
        // 20220413
        name: Option<String>,
        meta: DatabaseMeta,
    },

    DropDatabase {
        // latest add
        if_exists: Option<bool>,
        // latest add
        name_ident: Option<DatabaseNameIdent>,
        tenant: Option<String>,
        // 20220413
        name: Option<String>,
    },

    CreateTable {
        // latest add
        if_not_exists: Option<bool>,
        tenant: String,
        db_name: String,
        table_name: String,
        table_meta: TableMeta,
    },

    DropTable {
        // latest add
        if_exists: Option<bool>,
        tenant: String,
        db_name: String,
        table_name: String,
    },

    RenameTable {
        // latest add
        if_exists: Option<bool>,
        tenant: String,
        db_name: String,
        table_name: String,
        new_db_name: String,
        new_table_name: String,
    },
    // latest add
    CreateShare(CreateShareReq),
    // latest add
    DropShare(DropShareReq),

    UpsertTableOptions(UpsertTableOptionReq),

    UpsertKV {
        key: String,
        seq: MatchSeq,
        value: Operation<Vec<u8>>,
        value_meta: Option<KVMeta>,
    },

    // 20220425
    Transaction(TxnRequest),
}
