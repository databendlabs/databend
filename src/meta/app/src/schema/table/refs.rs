// Copyright 2021 Datafuse Labs
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
use std::fmt::Display;
use std::fmt::Formatter;

use chrono::DateTime;
use chrono::Utc;
use databend_meta_types::MatchSeq;
use databend_meta_types::SeqV;

use super::TableId;
use super::TableLvtCheck;
use super::TableMeta;
use super::TableNameIdent;
use crate::tenant::Tenant;

/// The option key for storing the base table id in a branch's TableMeta.
pub const OPT_KEY_BASE_TABLE_ID: &str = "base_table_id";

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TableBranch {
    /// After this timestamp, the branch becomes inactive.
    pub expire_at: Option<DateTime<Utc>>,
    /// The unique id of the branch.
    pub branch_id: u64,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TableTag {
    /// After this timestamp, the tag becomes inactive.
    pub expire_at: Option<DateTime<Utc>>,
    /// The location of the snapshot that the tag points to.
    pub snapshot_loc: String,
}

#[derive(Clone, Debug, Eq, PartialEq, Hash)]
pub struct TableIdBranchName {
    pub table_id: u64,
    pub branch_name: String,
}

impl TableIdBranchName {
    pub fn new(table_id: u64, branch_name: impl ToString) -> Self {
        TableIdBranchName {
            table_id,
            branch_name: branch_name.to_string(),
        }
    }
    pub fn display(&self) -> impl Display {
        format!("{}.'{}'", self.table_id, self.branch_name)
    }
}

impl Display for TableIdBranchName {
    fn fmt(&self, f: &mut Formatter) -> fmt::Result {
        write!(f, "{}.'{}'", self.table_id, self.branch_name)
    }
}

#[derive(Clone, Debug, Eq, PartialEq, Hash)]
pub struct TableIdTagName {
    pub table_id: u64,
    pub tag_name: String,
}

impl TableIdTagName {
    pub fn new(table_id: u64, tag_name: impl ToString) -> Self {
        TableIdTagName {
            table_id,
            tag_name: tag_name.to_string(),
        }
    }
    pub fn display(&self) -> impl Display {
        format!("{}.'{}'", self.table_id, self.tag_name)
    }
}

impl Display for TableIdTagName {
    fn fmt(&self, f: &mut Formatter) -> fmt::Result {
        write!(f, "{}.'{}'", self.table_id, self.tag_name)
    }
}

/// The meta-service key for storing table id history ever used by a table name
#[derive(Clone, Debug, Eq, PartialEq, Hash)]
pub struct BranchIdHistoryIdent {
    pub table_id: u64,
    pub branch_name: String,
}

impl Display for BranchIdHistoryIdent {
    fn fmt(&self, f: &mut Formatter) -> fmt::Result {
        write!(f, "{}.'{}'", self.table_id, self.branch_name)
    }
}

// -- Req types for RefApi --

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct CreateTableTagReq {
    pub table_id: u64,
    pub seq: MatchSeq,
    pub tag_name: String,
    pub snapshot_loc: String,
    pub expire_at: Option<DateTime<Utc>>,
    /// Optional optimistic LVT check against the base table.
    pub lvt_check: Option<TableLvtCheck>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct DropTableTagReq {
    pub table_id: u64,
    pub tag_name: String,
    pub seq: MatchSeq,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct GetTableTagReq {
    pub table_id: u64,
    pub tag_name: String,
    pub include_expired: bool,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ListTableTagsReq {
    pub table_id: u64,
    pub include_expired: bool,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct CreateTableBranchReq {
    pub name_ident: TableNameIdent,
    pub table_id: u64,
    pub source_table_id: u64,
    pub branch_name: String,
    pub seq: MatchSeq,
    pub table_meta: TableMeta,
    pub expire_at: Option<DateTime<Utc>>,
    /// Whether to create the branch in dropped state first.
    /// When `true`, caller must provide `table_meta.drop_on`.
    pub as_dropped: bool,
    /// Optional optimistic LVT check against the source object.
    pub lvt_check: Option<TableLvtCheck>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct CreateTableBranchReply {
    pub branch_id: u64,
    pub branch_id_seq: u64,
    pub branch_meta: TableMeta,
    pub orphan_branch_name: Option<String>,
    pub prev_branch_id: Option<u64>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct CommitTableBranchMetaReq {
    pub table_id: u64,
    pub branch_name: String,
    pub branch_id: u64,
    pub seq: MatchSeq,
    pub new_table_meta: TableMeta,
    pub expire_at: Option<DateTime<Utc>>,
    pub orphan_branch_name: String,
    pub prev_branch_id: Option<u64>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct DropTableBranchReq {
    pub tenant: Tenant,
    pub table_id: u64,
    pub branch_name: String,
    pub branch_id: u64,
    pub catalog_name: Option<String>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct UndropTableBranchReq {
    pub tenant: Tenant,
    pub table_id: u64,
    pub branch_name: String,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct GetTableBranchReq {
    pub name_ident: TableNameIdent,
    pub branch_name: String,
    pub include_expired: bool,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ListTableBranchesReq {
    pub table_id: u64,
    pub include_expired: bool,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ListHistoryTableBranchesReq {
    pub table_id: u64,
    /// `None`: return all branch histories.
    /// `Some(boundary)`: return active branches plus dropped branches whose `drop_on` is not
    /// older than the retention boundary.
    pub retention_boundary: Option<DateTime<Utc>>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct GcDroppedTableBranchReq {
    pub tenant: Tenant,
    /// Base table id.
    pub table_id: u64,
    pub branch_name: String,
    pub branch_id: u64,
}

pub type ListTableBranchMetaItem = (String, TableId, SeqV<TableMeta>);

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct HistoryTableBranchMetaItem {
    pub branch_name: String,
    pub branch_id: TableId,
    pub branch_meta: SeqV<TableMeta>,
    pub expire_at: Option<DateTime<Utc>>,
}

mod kvapi_key_impl {
    use databend_meta_kvapi::kvapi;
    use databend_meta_kvapi::kvapi::KeyBuilder;
    use databend_meta_kvapi::kvapi::KeyError;
    use databend_meta_kvapi::kvapi::KeyParser;

    use crate::schema::TableId;
    use crate::schema::TableIdList;
    use crate::schema::table::BranchIdHistoryIdent;
    use crate::schema::table::TableBranch;
    use crate::schema::table::TableIdBranchName;
    use crate::schema::table::TableIdTagName;
    use crate::schema::table::TableTag;

    impl kvapi::KeyCodec for TableIdBranchName {
        fn encode_key(&self, b: KeyBuilder) -> KeyBuilder {
            b.push_u64(self.table_id).push_str(&self.branch_name)
        }

        fn decode_key(b: &mut KeyParser) -> Result<Self, KeyError> {
            let table_id = b.next_u64()?;
            let branch_name = b.next_str()?;
            Ok(Self {
                table_id,
                branch_name,
            })
        }
    }

    /// "__fd_table_branch/<tb_id>/<branch_name> -> TableBranch"
    impl kvapi::Key for TableIdBranchName {
        const PREFIX: &'static str = "__fd_table_branch";

        type ValueType = TableBranch;

        fn parent(&self) -> Option<String> {
            Some(TableId::new(self.table_id).to_string_key())
        }
    }

    impl kvapi::KeyCodec for TableIdTagName {
        fn encode_key(&self, b: KeyBuilder) -> KeyBuilder {
            b.push_u64(self.table_id).push_str(&self.tag_name)
        }

        fn decode_key(b: &mut KeyParser) -> Result<Self, KeyError> {
            let table_id = b.next_u64()?;
            let tag_name = b.next_str()?;
            Ok(Self { table_id, tag_name })
        }
    }

    /// "__fd_table_tag/<tb_id>/<tag_name> -> TableTag"
    impl kvapi::Key for TableIdTagName {
        const PREFIX: &'static str = "__fd_table_tag";

        type ValueType = TableTag;

        fn parent(&self) -> Option<String> {
            Some(TableId::new(self.table_id).to_string_key())
        }
    }

    impl kvapi::Value for TableBranch {
        type KeyType = TableIdBranchName;
        fn dependency_keys(&self, _key: &Self::KeyType) -> impl IntoIterator<Item = String> {
            []
        }
    }

    impl kvapi::Value for TableTag {
        type KeyType = TableIdTagName;
        fn dependency_keys(&self, _key: &Self::KeyType) -> impl IntoIterator<Item = String> {
            []
        }
    }

    impl kvapi::KeyCodec for BranchIdHistoryIdent {
        fn encode_key(&self, b: KeyBuilder) -> KeyBuilder {
            b.push_u64(self.table_id).push_str(&self.branch_name)
        }

        fn decode_key(b: &mut KeyParser) -> Result<Self, KeyError> {
            let table_id = b.next_u64()?;
            let branch_name = b.next_str()?;
            Ok(Self {
                table_id,
                branch_name,
            })
        }
    }

    /// "__fd_branch_id_list/<tb_id>/<branch_name> -> id_list"
    impl kvapi::Key for BranchIdHistoryIdent {
        const PREFIX: &'static str = "__fd_branch_id_list";

        // reuse the TableIdList for branch id list.
        type ValueType = TableIdList;

        fn parent(&self) -> Option<String> {
            Some(TableId::new(self.table_id).to_string_key())
        }
    }
}
