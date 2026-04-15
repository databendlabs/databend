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
use databend_meta_client::types::MatchSeq;
use databend_meta_client::types::SeqV;

use super::TableId;
use super::TableLvtCheck;
use super::TableMeta;
use super::TableNameIdent;
use crate::tenant::Tenant;

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

/// Value stored in `__fd_dropped_branch/<table_id>/<branch_name>/<branch_id>`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DroppedBranchMeta {
    pub drop_on: DateTime<Utc>,
    pub expire_at: Option<DateTime<Utc>>,
}

#[derive(Clone, Debug, Eq, PartialEq, Hash)]
pub struct TableIdBranchName {
    pub table_id: u64,
    pub branch_name: String,
}

impl TableIdBranchName {
    pub fn new(table_id: u64, branch_name: impl ToString) -> Self {
        Self {
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

#[derive(Clone, Debug, Default, Eq, PartialEq, Hash)]
pub struct BranchIdToName {
    pub branch_id: u64,
}

impl Display for BranchIdToName {
    fn fmt(&self, f: &mut Formatter) -> fmt::Result {
        write!(f, "BranchIdToName{{{}}}", self.branch_id)
    }
}

#[derive(Clone, Debug, Eq, PartialEq, Hash)]
pub struct TableIdTagName {
    pub table_id: u64,
    pub tag_name: String,
}

impl TableIdTagName {
    pub fn new(table_id: u64, tag_name: impl ToString) -> Self {
        Self {
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

/// Key: `__fd_dropped_branch/<base_table_id>/<branch_name>/<branch_id>`
#[derive(Clone, Debug, Eq, PartialEq, Hash)]
pub struct DroppedBranchIdent {
    pub table_id: u64,
    pub branch_name: String,
    pub branch_id: u64,
}

impl DroppedBranchIdent {
    pub fn new(table_id: u64, branch_name: impl ToString, branch_id: u64) -> Self {
        Self {
            table_id,
            branch_name: branch_name.to_string(),
            branch_id,
        }
    }
}

impl Display for DroppedBranchIdent {
    fn fmt(&self, f: &mut Formatter) -> fmt::Result {
        write!(
            f,
            "{}.'{}'.{}",
            self.table_id, self.branch_name, self.branch_id
        )
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct CreateTableBranchReq {
    pub tenant: Tenant,
    /// Base table id that owns the branch namespace.
    pub base_table_id: u64,
    /// Fork from this branch. `None` means fork from the base table itself.
    pub from_branch_id: Option<u64>,
    pub branch_name: String,
    /// Optimistic seq check for the fork source.
    pub seq: MatchSeq,
    pub new_table_meta: TableMeta,
    pub expire_at: Option<DateTime<Utc>>,
    /// Check fork source LVT at branch creation time.
    pub lvt_check: Option<TableLvtCheck>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct DropTableBranchReq {
    pub tenant: Tenant,
    pub table_id: u64,
    pub branch_name: String,
    pub branch_id: u64,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct GetTableBranchReq {
    pub name_ident: TableNameIdent,
    pub branch_name: String,
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

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct UndropTableBranchReq {
    pub tenant: Tenant,
    pub table_id: u64,
    pub branch_name: String,
    /// Dropped branches older than this timestamp cannot be restored.
    pub retention_boundary: DateTime<Utc>,
    /// Optionally replace the restored branch expiration with a new future timestamp.
    pub new_expire_at: Option<DateTime<Utc>>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct UndropTableBranchByIdReq {
    pub tenant: Tenant,
    pub table_id: u64,
    pub branch_name: String,
    pub branch_id: u64,
    /// Dropped branches older than this timestamp cannot be restored.
    pub retention_boundary: DateTime<Utc>,
    /// Optionally replace the restored branch expiration with a new future timestamp.
    pub new_expire_at: Option<DateTime<Utc>>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct TableBranchMeta {
    pub branch_name: String,
    pub branch_id: TableId,
    pub branch_meta: SeqV<TableMeta>,
    pub expire_at: Option<DateTime<Utc>>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct CreateTableTagReq {
    pub table_id: u64,
    pub seq: MatchSeq,
    pub tag_name: String,
    pub snapshot_loc: String,
    pub expire_at: Option<DateTime<Utc>>,
    /// optimistic LVT check against the base table.
    pub lvt_check: TableLvtCheck,
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

mod kvapi_key_impl {
    use databend_meta_client::kvapi;
    use databend_meta_client::kvapi::Key;
    use databend_meta_client::kvapi::KeyBuilder;
    use databend_meta_client::kvapi::KeyError;
    use databend_meta_client::kvapi::KeyParser;

    use crate::schema::TableId;
    use crate::schema::table::BranchIdToName;
    use crate::schema::table::DroppedBranchIdent;
    use crate::schema::table::DroppedBranchMeta;
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

    impl kvapi::KeyCodec for BranchIdToName {
        fn encode_key(&self, b: KeyBuilder) -> KeyBuilder {
            b.push_u64(self.branch_id)
        }

        fn decode_key(b: &mut KeyParser) -> Result<Self, KeyError> {
            let branch_id = b.next_u64()?;
            Ok(Self { branch_id })
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

    /// "__fd_table_tag/<tb_id>/<tag_name> -> TableTag"
    impl kvapi::Key for TableIdTagName {
        const PREFIX: &'static str = "__fd_table_tag";

        type ValueType = TableTag;

        fn parent(&self) -> Option<String> {
            Some(TableId::new(self.table_id).to_string_key())
        }
    }

    /// "__fd_branch_id_to_name/<branch_id> -> TableIdBranchName"
    impl kvapi::Key for BranchIdToName {
        const PREFIX: &'static str = "__fd_branch_id_to_name";

        type ValueType = TableIdBranchName;

        fn parent(&self) -> Option<String> {
            Some(TableId::new(self.branch_id).to_string_key())
        }
    }

    impl kvapi::Value for TableBranch {
        type KeyType = TableIdBranchName;

        fn dependency_keys(&self, _key: &Self::KeyType) -> impl IntoIterator<Item = String> {
            [TableId::new(self.branch_id).to_string_key()]
        }
    }

    impl kvapi::Value for TableTag {
        type KeyType = TableIdTagName;

        fn dependency_keys(&self, _key: &Self::KeyType) -> impl IntoIterator<Item = String> {
            []
        }
    }

    impl kvapi::Value for TableIdBranchName {
        type KeyType = BranchIdToName;

        fn dependency_keys(&self, _key: &Self::KeyType) -> impl IntoIterator<Item = String> {
            [TableId::new(self.table_id).to_string_key()]
        }
    }

    impl kvapi::KeyCodec for DroppedBranchIdent {
        fn encode_key(&self, b: KeyBuilder) -> KeyBuilder {
            b.push_u64(self.table_id)
                .push_str(&self.branch_name)
                .push_u64(self.branch_id)
        }

        fn decode_key(p: &mut KeyParser) -> Result<Self, KeyError> {
            let table_id = p.next_u64()?;
            let branch_name = p.next_str()?;
            let branch_id = p.next_u64()?;
            Ok(Self {
                table_id,
                branch_name,
                branch_id,
            })
        }
    }

    /// "__fd_dropped_branch/<table_id>/<branch_name>/<branch_id> -> DroppedBranchMeta"
    impl kvapi::Key for DroppedBranchIdent {
        const PREFIX: &'static str = "__fd_dropped_branch";

        type ValueType = DroppedBranchMeta;

        fn parent(&self) -> Option<String> {
            Some(TableId::new(self.table_id).to_string_key())
        }
    }

    impl kvapi::Value for DroppedBranchMeta {
        type KeyType = DroppedBranchIdent;

        fn dependency_keys(&self, _key: &Self::KeyType) -> impl IntoIterator<Item = String> {
            []
        }
    }
}
