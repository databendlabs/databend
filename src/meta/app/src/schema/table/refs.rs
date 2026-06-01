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
use databend_meta_client::kvapi;
use databend_meta_client::types::MatchSeq;
use databend_meta_client::types::SeqV;

use super::TableId;
use super::TableLvtCheck;
use super::TableMeta;
use crate::tenant::Tenant;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TableTag {
    /// After this timestamp, the tag becomes inactive.
    pub expire_at: Option<DateTime<Utc>>,
    /// The location of the snapshot that the tag points to.
    pub snapshot_loc: String,
}

/// `__fd_table_tag/<tb_id>/<tag_name> -> TableTag`
#[derive(Clone, Debug, Eq, PartialEq, Hash, kvapi::StructKey)]
#[structkey(prefix = "__fd_table_tag")]
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

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TableBranch {
    /// After this timestamp, the branch becomes inactive.
    pub expire_at: Option<DateTime<Utc>>,
    /// The unique id of the branch.
    pub branch_id: u64,
}

/// `__fd_table_branch/<tb_id>/<branch_name> -> TableBranch`
#[derive(Clone, Debug, Eq, PartialEq, Hash, kvapi::StructKey)]
#[structkey(prefix = "__fd_table_branch")]
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

/// `__fd_branch_id_to_name/<branch_id> -> TableIdBranchName`
#[derive(Clone, Debug, Default, Eq, PartialEq, Hash, kvapi::StructKey)]
#[structkey(prefix = "__fd_branch_id_to_name")]
pub struct BranchIdToName {
    pub branch_id: u64,
}

impl Display for BranchIdToName {
    fn fmt(&self, f: &mut Formatter) -> fmt::Result {
        write!(f, "BranchIdToName{{{}}}", self.branch_id)
    }
}

/// Value stored in `__fd_dropped_branch/<table_id>/<branch_name>/<branch_id>`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DroppedBranchMeta {
    pub drop_on: DateTime<Utc>,
    pub expire_at: Option<DateTime<Utc>>,
}

/// Key: `__fd_dropped_branch/<base_table_id>/<branch_name>/<branch_id>`
#[derive(Clone, Debug, Eq, PartialEq, Hash, kvapi::StructKey)]
#[structkey(prefix = "__fd_dropped_branch")]
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
    pub source_branch_id: Option<u64>,
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
    pub table_id: u64,
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
    /// Optionally replace the restored branch expiration with a new future timestamp.
    pub new_expire_at: Option<DateTime<Utc>>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct UndropTableBranchByIdReq {
    pub tenant: Tenant,
    pub table_id: u64,
    pub branch_id: u64,
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

    use crate::schema::table::BranchIdToName;
    use crate::schema::table::DroppedBranchIdent;
    use crate::schema::table::DroppedBranchMeta;
    use crate::schema::table::TableBranch;
    use crate::schema::table::TableIdBranchName;
    use crate::schema::table::TableIdTagName;
    use crate::schema::table::TableTag;

    /// "__fd_table_branch/<tb_id>/<branch_name> -> TableBranch"
    impl kvapi::Key for TableIdBranchName {
        type ValueType = TableBranch;
    }

    /// "__fd_table_tag/<tb_id>/<tag_name> -> TableTag"
    impl kvapi::Key for TableIdTagName {
        type ValueType = TableTag;
    }

    /// "__fd_branch_id_to_name/<branch_id> -> TableIdBranchName"
    impl kvapi::Key for BranchIdToName {
        type ValueType = TableIdBranchName;
    }

    impl kvapi::Value for TableBranch {
        type KeyType = TableIdBranchName;
    }

    impl kvapi::Value for TableTag {
        type KeyType = TableIdTagName;
    }

    impl kvapi::Value for TableIdBranchName {
        type KeyType = BranchIdToName;
    }

    /// "__fd_dropped_branch/<table_id>/<branch_name>/<branch_id> -> DroppedBranchMeta"
    impl kvapi::Key for DroppedBranchIdent {
        type ValueType = DroppedBranchMeta;
    }

    impl kvapi::Value for DroppedBranchMeta {
        type KeyType = DroppedBranchIdent;
    }
}

#[cfg(test)]
mod tests {
    use databend_meta_client::kvapi::testing::assert_round_trip;

    use super::BranchIdToName;
    use super::DroppedBranchIdent;
    use super::TableIdBranchName;
    use super::TableIdTagName;

    #[test]
    fn test_table_id_tag_name_key_format() {
        assert_round_trip(TableIdTagName::new(9, "tag/a"), "__fd_table_tag/9/tag%2fa");
    }

    #[test]
    fn test_table_id_branch_name_key_format() {
        assert_round_trip(
            TableIdBranchName::new(7, "branch/a"),
            "__fd_table_branch/7/branch%2fa",
        );
    }

    #[test]
    fn test_branch_id_to_name_key_format() {
        assert_round_trip(BranchIdToName { branch_id: 9 }, "__fd_branch_id_to_name/9");
    }

    #[test]
    fn test_dropped_branch_ident_key_format() {
        assert_round_trip(
            DroppedBranchIdent::new(7, "branch/a", 9),
            "__fd_dropped_branch/7/branch%2fa/9",
        );
    }
}
