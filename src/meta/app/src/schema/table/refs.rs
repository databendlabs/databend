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

use super::TableLvtCheck;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TableTag {
    /// After this timestamp, the tag becomes inactive.
    pub expire_at: Option<DateTime<Utc>>,
    /// The location of the snapshot that the tag points to.
    pub snapshot_loc: String,
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

mod kvapi_key_impl {
    use databend_meta_kvapi::kvapi;
    use databend_meta_kvapi::kvapi::KeyBuilder;
    use databend_meta_kvapi::kvapi::KeyError;
    use databend_meta_kvapi::kvapi::KeyParser;

    use crate::schema::TableId;
    use crate::schema::table::TableIdTagName;
    use crate::schema::table::TableTag;

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

    impl kvapi::Value for TableTag {
        type KeyType = TableIdTagName;

        fn dependency_keys(&self, _key: &Self::KeyType) -> impl IntoIterator<Item = String> {
            []
        }
    }
}
