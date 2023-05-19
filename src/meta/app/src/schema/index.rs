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
use common_meta_types::MetaId;

#[derive(serde::Serialize, serde::Deserialize, Clone, Debug, Eq, PartialEq, Default)]
pub struct IndexNameIdent {
    pub tenant: String,
    pub index_name: String,
}

impl IndexNameIdent {
    pub fn new(tenant: impl Into<String>, index_name: impl Into<String>) -> IndexNameIdent {
        IndexNameIdent {
            tenant: tenant.into(),
            index_name: index_name.into(),
        }
    }

    pub fn index_name(&self) -> String {
        self.index_name.clone()
    }
}

impl Display for IndexNameIdent {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        write!(f, "'{}'.'{}'", self.tenant, self.index_name)
    }
}

#[derive(serde::Serialize, serde::Deserialize, Clone, Debug, Default, Eq, PartialEq)]
pub struct IndexId {
    pub index_id: u64,
}

impl Display for IndexId {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.index_id)
    }
}

#[derive(
    serde::Serialize,
    serde::Deserialize,
    Clone,
    Debug,
    Default,
    Eq,
    PartialEq,
    num_derive::FromPrimitive,
)]
pub enum IndexType {
    #[default]
    AGGREGATING = 1,
    JOIN = 2,
}

impl Display for IndexType {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        match self {
            IndexType::AGGREGATING => write!(f, "AGGREGATING"),
            IndexType::JOIN => write!(f, "JOIN"),
        }
    }
}

#[derive(serde::Serialize, serde::Deserialize, Clone, Debug, Default, Eq, PartialEq)]
pub struct IndexIdListKey {
    pub tenant: String,
}

impl Display for IndexIdListKey {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "'{}'", self.tenant)
    }
}

/// Save index id list history.
#[derive(serde::Serialize, serde::Deserialize, Clone, Debug, Eq, Default, PartialEq)]
pub struct IndexIdList {
    pub id_list: Vec<u64>,
}

impl IndexIdList {
    pub fn new() -> IndexIdList {
        IndexIdList::default()
    }

    pub fn len(&self) -> usize {
        self.id_list.len()
    }

    pub fn id_list(&self) -> &Vec<u64> {
        &self.id_list
    }

    pub fn append(&mut self, index_id: u64) {
        self.id_list.push(index_id);
    }

    pub fn is_empty(&self) -> bool {
        self.id_list.is_empty()
    }

    pub fn pop(&mut self) -> Option<u64> {
        self.id_list.pop()
    }

    pub fn last(&mut self) -> Option<&u64> {
        self.id_list.last()
    }
}

impl Display for IndexIdList {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        write!(f, "Index id list: {:?}", self.id_list)
    }
}

#[derive(serde::Serialize, serde::Deserialize, Clone, Debug, Eq, PartialEq)]
pub struct IndexMeta {
    pub ident: IndexNameIdent,

    pub table_id: MetaId,
    pub table_desc: String,

    pub index_type: IndexType,
    pub created_on: DateTime<Utc>,
    // if used in CreateIndexReq, this field MUST set to None.
    pub drop_on: Option<DateTime<Utc>>,
    pub query: String,
}

impl Default for IndexMeta {
    fn default() -> Self {
        IndexMeta {
            ident: IndexNameIdent::default(),
            table_id: 0,
            table_desc: "".to_string(),
            index_type: IndexType::default(),
            created_on: Utc::now(),
            drop_on: None,
            query: "".to_string(),
        }
    }
}

#[derive(serde::Serialize, serde::Deserialize, Clone, Debug, PartialEq, Eq)]
pub struct CreateIndexReq {
    pub if_not_exists: bool,
    pub meta: IndexMeta,
}

impl Display for CreateIndexReq {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "create_index(if_not_exists={}):{}={:?}",
            self.if_not_exists, self.meta.ident.tenant, self.meta
        )
    }
}

#[derive(serde::Serialize, serde::Deserialize, Clone, Debug, Eq, PartialEq)]
pub struct CreateIndexReply {
    pub index_id: u64,
}

#[derive(serde::Serialize, serde::Deserialize, Clone, Debug, PartialEq, Eq)]
pub struct DropIndexReq {
    pub if_exists: bool,
    pub name_ident: IndexNameIdent,
}

impl Display for DropIndexReq {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "drop_index(if_exists={}):{}/{}",
            self.if_exists, self.name_ident.tenant, self.name_ident.index_name
        )
    }
}

#[derive(serde::Serialize, serde::Deserialize, Clone, Debug, PartialEq, Eq)]
pub struct DropIndexReply {}

#[derive(serde::Serialize, serde::Deserialize, Clone, Debug, PartialEq, Eq)]
pub struct ListIndexesReq {
    pub tenant: String,
    pub table_id: Option<MetaId>,
}

impl ListIndexesReq {
    pub fn new(tenant: impl Into<String>, table_id: Option<MetaId>) -> ListIndexesReq {
        ListIndexesReq {
            tenant: tenant.into(),
            table_id,
        }
    }
}

mod kvapi_key_impl {
    use common_meta_kvapi::kvapi;

    use crate::schema::IndexId;
    use crate::schema::IndexIdListKey;
    use crate::schema::IndexNameIdent;
    use crate::schema::PREFIX_INDEX;
    use crate::schema::PREFIX_INDEX_BY_ID;
    use crate::schema::PREFIX_INDEX_ID_LIST;

    /// <prefix>/<tenant>/<index_name> -> <index_id>
    impl kvapi::Key for IndexNameIdent {
        const PREFIX: &'static str = PREFIX_INDEX;

        fn to_string_key(&self) -> String {
            kvapi::KeyBuilder::new_prefixed(Self::PREFIX)
                .push_str(&self.tenant)
                .push_str(&self.index_name)
                .done()
        }

        fn from_str_key(s: &str) -> Result<Self, kvapi::KeyError> {
            let mut p = kvapi::KeyParser::new_prefixed(s, Self::PREFIX)?;

            let tenant = p.next_str()?;
            let index_name = p.next_str()?;
            p.done()?;

            Ok(IndexNameIdent { tenant, index_name })
        }
    }

    /// "<prefix>/<index_id>"
    impl kvapi::Key for IndexId {
        const PREFIX: &'static str = PREFIX_INDEX_BY_ID;

        fn to_string_key(&self) -> String {
            kvapi::KeyBuilder::new_prefixed(Self::PREFIX)
                .push_u64(self.index_id)
                .done()
        }

        fn from_str_key(s: &str) -> Result<Self, kvapi::KeyError> {
            let mut p = kvapi::KeyParser::new_prefixed(s, Self::PREFIX)?;

            let index_id = p.next_u64()?;
            p.done()?;

            Ok(IndexId { index_id })
        }
    }

    /// "<prefix>/<tenant> -> index_id_list"
    impl kvapi::Key for IndexIdListKey {
        const PREFIX: &'static str = PREFIX_INDEX_ID_LIST;

        fn to_string_key(&self) -> String {
            kvapi::KeyBuilder::new_prefixed(Self::PREFIX)
                .push_str(&self.tenant)
                .done()
        }

        fn from_str_key(s: &str) -> Result<Self, kvapi::KeyError> {
            let mut p = kvapi::KeyParser::new_prefixed(s, Self::PREFIX)?;

            let tenant = p.next_str()?;
            p.done()?;

            Ok(IndexIdListKey { tenant })
        }
    }
}
