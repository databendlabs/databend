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
use databend_common_meta_types::MetaId;

use super::CreateOption;

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

#[derive(serde::Serialize, serde::Deserialize, Clone, Debug, Eq, PartialEq, Default)]
pub struct IndexIdToName {
    pub index_id: u64,
}

impl Display for IndexIdToName {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.index_id)
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

#[derive(serde::Serialize, serde::Deserialize, Clone, Debug, Eq, PartialEq)]
pub struct IndexMeta {
    pub table_id: MetaId,

    pub index_type: IndexType,
    pub created_on: DateTime<Utc>,
    // if used in CreateIndexReq, `dropped_on` and `updated_on` MUST set to None.
    pub dropped_on: Option<DateTime<Utc>>,
    pub updated_on: Option<DateTime<Utc>>,
    pub original_query: String,
    pub query: String,
    // if true, index will create after data written to databend,
    // no need execute refresh index manually.
    pub sync_creation: bool,
}

impl Default for IndexMeta {
    fn default() -> Self {
        IndexMeta {
            table_id: 0,
            index_type: IndexType::default(),
            created_on: Utc::now(),
            dropped_on: None,
            updated_on: None,
            original_query: "".to_string(),
            query: "".to_string(),
            sync_creation: false,
        }
    }
}

#[derive(serde::Serialize, serde::Deserialize, Clone, Debug, PartialEq, Eq)]
pub struct CreateIndexReq {
    pub create_option: CreateOption,
    pub name_ident: IndexNameIdent,
    pub meta: IndexMeta,
}

impl Display for CreateIndexReq {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        if let CreateOption::CreateIfNotExists(if_not_exists) = self.create_option {
            write!(
                f,
                "create_index(if_not_exists={}):{}={:?}",
                if_not_exists, self.name_ident.tenant, self.meta
            )
        } else {
            write!(
                f,
                "create_or_replace_index:{}={:?}",
                self.name_ident.tenant, self.meta
            )
        }
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
pub struct GetIndexReq {
    pub name_ident: IndexNameIdent,
}

impl Display for GetIndexReq {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "get_index:{}/{}",
            self.name_ident.tenant, self.name_ident.index_name
        )
    }
}

#[derive(serde::Serialize, serde::Deserialize, Clone, Debug, PartialEq, Eq)]
pub struct GetIndexReply {
    pub index_id: u64,
    pub index_meta: IndexMeta,
}

#[derive(serde::Serialize, serde::Deserialize, Clone, Debug, PartialEq, Eq)]
pub struct UpdateIndexReq {
    pub index_id: u64,
    pub index_name: String,
    pub index_meta: IndexMeta,
}

#[derive(serde::Serialize, serde::Deserialize, Clone, Debug, PartialEq, Eq)]
pub struct UpdateIndexReply {}

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

#[derive(serde::Serialize, serde::Deserialize, Clone, Debug, PartialEq, Eq)]
pub struct ListIndexesByIdReq {
    pub tenant: String,
    pub table_id: MetaId,
}

impl ListIndexesByIdReq {
    pub fn new(tenant: impl Into<String>, table_id: MetaId) -> Self {
        Self {
            tenant: tenant.into(),
            table_id,
        }
    }
}

mod kvapi_key_impl {
    use databend_common_meta_kvapi::kvapi;

    use crate::schema::IndexId;
    use crate::schema::IndexIdToName;
    use crate::schema::IndexMeta;
    use crate::schema::IndexNameIdent;

    /// <prefix>/<tenant>/<index_name> -> <index_id>
    impl kvapi::Key for IndexNameIdent {
        const PREFIX: &'static str = "__fd_index";

        type ValueType = IndexId;

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
        const PREFIX: &'static str = "__fd_index_by_id";

        type ValueType = IndexMeta;

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

    /// "<prefix>/<index_id> -> IndexNameIdent"
    impl kvapi::Key for IndexIdToName {
        const PREFIX: &'static str = "__fd_index_id_to_name";

        type ValueType = IndexNameIdent;

        fn to_string_key(&self) -> String {
            kvapi::KeyBuilder::new_prefixed(Self::PREFIX)
                .push_u64(self.index_id)
                .done()
        }

        fn from_str_key(s: &str) -> Result<Self, kvapi::KeyError> {
            let mut p = kvapi::KeyParser::new_prefixed(s, Self::PREFIX)?;

            let index_id = p.next_u64()?;
            p.done()?;

            Ok(IndexIdToName { index_id })
        }
    }

    impl kvapi::Value for IndexId {}

    impl kvapi::Value for IndexMeta {}

    impl kvapi::Value for IndexNameIdent {}
}
