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
use crate::schema::IndexNameIdent;
use crate::tenant::Tenant;
use crate::tenant::ToTenant;
use crate::KeyWithTenant;

#[derive(Clone, Debug, Eq, PartialEq, Default)]
pub struct IndexIdToName {
    pub index_id: u64,
}

impl Display for IndexIdToName {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.index_id)
    }
}

#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct IndexId {
    pub index_id: u64,
}

impl IndexId {
    pub fn new(index_id: u64) -> IndexId {
        IndexId { index_id }
    }
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

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct CreateIndexReq {
    pub create_option: CreateOption,
    pub name_ident: IndexNameIdent,
    pub meta: IndexMeta,
}

impl Display for CreateIndexReq {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self.create_option {
            CreateOption::Create => {
                write!(
                    f,
                    "create_index:{}={:?}",
                    self.name_ident.tenant_name(),
                    self.meta
                )
            }
            CreateOption::CreateIfNotExists => write!(
                f,
                "create_index_if_not_exists:{}={:?}",
                self.name_ident.tenant_name(),
                self.meta
            ),
            CreateOption::CreateOrReplace => write!(
                f,
                "create_or_replace_index:{}={:?}",
                self.name_ident.tenant_name(),
                self.meta
            ),
        }
    }
}

#[derive(serde::Serialize, serde::Deserialize, Clone, Debug, Eq, PartialEq)]
pub struct CreateIndexReply {
    pub index_id: u64,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct DropIndexReq {
    pub if_exists: bool,
    pub name_ident: IndexNameIdent,
}

impl Display for DropIndexReq {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "drop_index(if_exists={}):{}/{}",
            self.if_exists,
            self.name_ident.tenant_name(),
            self.name_ident.index_name()
        )
    }
}

#[derive(serde::Serialize, serde::Deserialize, Clone, Debug, PartialEq, Eq)]
pub struct DropIndexReply {}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct GetIndexReq {
    pub name_ident: IndexNameIdent,
}

impl Display for GetIndexReq {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "get_index:{}/{}",
            self.name_ident.tenant_name(),
            self.name_ident.index_name()
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

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ListIndexesReq {
    pub tenant: Tenant,
    pub table_id: Option<MetaId>,
}

impl ListIndexesReq {
    pub fn new(tenant: impl ToTenant, table_id: Option<MetaId>) -> ListIndexesReq {
        ListIndexesReq {
            tenant: tenant.to_tenant(),
            table_id,
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ListIndexesByIdReq {
    pub tenant: Tenant,
    pub table_id: MetaId,
}

impl ListIndexesByIdReq {
    pub fn new(tenant: impl ToTenant, table_id: MetaId) -> Self {
        Self {
            tenant: tenant.to_tenant(),
            table_id,
        }
    }
}

mod kvapi_key_impl {
    use databend_common_meta_kvapi::kvapi;

    use crate::schema::IndexId;
    use crate::schema::IndexIdToName;
    use crate::schema::IndexMeta;
    use crate::schema::IndexNameIdentRaw;

    impl kvapi::KeyCodec for IndexId {
        fn encode_key(&self, b: kvapi::KeyBuilder) -> kvapi::KeyBuilder {
            b.push_u64(self.index_id)
        }

        fn decode_key(parser: &mut kvapi::KeyParser) -> Result<Self, kvapi::KeyError> {
            let index_id = parser.next_u64()?;

            Ok(Self { index_id })
        }
    }

    /// "<prefix>/<index_id>"
    impl kvapi::Key for IndexId {
        const PREFIX: &'static str = "__fd_index_by_id";

        type ValueType = IndexMeta;

        fn parent(&self) -> Option<String> {
            None
        }
    }

    impl kvapi::KeyCodec for IndexIdToName {
        fn encode_key(&self, b: kvapi::KeyBuilder) -> kvapi::KeyBuilder {
            b.push_u64(self.index_id)
        }

        fn decode_key(parser: &mut kvapi::KeyParser) -> Result<Self, kvapi::KeyError> {
            let index_id = parser.next_u64()?;

            Ok(Self { index_id })
        }
    }

    /// "<prefix>/<index_id> -> IndexNameIdentRaw"
    impl kvapi::Key for IndexIdToName {
        const PREFIX: &'static str = "__fd_index_id_to_name";

        type ValueType = IndexNameIdentRaw;

        fn parent(&self) -> Option<String> {
            Some(IndexId::new(self.index_id).to_string_key())
        }
    }

    impl kvapi::Value for IndexMeta {
        fn dependency_keys(&self) -> impl IntoIterator<Item = String> {
            []
        }
    }

    impl kvapi::Value for IndexNameIdentRaw {
        fn dependency_keys(&self) -> impl IntoIterator<Item = String> {
            []
        }
    }
}
