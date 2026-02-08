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

use std::collections::HashMap;
use std::fmt;
use std::fmt::Display;
use std::fmt::Formatter;

use chrono::DateTime;
use chrono::Utc;
use databend_meta_types::MetaId;

use super::CreateOption;
use crate::KeyWithTenant;
use crate::schema::IndexNameIdent;
use crate::tenant::Tenant;
use crate::tenant::ToTenant;

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
    fn fmt(&self, f: &mut Formatter) -> fmt::Result {
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

#[derive(Clone, Debug, Eq, PartialEq, num_derive::FromPrimitive)]
pub enum MarkedDeletedIndexType {
    AGGREGATING = 1,
    INVERTED = 2,
    NGRAM = 3,
    VECTOR = 4,
    SPATIAL = 5,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct MarkedDeletedIndexMeta {
    pub dropped_on: DateTime<Utc>,
    pub index_type: MarkedDeletedIndexType,
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
    fn fmt(&self, f: &mut Formatter) -> std::fmt::Result {
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
    fn fmt(&self, f: &mut Formatter) -> std::fmt::Result {
        write!(
            f,
            "drop_index(if_exists={}):{}/{}",
            self.if_exists,
            self.name_ident.tenant_name(),
            self.name_ident.index_name()
        )
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct GetIndexReq {
    pub name_ident: IndexNameIdent,
}

impl Display for GetIndexReq {
    fn fmt(&self, f: &mut Formatter) -> std::fmt::Result {
        write!(
            f,
            "get_index:{}/{}",
            self.name_ident.tenant_name(),
            self.name_ident.index_name()
        )
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct GetIndexReply {
    pub index_id: u64,
    pub index_meta: IndexMeta,
}

/// Maps table_id to a vector of (index_id, marked_deleted_index_meta) pairs.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct GetMarkedDeletedIndexesReply {
    pub table_indexes: HashMap<u64, Vec<(u64, MarkedDeletedIndexMeta)>>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct UpdateIndexReq {
    pub tenant: Tenant,
    pub index_id: u64,
    pub index_meta: IndexMeta,
}

#[derive(Clone, Debug, PartialEq, Eq)]
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
