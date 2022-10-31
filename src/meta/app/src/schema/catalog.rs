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

use std::collections::BTreeMap;
use std::fmt::Display;
use std::fmt::Formatter;
use std::ops::Deref;

use common_datavalues::chrono::DateTime;
use common_datavalues::chrono::Utc;
use serde::Deserialize;
use serde::Serialize;

#[derive(Copy, Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum CatalogType {
    Default = 0,
    Hive = 1,
}

impl Default for CatalogType {
    fn default() -> Self {
        CatalogType::Default
    }
}

impl Display for CatalogType {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            CatalogType::Default => write!(f, "DEFAULT"),
            CatalogType::Hive => write!(f, "HIVE"),
        }
    }
}

#[derive(Serialize, Deserialize, Clone, Debug, Default, Eq, PartialEq)]
pub struct CatalogNameIdent {
    pub tenant: String,
    pub ctl_name: String,
}

impl Display for CatalogNameIdent {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "'{}'/'{}'", self.tenant, self.ctl_name)
    }
}

#[derive(Serialize, Deserialize, Clone, Debug, Default, Eq, PartialEq)]
pub struct CatalogInfo {
    pub name_ident: CatalogNameIdent,
    pub meta: CatalogMeta,
}

impl CatalogInfo {
    pub fn catalog_type(&self) -> CatalogType {
        self.meta.catalog_type
    }
}

#[derive(Serialize, Deserialize, Clone, Debug, Default, Eq, PartialEq)]
pub struct CatalogMeta {
    pub catalog_type: CatalogType,
    pub options: BTreeMap<String, String>,
    pub created_on: DateTime<Utc>,
    pub dropped_on: Option<DateTime<Utc>>,
}

impl Display for CatalogMeta {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "Type: {}={:?}, CreatedOn: {:?}",
            self.catalog_type, self.options, self.created_on
        )
    }
}

#[derive(Serialize, Deserialize, Clone, Debug, PartialEq, Eq)]
pub struct CreateCatalogReq {
    pub if_not_exists: bool,
    pub name_ident: CatalogNameIdent,
    pub meta: CatalogMeta,
}

impl Display for CreateCatalogReq {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "create_catalog(if_not_exists={}):{}/{}={:?}",
            self.if_not_exists, self.name_ident.tenant, self.name_ident.ctl_name, self.meta
        )
    }
}

#[derive(Serialize, Deserialize, Clone, Debug, PartialEq, Eq)]
pub struct CreateCatalogReply {
    pub ctl_id: u64,
}

#[derive(Serialize, Deserialize, Clone, Debug, PartialEq, Eq)]
pub struct DropCatalogReq {
    pub if_exists: bool,
    pub name_ident: CatalogNameIdent,
}

impl Display for DropCatalogReq {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "drop_ctl(if_exists={}):{}/{}",
            self.if_exists, self.name_ident.tenant, self.name_ident.ctl_name
        )
    }
}

#[derive(Serialize, Deserialize, Clone, Debug, PartialEq, Eq)]
pub struct DropCatalogReply {}

#[derive(Serialize, Deserialize, Clone, Debug, PartialEq, Eq)]
pub struct GetCatalogReq {
    pub inner: CatalogNameIdent,
}

impl Deref for GetCatalogReq {
    type Target = CatalogNameIdent;

    fn deref(&self) -> &Self::Target {
        &self.inner
    }
}

impl GetCatalogReq {
    pub fn new(tenant: impl Into<String>, ctl_name: impl Into<String>) -> GetCatalogReq {
        Self {
            inner: CatalogNameIdent {
                tenant: tenant.into(),
                ctl_name: ctl_name.into(),
            },
        }
    }
}

#[derive(Serialize, Deserialize, Clone, Debug, PartialEq, Eq)]
pub struct ListCatalogReq {
    pub tenant: String,
}

/// same as DbIdList
///
/// Save catalog name id list history
#[derive(Serialize, Deserialize, Clone, Debug, Default, PartialEq, Eq)]
pub struct CatalogIdList {
    pub id_list: Vec<u64>,
}

impl CatalogIdList {
    pub fn new() -> CatalogIdList {
        CatalogIdList::default()
    }

    pub fn len(&self) -> usize {
        self.id_list.len()
    }

    pub fn id_list(&self) -> &Vec<u64> {
        &self.id_list
    }

    pub fn append(&mut self, table_id: u64) {
        self.id_list.push(table_id);
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

#[derive(Serialize, Deserialize, Clone, Debug, Default, Eq, PartialEq)]
pub struct CatalogIdToName {
    pub ctl_id: u64,
}

impl Display for CatalogIdToName {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.ctl_id)
    }
}

#[derive(Serialize, Deserialize, Clone, Debug, Default, Eq, PartialEq)]
pub struct CatalogIdListKey {
    pub tenant: String,
    pub ctl_name: String,
}

impl Display for CatalogIdListKey {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "'{}'/'{}'", self.tenant, self.ctl_name)
    }
}

#[derive(Serialize, Deserialize, Clone, Debug, Default, Eq, PartialEq)]
pub struct CatalogId {
    pub ctl_id: u64,
}

impl Display for CatalogId {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.ctl_id)
    }
}
