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

use std::collections::BTreeMap;
use std::collections::BTreeSet;
use std::fmt;
use std::fmt::Display;
use std::fmt::Formatter;
use std::ops::Deref;

use common_datavalues::chrono::DateTime;
use common_datavalues::chrono::Utc;

use crate::share::ShareNameIdent;

#[derive(serde::Serialize, serde::Deserialize, Clone, Debug, Default, Eq, PartialEq)]
pub struct DatabaseNameIdent {
    pub tenant: String,
    pub db_name: String,
}

impl Display for DatabaseNameIdent {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "'{}'/'{}'", self.tenant, self.db_name)
    }
}

#[derive(serde::Serialize, serde::Deserialize, Clone, Debug, Default, Eq, PartialEq)]
pub struct DatabaseInfo {
    pub ident: DatabaseIdent,
    pub name_ident: DatabaseNameIdent,
    pub meta: DatabaseMeta,
}

#[derive(serde::Serialize, serde::Deserialize, Clone, Debug, Default, Eq, PartialEq)]
pub struct DatabaseIdent {
    pub db_id: u64,
    pub seq: u64,
}

#[derive(serde::Serialize, serde::Deserialize, Clone, Debug, Default, Eq, PartialEq)]
pub struct DatabaseId {
    pub db_id: u64,
}

impl Display for DatabaseId {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.db_id)
    }
}

#[derive(serde::Serialize, serde::Deserialize, Clone, Debug, Default, Eq, PartialEq)]
pub struct DatabaseIdToName {
    pub db_id: u64,
}

impl Display for DatabaseIdToName {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.db_id)
    }
}

#[derive(serde::Serialize, serde::Deserialize, Clone, Debug, Default, Eq, PartialEq)]
pub struct DbIdListKey {
    pub tenant: String,
    pub db_name: String,
}

impl Display for DbIdListKey {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "'{}'/'{}'", self.tenant, self.db_name)
    }
}

#[derive(serde::Serialize, serde::Deserialize, Clone, Debug, Eq, PartialEq)]
pub struct DatabaseMeta {
    pub engine: String,
    pub engine_options: BTreeMap<String, String>,
    pub options: BTreeMap<String, String>,
    pub created_on: DateTime<Utc>,
    pub updated_on: DateTime<Utc>,
    pub comment: String,

    // if used in CreateDatabaseReq, this field MUST set to None.
    pub drop_on: Option<DateTime<Utc>>,
    // shared by share_id
    pub shared_by: BTreeSet<u64>,
    pub from_share: Option<ShareNameIdent>,
}

impl Default for DatabaseMeta {
    fn default() -> Self {
        DatabaseMeta {
            engine: "".to_string(),
            engine_options: BTreeMap::new(),
            options: BTreeMap::new(),
            created_on: Utc::now(),
            updated_on: Utc::now(),
            comment: "".to_string(),
            drop_on: None,
            shared_by: BTreeSet::new(),
            from_share: None,
        }
    }
}

impl Display for DatabaseMeta {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "Engine: {}={:?}, Options: {:?}, CreatedOn: {:?}",
            self.engine, self.engine_options, self.options, self.created_on
        )
    }
}

impl DatabaseInfo {
    pub fn engine(&self) -> &str {
        &self.meta.engine
    }
}

/// Save db name id list history.
#[derive(serde::Serialize, serde::Deserialize, Clone, Debug, Eq, Default, PartialEq)]
pub struct DbIdList {
    pub id_list: Vec<u64>,
}

impl DbIdList {
    pub fn new() -> DbIdList {
        DbIdList::default()
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

impl Display for DbIdList {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        write!(f, "DB id list: {:?}", self.id_list)
    }
}

#[derive(serde::Serialize, serde::Deserialize, Clone, Debug, PartialEq, Eq)]
pub struct CreateDatabaseReq {
    pub if_not_exists: bool,
    pub name_ident: DatabaseNameIdent,
    pub meta: DatabaseMeta,
}

impl Display for CreateDatabaseReq {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "create_db(if_not_exists={}):{}/{}={:?}",
            self.if_not_exists, self.name_ident.tenant, self.name_ident.db_name, self.meta
        )
    }
}

#[derive(serde::Serialize, serde::Deserialize, Clone, Debug, Eq, PartialEq)]
pub struct CreateDatabaseReply {
    pub db_id: u64,
}

#[derive(serde::Serialize, serde::Deserialize, Clone, Debug, PartialEq, Eq)]
pub struct RenameDatabaseReq {
    pub if_exists: bool,
    pub name_ident: DatabaseNameIdent,
    pub new_db_name: String,
}

impl Display for RenameDatabaseReq {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "rename_database:{}/{}=>{}",
            self.name_ident.tenant, self.name_ident.db_name, self.new_db_name
        )
    }
}

#[derive(serde::Serialize, serde::Deserialize, Clone, Debug, PartialEq, Eq)]
pub struct RenameDatabaseReply {}

#[derive(serde::Serialize, serde::Deserialize, Clone, Debug, PartialEq, Eq)]
pub struct DropDatabaseReq {
    pub if_exists: bool,
    pub name_ident: DatabaseNameIdent,
}

impl Display for DropDatabaseReq {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "drop_db(if_exists={}):{}/{}",
            self.if_exists, self.name_ident.tenant, self.name_ident.db_name
        )
    }
}

#[derive(serde::Serialize, serde::Deserialize, Clone, Debug, PartialEq, Eq)]
pub struct DropDatabaseReply {}

#[derive(serde::Serialize, serde::Deserialize, Clone, Debug, PartialEq, Eq)]
pub struct UndropDatabaseReq {
    pub name_ident: DatabaseNameIdent,
}

impl Display for UndropDatabaseReq {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "undrop_db:{}/{}",
            self.name_ident.tenant, self.name_ident.db_name
        )
    }
}

impl UndropDatabaseReq {
    pub fn tenant(&self) -> &str {
        &self.name_ident.tenant
    }
    pub fn db_name(&self) -> &str {
        &self.name_ident.db_name
    }
}

#[derive(serde::Serialize, serde::Deserialize, Clone, Debug, PartialEq, Eq)]
pub struct UndropDatabaseReply {}

#[derive(serde::Serialize, serde::Deserialize, Clone, Debug, PartialEq, Eq)]
pub struct GetDatabaseReq {
    pub inner: DatabaseNameIdent,
}

impl Deref for GetDatabaseReq {
    type Target = DatabaseNameIdent;

    fn deref(&self) -> &Self::Target {
        &self.inner
    }
}

impl GetDatabaseReq {
    pub fn new(tenant: impl Into<String>, db_name: impl Into<String>) -> GetDatabaseReq {
        GetDatabaseReq {
            inner: DatabaseNameIdent {
                tenant: tenant.into(),
                db_name: db_name.into(),
            },
        }
    }
}

#[derive(serde::Serialize, serde::Deserialize, Clone, Debug, PartialEq, Eq)]
pub struct ListDatabaseReq {
    pub tenant: String,
}
