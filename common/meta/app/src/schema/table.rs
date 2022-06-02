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
use std::collections::HashMap;
use std::fmt;
use std::fmt::Display;
use std::fmt::Formatter;
use std::ops::Deref;
use std::sync::Arc;

use common_datavalues::chrono::DateTime;
use common_datavalues::chrono::Utc;
use common_datavalues::prelude::*;
use common_meta_types::MatchSeq;
use maplit::hashmap;

use crate::schema::database::DatabaseNameIdent;

/// Globally unique identifier of a version of TableMeta.
#[derive(serde::Serialize, serde::Deserialize, Clone, Debug, Eq, PartialEq, Default)]
pub struct TableIdent {
    /// Globally unique id to identify a table.
    pub table_id: u64,

    /// seq AKA version of this table snapshot.
    ///
    /// Any change to a table causes the seq to increment, e.g. insert or delete rows, update schema etc.
    /// But renaming a table should not affect the seq, since the table itself does not change.
    /// The tuple (table_id, seq) identifies a unique and consistent table snapshot.
    ///
    /// A seq is not guaranteed to be consecutive.
    pub seq: u64,
}

impl TableIdent {
    pub fn new(table_id: u64, seq: u64) -> Self {
        TableIdent { table_id, seq }
    }
}

impl Display for TableIdent {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        write!(f, "table_id:{}, ver:{}", self.table_id, self.seq)
    }
}

#[derive(serde::Serialize, serde::Deserialize, Clone, Debug, Eq, PartialEq, Default)]
pub struct TableNameIdent {
    pub tenant: String,
    pub db_name: String,
    pub table_name: String,
}

impl TableNameIdent {
    pub fn new(
        tenant: impl Into<String>,
        db_name: impl Into<String>,
        table_name: impl Into<String>,
    ) -> TableNameIdent {
        TableNameIdent {
            tenant: tenant.into(),
            db_name: db_name.into(),
            table_name: table_name.into(),
        }
    }

    pub fn table_name(&self) -> String {
        self.table_name.clone()
    }

    pub fn db_name_ident(&self) -> DatabaseNameIdent {
        DatabaseNameIdent {
            tenant: self.tenant.clone(),
            db_name: self.db_name.clone(),
        }
    }
}

impl Display for TableNameIdent {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "'{}'.'{}'.'{}'",
            self.tenant, self.db_name, self.table_name
        )
    }
}

#[derive(serde::Serialize, serde::Deserialize, Clone, Debug, Eq, PartialEq, Default)]
pub struct DBIdTableName {
    pub db_id: u64,
    pub table_name: String,
}

impl Display for DBIdTableName {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        write!(f, "{}.'{}'", self.db_id, self.table_name)
    }
}

#[derive(serde::Serialize, serde::Deserialize, Clone, Debug, Eq, PartialEq, Default)]
pub struct TableId {
    pub table_id: u64,
}

impl Display for TableId {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.table_id)
    }
}

#[derive(serde::Serialize, serde::Deserialize, Clone, Debug, Eq, PartialEq, Default)]
pub struct TableIdListKey {
    pub db_id: u64,
    pub table_name: String,
}

impl Display for TableIdListKey {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        write!(f, "{}.'{}'", self.db_id, self.table_name)
    }
}

#[derive(serde::Serialize, serde::Deserialize, Clone, Debug, Eq, PartialEq, Default)]
pub struct TableInfo {
    pub ident: TableIdent,

    /// For a table it is `db_name.table_name`.
    /// For a table function, it is `table_name(args)`
    pub desc: String,

    /// `name` is meant to be used with table-function.
    /// Table-function is identified by `name`.
    /// A table in the contrast, can only be identified by table-id.
    pub name: String,

    /// The essential information about a table definition.
    ///
    /// It is about what a table actually is.
    /// `name`, `id` or `version` is not included in the table structure definition.
    pub meta: TableMeta,
}

#[derive(serde::Serialize, serde::Deserialize, Clone, Debug, Eq, PartialEq, Default)]
pub struct TableStatistics {
    /// Number of rows
    pub number_of_rows: u64,
    // Size of data in bytes
    pub data_bytes: u64,
    /// Size of data compressed in bytes
    pub compressed_data_bytes: u64,
    /// Size of index data in bytes
    pub index_data_bytes: u64,
}

/// The essential state that defines what a table is.
///
/// It is what a meta store just needs to save.
#[derive(serde::Serialize, serde::Deserialize, Clone, Debug, Eq, PartialEq)]
pub struct TableMeta {
    pub schema: Arc<DataSchema>,
    pub engine: String,
    pub engine_options: BTreeMap<String, String>,
    pub options: BTreeMap<String, String>,
    pub cluster_keys: Option<String>,
    pub created_on: DateTime<Utc>,
    pub updated_on: DateTime<Utc>,
    pub comment: String,

    // if used in CreateTableReq, this field MUST set to None.
    pub drop_on: Option<DateTime<Utc>>,
    pub statistics: TableStatistics,
}

impl TableInfo {
    /// Create a TableInfo with only db, table, schema
    pub fn simple(db: &str, table: &str, schema: Arc<DataSchema>) -> TableInfo {
        TableInfo {
            desc: format!("'{}'.'{}'", db, table),
            name: table.to_string(),
            meta: TableMeta {
                schema,
                ..Default::default()
            },
            ..Default::default()
        }
    }

    pub fn new(db_name: &str, table_name: &str, ident: TableIdent, meta: TableMeta) -> TableInfo {
        TableInfo {
            ident,
            desc: format!("'{}'.'{}'", db_name, table_name),
            name: table_name.to_string(),
            meta,
        }
    }

    pub fn schema(&self) -> Arc<DataSchema> {
        self.meta.schema.clone()
    }

    pub fn options(&self) -> &BTreeMap<String, String> {
        &self.meta.options
    }

    pub fn engine(&self) -> &str {
        &self.meta.engine
    }

    pub fn engine_options(&self) -> &BTreeMap<String, String> {
        &self.meta.engine_options
    }

    #[must_use]
    pub fn set_schema(mut self, schema: Arc<DataSchema>) -> TableInfo {
        self.meta.schema = schema;
        self
    }
}

impl Default for TableMeta {
    fn default() -> Self {
        TableMeta {
            schema: Arc::new(DataSchema::empty()),
            engine: "".to_string(),
            engine_options: BTreeMap::new(),
            options: BTreeMap::new(),
            cluster_keys: None,
            created_on: Default::default(),
            updated_on: Default::default(),
            comment: "".to_string(),
            drop_on: None,
            statistics: Default::default(),
        }
    }
}

impl Display for TableMeta {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "Engine: {}={:?}, Schema: {}, Options: {:?} CreatedOn: {:?} DropOn: {:?}",
            self.engine,
            self.engine_options,
            self.schema,
            self.options,
            self.created_on,
            self.drop_on,
        )
    }
}

impl Display for TableInfo {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "DB.Table: {}, Table: {}-{}, Engine: {}",
            self.desc, self.name, self.ident, self.meta.engine
        )
    }
}

/// Save table name id list history.
#[derive(serde::Serialize, serde::Deserialize, Clone, Debug, Default, Eq, PartialEq)]
pub struct TableIdList {
    pub id_list: Vec<u64>,
}

impl TableIdList {
    pub fn new() -> TableIdList {
        TableIdList::default()
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

impl Display for TableIdList {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        write!(f, "DB.Table id list: {:?}", self.id_list)
    }
}

#[derive(serde::Serialize, serde::Deserialize, Clone, Debug, PartialEq)]
pub struct CreateTableReq {
    pub if_not_exists: bool,
    pub name_ident: TableNameIdent,
    pub table_meta: TableMeta,
}

impl CreateTableReq {
    pub fn tenant(&self) -> &str {
        &self.name_ident.tenant
    }
    pub fn db_name(&self) -> &str {
        &self.name_ident.db_name
    }
    pub fn table_name(&self) -> &str {
        &self.name_ident.table_name
    }
}

impl Display for CreateTableReq {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "create_table(if_not_exists={}):{}/{}-{}={}",
            self.if_not_exists,
            self.tenant(),
            self.db_name(),
            self.table_name(),
            self.table_meta
        )
    }
}

#[derive(serde::Serialize, serde::Deserialize, Clone, Debug, Eq, PartialEq)]
pub struct CreateTableReply {
    pub table_id: u64,
}

#[derive(serde::Serialize, serde::Deserialize, Clone, Debug, PartialEq)]
pub struct DropTableReq {
    pub if_exists: bool,
    pub name_ident: TableNameIdent,
}

impl DropTableReq {
    pub fn tenant(&self) -> &str {
        &self.name_ident.tenant
    }
    pub fn db_name(&self) -> &str {
        &self.name_ident.db_name
    }
    pub fn table_name(&self) -> &str {
        &self.name_ident.table_name
    }
}

impl Display for DropTableReq {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "drop_table(if_exists={}):{}/{}-{}",
            self.if_exists,
            self.tenant(),
            self.db_name(),
            self.table_name()
        )
    }
}

#[derive(serde::Serialize, serde::Deserialize, Clone, Debug, PartialEq)]
pub struct DropTableReply {}

#[derive(serde::Serialize, serde::Deserialize, Clone, Debug, PartialEq)]
pub struct UndropTableReq {
    pub name_ident: TableNameIdent,
}

impl UndropTableReq {
    pub fn tenant(&self) -> &str {
        &self.name_ident.tenant
    }
    pub fn db_name(&self) -> &str {
        &self.name_ident.db_name
    }
    pub fn table_name(&self) -> &str {
        &self.name_ident.table_name
    }
}

impl Display for UndropTableReq {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "undrop_table:{}/{}-{}",
            self.tenant(),
            self.db_name(),
            self.table_name()
        )
    }
}

#[derive(serde::Serialize, serde::Deserialize, Clone, Debug, PartialEq)]
pub struct UndropTableReply {}

#[derive(serde::Serialize, serde::Deserialize, Clone, Debug, PartialEq)]
pub struct RenameTableReq {
    pub if_exists: bool,
    pub name_ident: TableNameIdent,
    pub new_db_name: String,
    pub new_table_name: String,
}

impl RenameTableReq {
    pub fn tenant(&self) -> &str {
        &self.name_ident.tenant
    }
    pub fn db_name(&self) -> &str {
        &self.name_ident.db_name
    }
    pub fn table_name(&self) -> &str {
        &self.name_ident.table_name
    }
}

impl Display for RenameTableReq {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "rename_table:{}/{}-{}=>{}-{}",
            self.tenant(),
            self.db_name(),
            self.table_name(),
            self.new_db_name,
            self.new_table_name
        )
    }
}

#[derive(serde::Serialize, serde::Deserialize, Clone, Debug, PartialEq)]
pub struct RenameTableReply {
    pub table_id: u64,
}

#[derive(serde::Serialize, serde::Deserialize, Clone, Debug, PartialEq)]
pub struct UpsertTableOptionReq {
    pub table_id: u64,
    pub seq: MatchSeq,

    /// Add or remove options
    ///
    /// Some(String): add or update an option.
    /// None: delete an option.
    pub options: HashMap<String, Option<String>>,
}

#[derive(serde::Serialize, serde::Deserialize, Clone, Debug, PartialEq)]
pub struct UpdateTableMetaReq {
    pub table_id: u64,
    pub seq: MatchSeq,
    pub new_table_meta: TableMeta,
}

impl UpsertTableOptionReq {
    pub fn new(
        table_ident: &TableIdent,
        key: impl Into<String>,
        value: impl Into<String>,
    ) -> UpsertTableOptionReq {
        UpsertTableOptionReq {
            table_id: table_ident.table_id,
            seq: MatchSeq::Exact(table_ident.seq),
            options: hashmap! {key.into() => Some(value.into())},
        }
    }
}

impl Display for UpsertTableOptionReq {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "upsert-table-options: table-id:{}({:?}) = {:?}",
            self.table_id, self.seq, self.options
        )
    }
}

#[derive(serde::Serialize, serde::Deserialize, Clone, Debug, PartialEq)]
pub struct UpsertTableOptionReply {}

#[derive(serde::Serialize, serde::Deserialize, Clone, Debug, PartialEq)]
pub struct UpdateTableMetaReply {}

#[derive(serde::Serialize, serde::Deserialize, Clone, Debug, PartialEq)]
pub struct GetTableReq {
    pub inner: TableNameIdent,
}

impl Deref for GetTableReq {
    type Target = TableNameIdent;

    fn deref(&self) -> &Self::Target {
        &self.inner
    }
}

impl From<(&str, &str, &str)> for GetTableReq {
    fn from(db_table: (&str, &str, &str)) -> Self {
        Self::new(db_table.0, db_table.1, db_table.2)
    }
}

impl GetTableReq {
    pub fn new(
        tenant: impl Into<String>,
        db_name: impl Into<String>,
        table_name: impl Into<String>,
    ) -> GetTableReq {
        GetTableReq {
            inner: TableNameIdent::new(tenant, db_name, table_name),
        }
    }
}

#[derive(serde::Serialize, serde::Deserialize, Clone, Debug, PartialEq)]
pub struct ListTableReq {
    pub inner: DatabaseNameIdent,
}

impl Deref for ListTableReq {
    type Target = DatabaseNameIdent;

    fn deref(&self) -> &Self::Target {
        &self.inner
    }
}

impl ListTableReq {
    pub fn new(tenant: impl Into<String>, db_name: impl Into<String>) -> ListTableReq {
        ListTableReq {
            inner: DatabaseNameIdent {
                tenant: tenant.into(),
                db_name: db_name.into(),
            },
        }
    }
}

#[derive(serde::Serialize, serde::Deserialize, Clone, Debug, Eq, PartialEq, Default)]

pub struct CountTablesKey {
    pub tenant: String,
}

impl Display for CountTablesKey {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "'{}'", self.tenant)
    }
}

/// count tables for a tenant
#[derive(serde::Serialize, serde::Deserialize, Clone, Debug, PartialEq)]
pub struct CountTablesReq {
    pub tenant: String,
}

pub struct CountTablesReply {
    pub count: u64,
}
