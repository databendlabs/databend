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

use std::collections::HashMap;
use std::fmt;
use std::fmt::Display;
use std::fmt::Formatter;
use std::ops::Deref;
use std::sync::Arc;

use common_datavalues::chrono::DateTime;
use common_datavalues::chrono::Utc;
use common_datavalues::prelude::*;
use maplit::hashmap;

use crate::database::DatabaseNameIdent;
use crate::MatchSeq;
use crate::MetaVersion;

/// Globally unique identifier of a version of TableMeta.
#[derive(serde::Serialize, serde::Deserialize, Clone, Debug, Eq, PartialEq, Default)]
pub struct TableIdent {
    /// Globally unique id to identify a table.
    pub table_id: u64,

    /// version of this table snapshot.
    ///
    /// Any change to a table causes the version to increment, e.g. insert or delete rows, update schema etc.
    /// But renaming a table should not affect the version, since the table itself does not change.
    /// The tuple (database_id, table_id, version) identifies a unique and consistent table snapshot.
    ///
    /// A version is not guaranteed to be consecutive.
    ///
    pub version: MetaVersion,
}

impl TableIdent {
    pub fn new(table_id: u64, version: MetaVersion) -> Self {
        TableIdent { table_id, version }
    }
}

impl Display for TableIdent {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        write!(f, "table_id:{}, ver:{}", self.table_id, self.version)
    }
}

#[derive(serde::Serialize, serde::Deserialize, Clone, Debug, Eq, PartialEq, Default)]
pub struct TableNameIndent {
    pub tenant: String,
    pub db_name: String,
    pub table_name: String,
}

impl TableNameIndent {
    pub fn new(
        tenant: impl Into<String>,
        db_name: impl Into<String>,
        table_name: impl Into<String>,
    ) -> TableNameIndent {
        TableNameIndent {
            tenant: tenant.into(),
            db_name: db_name.into(),
            table_name: table_name.into(),
        }
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

/// The essential state that defines what a table is.
///
/// It is what a meta store just needs to save.
#[derive(serde::Serialize, serde::Deserialize, Clone, Debug, Eq, PartialEq)]
pub struct TableMeta {
    pub schema: Arc<DataSchema>,
    pub engine: String,
    pub engine_options: HashMap<String, String>,
    pub options: HashMap<String, String>,
    pub created_on: DateTime<Utc>,
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

    pub fn options(&self) -> &HashMap<String, String> {
        &self.meta.options
    }

    pub fn engine(&self) -> &str {
        &self.meta.engine
    }

    pub fn engine_options(&self) -> &HashMap<String, String> {
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
            engine_options: HashMap::new(),
            options: HashMap::new(),
            created_on: Utc::now(),
        }
    }
}

impl Display for TableMeta {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "Engine: {}={:?}, Schema: {}, Options: {:?} CreatedOn: {:?}",
            self.engine, self.engine_options, self.schema, self.options, self.created_on
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

#[derive(serde::Serialize, serde::Deserialize, Clone, Debug, PartialEq)]
pub struct CreateTableReq {
    pub if_not_exists: bool,
    pub tenant: String,
    pub db: String,
    pub table: String,
    pub table_meta: TableMeta,
}

#[derive(serde::Serialize, serde::Deserialize, Clone, Debug, Eq, PartialEq)]
pub struct CreateTableReply {
    pub table_id: u64,
}

#[derive(serde::Serialize, serde::Deserialize, Clone, Debug, PartialEq)]
pub struct DropTableReq {
    pub if_exists: bool,
    pub tenant: String,
    pub db: String,
    pub table: String,
}

#[derive(serde::Serialize, serde::Deserialize, Clone, Debug, PartialEq)]
pub struct DropTableReply {}

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

impl UpsertTableOptionReq {
    pub fn new(
        table_ident: &TableIdent,
        key: impl Into<String>,
        value: impl Into<String>,
    ) -> UpsertTableOptionReq {
        UpsertTableOptionReq {
            table_id: table_ident.table_id,
            seq: MatchSeq::Exact(table_ident.version),
            options: hashmap! {key.into() => Some(value.into())},
        }
    }
}

#[derive(serde::Serialize, serde::Deserialize, Clone, Debug, PartialEq)]
pub struct UpsertTableOptionReply {}

#[derive(serde::Serialize, serde::Deserialize, Clone, Debug, PartialEq)]
pub struct GetTableReq {
    pub inner: TableNameIndent,
}

impl Deref for GetTableReq {
    type Target = TableNameIndent;

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
            inner: TableNameIndent::new(tenant, db_name, table_name),
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
