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

//! Table identifier types

use std::fmt;
use std::fmt::Display;
use std::fmt::Formatter;

use crate::schema::database_name_ident::DatabaseNameIdent;
use crate::tenant::Tenant;
use crate::tenant::ToTenant;

/// Globally unique identifier of a version of TableMeta.
#[derive(serde::Serialize, serde::Deserialize, Clone, Copy, Debug, Eq, PartialEq, Default)]
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
    fn fmt(&self, f: &mut Formatter) -> fmt::Result {
        write!(f, "table_id:{}, ver:{}", self.table_id, self.seq)
    }
}

#[derive(Clone, Debug, Eq, PartialEq, Hash)]
pub struct TableNameIdent {
    pub tenant: Tenant,
    pub db_name: String,
    pub table_name: String,
}

impl TableNameIdent {
    pub fn new(
        tenant: impl ToTenant,
        db_name: impl ToString,
        table_name: impl ToString,
    ) -> TableNameIdent {
        TableNameIdent {
            tenant: tenant.to_tenant(),
            db_name: db_name.to_string(),
            table_name: table_name.to_string(),
        }
    }

    pub fn tenant(&self) -> &Tenant {
        &self.tenant
    }

    pub fn table_name(&self) -> String {
        self.table_name.clone()
    }

    pub fn db_name_ident(&self) -> DatabaseNameIdent {
        DatabaseNameIdent::new(&self.tenant, &self.db_name)
    }
}

impl Display for TableNameIdent {
    fn fmt(&self, f: &mut Formatter) -> fmt::Result {
        write!(
            f,
            "'{}'.'{}'.'{}'",
            self.tenant.tenant_name(),
            self.db_name,
            self.table_name
        )
    }
}

#[derive(Clone, Debug, Eq, PartialEq, Default)]
pub struct DBIdTableName {
    pub db_id: u64,
    pub table_name: String,
}

impl DBIdTableName {
    pub fn new(db_id: u64, table_name: impl ToString) -> Self {
        DBIdTableName {
            db_id,
            table_name: table_name.to_string(),
        }
    }
    pub fn display(&self) -> impl Display {
        format!("{}.'{}'", self.db_id, self.table_name)
    }
}

impl Display for DBIdTableName {
    fn fmt(&self, f: &mut Formatter) -> fmt::Result {
        write!(f, "{}.'{}'", self.db_id, self.table_name)
    }
}

#[derive(Clone, Debug, Eq, PartialEq, Default)]
pub struct TableId {
    pub table_id: u64,
}

impl TableId {
    pub fn new(table_id: u64) -> Self {
        TableId { table_id }
    }
}

impl Display for TableId {
    fn fmt(&self, f: &mut Formatter) -> fmt::Result {
        write!(f, "TableId{{{}}}", self.table_id)
    }
}

/// The meta-service key for storing table id history ever used by a table name
#[derive(Clone, Debug, Eq, PartialEq, Hash)]
pub struct TableIdHistoryIdent {
    pub database_id: u64,
    pub table_name: String,
}

impl Display for TableIdHistoryIdent {
    fn fmt(&self, f: &mut Formatter) -> fmt::Result {
        write!(f, "{}.'{}'", self.database_id, self.table_name)
    }
}
