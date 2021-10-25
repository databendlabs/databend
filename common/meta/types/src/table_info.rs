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
use std::sync::Arc;

use common_datavalues::DataSchema;

use crate::MetaVersion;

#[derive(serde::Serialize, serde::Deserialize, Clone, Debug, Eq, PartialEq)]
pub struct TableInfo {
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

    pub desc: String,
    pub name: String,

    pub schema: Arc<DataSchema>,
    pub engine: String,
    pub options: HashMap<String, String>,
}

impl TableInfo {
    /// Create a TableInfo with only db, table, schema
    pub fn simple(db: &str, table: &str, schema: Arc<DataSchema>) -> TableInfo {
        TableInfo {
            desc: format!("'{}'.'{}'", db, table),
            name: table.to_string(),
            schema,
            ..Default::default()
        }
    }

    pub fn schema(mut self, schema: Arc<DataSchema>) -> TableInfo {
        self.schema = schema;
        self
    }
}

impl Default for TableInfo {
    fn default() -> Self {
        TableInfo {
            table_id: 0,
            version: 0,
            desc: "".to_string(),
            name: "".to_string(),
            schema: Arc::new(DataSchema::empty()),
            engine: "".to_string(),
            options: HashMap::new(),
        }
    }
}

impl Display for TableInfo {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "DB.Table: {}, Table: {}-{}, Version: {}, Engine: {}",
            self.desc, self.name, self.table_id, self.version, self.engine
        )
    }
}
