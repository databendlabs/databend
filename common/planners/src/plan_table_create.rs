// Copyright 2020 Datafuse Labs.
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

use common_datavalues::DataSchemaRef;
use common_meta_types::TableMeta;

pub type TableOptions = HashMap<String, String>;

#[derive(serde::Serialize, serde::Deserialize, Clone, Debug, PartialEq)]
pub struct CreateTablePlan {
    pub if_not_exists: bool,
    pub db: String,
    /// The table name
    pub table: String,

    pub table_meta: TableMeta,
}

impl CreateTablePlan {
    pub fn schema(&self) -> DataSchemaRef {
        self.table_meta.schema.clone()
    }

    pub fn options(&self) -> &HashMap<String, String> {
        &self.table_meta.options
    }

    pub fn engine(&self) -> &str {
        &self.table_meta.engine
    }
}
