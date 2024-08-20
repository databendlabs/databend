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

use std::sync::Arc;

use arrow_schema::Schema;
use databend_common_expression::TableSchema;
use databend_common_meta_app::schema::TableInfo;

#[derive(serde::Serialize, serde::Deserialize, Clone, PartialEq, Eq, Debug)]
pub struct ResultScanTableInfo {
    pub table_info: TableInfo,
    pub query_id: String,
    pub location: String,
    pub schema: Schema,
    pub file_size: u64,
}

impl ResultScanTableInfo {
    pub fn schema(&self) -> Arc<TableSchema> {
        self.table_info.schema()
    }

    pub fn desc(&self) -> String {
        "RESULT_SCAN table function".to_string()
    }
}
