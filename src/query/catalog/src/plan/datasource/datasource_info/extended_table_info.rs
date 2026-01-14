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

use databend_common_expression::TableSchema;
use databend_common_meta_app::schema::BranchInfo;
use databend_common_meta_app::schema::TableInfo;

#[derive(serde::Serialize, serde::Deserialize, Clone, Debug)]
pub struct ExtendedTableInfo {
    pub table_info: TableInfo,
    pub branch_info: Option<BranchInfo>,
}

impl ExtendedTableInfo {
    pub fn schema(&self) -> Arc<TableSchema> {
        self.branch_info
            .as_ref()
            .map_or_else(|| self.table_info.schema(), |v| v.schema.clone())
    }

    pub fn desc(&self) -> String {
        self.table_info.desc.clone()
    }

    pub fn catalog_name(&self) -> &str {
        &self.table_info.catalog_info.name_ident.catalog_name
    }
}
