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

use common_catalog::plan::Filters;
use common_catalog::plan::Partitions;
use common_exception::Result;
use common_expression::DataSchemaRef;
use common_meta_app::schema::CatalogInfo;
use common_meta_app::schema::TableInfo;
use storages_common_table_meta::meta::TableSnapshot;

#[derive(Clone, Debug, serde::Serialize, serde::Deserialize)]
pub struct DeleteSource {
    pub parts: Partitions,
    pub filters: Filters,
    pub table_info: TableInfo,
    pub catalog_info: CatalogInfo,
    pub col_indices: Vec<usize>,
    pub query_row_id_col: bool,
    pub snapshot: Arc<TableSnapshot>,
}

impl DeleteSource {
    pub fn output_schema(&self) -> Result<DataSchemaRef> {
        Ok(DataSchemaRef::default())
    }
}
