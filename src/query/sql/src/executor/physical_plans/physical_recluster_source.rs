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

use databend_common_catalog::plan::PartStatistics;
use databend_common_catalog::plan::Partitions;
use databend_common_meta_app::schema::CatalogInfo;
use databend_common_meta_app::schema::TableInfo;

#[derive(Clone, Debug, serde::Serialize, serde::Deserialize)]
pub struct ReclusterTask {
    pub parts: Partitions,
    pub stats: PartStatistics,
    pub total_rows: usize,
    pub total_bytes: usize,
    pub level: i32,
}

#[derive(Clone, Debug, serde::Serialize, serde::Deserialize)]
pub struct ReclusterSource {
    pub plan_id: u32,
    pub tasks: Vec<ReclusterTask>,
    pub table_info: TableInfo,
    pub catalog_info: Arc<CatalogInfo>,
}
