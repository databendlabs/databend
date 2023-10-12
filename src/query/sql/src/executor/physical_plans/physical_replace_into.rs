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

use common_expression::BlockThresholds;
use common_expression::FieldIndex;
use common_meta_app::schema::CatalogInfo;
use common_meta_app::schema::TableInfo;
use storages_common_table_meta::meta::BlockSlotDescription;
use storages_common_table_meta::meta::Location;

use crate::executor::physical_plans::common::OnConflictField;
use crate::executor::PhysicalPlan;

#[derive(Clone, Debug, serde::Serialize, serde::Deserialize)]
pub struct ReplaceInto {
    pub input: Box<PhysicalPlan>,
    pub block_thresholds: BlockThresholds,
    pub table_info: TableInfo,
    pub on_conflicts: Vec<OnConflictField>,
    pub bloom_filter_column_indexes: Vec<FieldIndex>,
    pub catalog_info: CatalogInfo,
    pub segments: Vec<(usize, Location)>,
    pub block_slots: Option<BlockSlotDescription>,
    pub need_insert: bool,
}
