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

use databend_common_catalog::plan::DataSourcePlan;
use databend_common_exception::Result;
use databend_common_expression::BlockThresholds;
use databend_common_expression::DataSchemaRef;
use databend_common_expression::FieldIndex;
use databend_common_meta_app::schema::TableInfo;
use databend_storages_common_table_meta::meta::BlockSlotDescription;
use databend_storages_common_table_meta::meta::Location;
use databend_storages_common_table_meta::meta::TableMetaTimestamps;

use crate::executor::physical_plan::PhysicalPlanDeriveHandle;
use crate::executor::physical_plans::common::OnConflictField;
use crate::executor::IPhysicalPlan;
use crate::executor::PhysicalPlan;
use crate::executor::PhysicalPlanMeta;

#[derive(Clone, Debug, serde::Serialize, serde::Deserialize)]
pub struct ReplaceInto {
    pub meta: PhysicalPlanMeta,
    pub input: Box<dyn IPhysicalPlan>,
    pub block_thresholds: BlockThresholds,
    pub table_info: TableInfo,
    pub on_conflicts: Vec<OnConflictField>,
    pub bloom_filter_column_indexes: Vec<FieldIndex>,
    pub segments: Vec<(usize, Location)>,
    pub block_slots: Option<BlockSlotDescription>,
    pub need_insert: bool,
    pub table_meta_timestamps: TableMetaTimestamps,
}

#[typetag::serde]
impl IPhysicalPlan for ReplaceInto {
    fn get_meta(&self) -> &PhysicalPlanMeta {
        &self.meta
    }

    fn get_meta_mut(&mut self) -> &mut PhysicalPlanMeta {
        &mut self.meta
    }

    fn output_schema(&self) -> Result<DataSchemaRef> {
        Ok(DataSchemaRef::default())
    }

    fn children<'a>(&'a self) -> Box<dyn Iterator<Item = &'a Box<dyn IPhysicalPlan>> + 'a> {
        Box::new(std::iter::once(&self.input))
    }

    fn children_mut<'a>(
        &'a mut self,
    ) -> Box<dyn Iterator<Item = &'a mut Box<dyn IPhysicalPlan>> + 'a> {
        Box::new(std::iter::once(&mut self.input))
    }

    fn derive_with(
        &self,
        handle: &mut Box<dyn PhysicalPlanDeriveHandle>,
    ) -> Box<dyn IPhysicalPlan> {
        let derive_input = self.input.derive_with(handle);

        match handle.derive(self, vec![derive_input]) {
            Ok(v) => v,
            Err(children) => {
                let mut new_replace_into = self.clone();
                assert_eq!(children.len(), 1);
                new_replace_into.input = children[0];
                Box::new(new_replace_into)
            }
        }
    }
}
