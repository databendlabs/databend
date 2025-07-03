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

use std::any::Any;
use std::collections::HashMap;

use databend_common_catalog::plan::DataSourcePlan;
use databend_common_exception::Result;
use databend_common_expression::ColumnId;
use databend_common_expression::DataSchemaRef;
use databend_common_expression::FieldIndex;
use databend_common_expression::RemoteExpr;
use databend_common_expression::TableSchemaRef;
use databend_common_meta_app::schema::TableInfo;
use databend_storages_common_table_meta::meta::ColumnStatistics;

use crate::executor::physical_plan::DeriveHandle;
use crate::executor::physical_plans::common::OnConflictField;
use crate::executor::IPhysicalPlan;
use crate::executor::PhysicalPlan;
use crate::executor::PhysicalPlanMeta;
use crate::ColumnBinding;

#[derive(Clone, Debug, serde::Serialize, serde::Deserialize)]
pub struct ReplaceDeduplicate {
    pub meta: PhysicalPlanMeta,
    pub input: Box<dyn IPhysicalPlan>,
    pub on_conflicts: Vec<OnConflictField>,
    pub bloom_filter_column_indexes: Vec<FieldIndex>,
    pub table_is_empty: bool,
    pub table_info: TableInfo,
    pub target_schema: TableSchemaRef,
    pub select_ctx: Option<ReplaceSelectCtx>,
    pub table_level_range_index: HashMap<ColumnId, ColumnStatistics>,
    pub need_insert: bool,
    pub delete_when: Option<(RemoteExpr, String)>,
}

#[typetag::serde]
impl IPhysicalPlan for ReplaceDeduplicate {
    fn as_any(&self) -> &dyn Any {
        self
    }
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

    fn derive(&self, mut children: Vec<Box<dyn IPhysicalPlan>>) -> Box<dyn IPhysicalPlan> {
        let mut new_physical_plan = self.clone();
        assert_eq!(children.len(), 1);
        new_physical_plan.input = children.pop().unwrap();
        Box::new(new_physical_plan)
    }
}

#[derive(Clone, Debug, serde::Serialize, serde::Deserialize)]
pub struct ReplaceSelectCtx {
    pub select_column_bindings: Vec<ColumnBinding>,
    pub select_schema: DataSchemaRef,
}
