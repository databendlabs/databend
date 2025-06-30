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

use databend_common_catalog::plan::{DataSourcePlan, StageTableInfo};
use databend_common_exception::Result;
use databend_common_expression::DataSchemaRef;
use databend_common_expression::DataSchemaRefExt;
use databend_common_expression::Scalar;
use databend_common_meta_app::schema::TableInfo;
use databend_storages_common_table_meta::meta::TableMetaTimestamps;
use enum_as_inner::EnumAsInner;

use crate::executor::physical_plan::PhysicalPlan;
use crate::plans::CopyIntoTableMode;
use crate::plans::ValidationMode;
use crate::ColumnBinding;
use crate::executor::{IPhysicalPlan, PhysicalPlanMeta};

#[derive(Clone, Debug, serde::Serialize, serde::Deserialize)]
pub struct CopyIntoTable {
    pub plan_id: u32,

    meta: PhysicalPlanMeta,
    pub required_values_schema: DataSchemaRef,
    pub values_consts: Vec<Scalar>,
    pub required_source_schema: DataSchemaRef,
    pub write_mode: CopyIntoTableMode,
    pub validation_mode: ValidationMode,
    pub stage_table_info: StageTableInfo,
    pub table_info: TableInfo,

    pub project_columns: Option<Vec<ColumnBinding>>,
    pub source: CopyIntoTableSource,
    pub is_transform: bool,
    pub table_meta_timestamps: TableMetaTimestamps,
}

impl IPhysicalPlan for CopyIntoTable {
    fn get_meta(&self) -> &PhysicalPlanMeta {
        &self.meta
    }

    fn get_meta_mut(&mut self) -> &mut PhysicalPlanMeta {
        &mut self.meta
    }

    fn output_schema(&self) -> Result<DataSchemaRef> {
        Ok(DataSchemaRefExt::create(vec![]))
    }

    fn children<'a>(&'a self) -> Box<dyn Iterator<Item=&'a Box<dyn IPhysicalPlan>> + 'a> {
        match &self.source {
            CopyIntoTableSource::Query(v) => Box::new(std::iter::once(v)),
            CopyIntoTableSource::Stage(v) => Box::new(std::iter::once(v)),
        }
    }

    fn children_mut<'a>(&'a self) -> Box<dyn Iterator<Item=&'a mut Box<dyn IPhysicalPlan>> + 'a> {
        match &mut self.source {
            CopyIntoTableSource::Query(v) => Box::new(std::iter::once(v)),
            CopyIntoTableSource::Stage(v) => Box::new(std::iter::once(v)),
        }
    }
}

#[derive(Clone, Debug, serde::Serialize, serde::Deserialize, EnumAsInner)]
pub enum CopyIntoTableSource {
    Query(Box<dyn IPhysicalPlan>),
    Stage(Box<dyn IPhysicalPlan>),
}
