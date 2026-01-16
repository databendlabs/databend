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
use std::fmt::Display;

use databend_common_exception::Result;
use databend_common_expression::DataSchemaRef;
use databend_common_expression::DataSchemaRefExt;

use crate::physical_plans::PhysicalPlanBuilder;
use crate::physical_plans::explain::PlanStatsInfo;
use crate::physical_plans::physical_plan::IPhysicalPlan;
use crate::physical_plans::physical_plan::PhysicalPlan;
use crate::physical_plans::physical_plan::PhysicalPlanMeta;
use crate::pipelines::PipelineBuilder;
use crate::pipelines::processors::transforms::TransformRecursiveCteScan;

#[derive(Clone, Debug, serde::Serialize, serde::Deserialize)]
pub struct RecursiveCteScan {
    meta: PhysicalPlanMeta,
    pub output_schema: DataSchemaRef,
    pub table_name: String,
    pub stat: PlanStatsInfo,
    pub exec_id: Option<u64>,
}

#[typetag::serde]
impl IPhysicalPlan for RecursiveCteScan {
    fn as_any(&self) -> &dyn Any {
        self
    }
    fn get_meta(&self) -> &PhysicalPlanMeta {
        &self.meta
    }

    fn get_meta_mut(&mut self) -> &mut PhysicalPlanMeta {
        &mut self.meta
    }

    #[recursive::recursive]
    fn output_schema(&self) -> Result<DataSchemaRef> {
        Ok(self.output_schema.clone())
    }

    fn derive(&self, children: Vec<PhysicalPlan>) -> PhysicalPlan {
        assert!(children.is_empty());
        PhysicalPlan::new(RecursiveCteScan {
            meta: self.meta.clone(),
            output_schema: self.output_schema.clone(),
            table_name: self.table_name.clone(),
            stat: self.stat.clone(),
            exec_id: self.exec_id,
        })
    }

    fn build_pipeline2(&self, builder: &mut PipelineBuilder) -> Result<()> {
        let max_threads = builder.settings.get_max_threads()?;
        builder.main_pipeline.add_source(
            |output_port| {
                TransformRecursiveCteScan::create(
                    builder.ctx.clone(),
                    output_port.clone(),
                    self.table_name.clone(),
                    self.exec_id,
                )
            },
            1,
        )?;
        builder.main_pipeline.resize(max_threads as usize, true)
    }
}

impl PhysicalPlanBuilder {
    pub async fn build_recursive_cte_scan(
        &mut self,
        recursive_cte_scan: &databend_common_sql::plans::RecursiveCteScan,
        stat_info: PlanStatsInfo,
    ) -> Result<PhysicalPlan> {
        Ok(PhysicalPlan::new(RecursiveCteScan {
            meta: PhysicalPlanMeta::new("RecursiveCteScan"),
            output_schema: DataSchemaRefExt::create(recursive_cte_scan.fields.clone()),
            table_name: recursive_cte_scan.table_name.clone(),
            stat: stat_info,
            exec_id: None,
        }))
    }
}

impl Display for RecursiveCteScan {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        write!(f, "RecursiveCTEScan")
    }
}
