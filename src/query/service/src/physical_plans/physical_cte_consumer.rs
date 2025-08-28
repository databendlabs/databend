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

use databend_common_exception::Result;
use databend_common_expression::DataField;
use databend_common_expression::DataSchemaRef;
use databend_common_expression::DataSchemaRefExt;

use crate::physical_plans::explain::PlanStatsInfo;
use crate::physical_plans::format::MaterializeCTERefFormatter;
use crate::physical_plans::format::PhysicalFormat;
use crate::physical_plans::IPhysicalPlan;
use crate::physical_plans::PhysicalPlan;
use crate::physical_plans::PhysicalPlanBuilder;
use crate::physical_plans::PhysicalPlanMeta;
use crate::pipelines::processors::transforms::CTESource;
use crate::pipelines::PipelineBuilder;

/// This is a leaf operator that consumes the result of a materialized CTE.
#[derive(Clone, Debug, serde::Serialize, serde::Deserialize)]
pub struct MaterializeCTERef {
    // A unique id of operator in a `PhysicalPlan` tree, only used for display.
    pub plan_id: u32,
    // Only used for explain
    pub stat_info: Option<PlanStatsInfo>,
    pub cte_name: String,
    pub cte_schema: DataSchemaRef,

    pub meta: PhysicalPlanMeta,
}

#[typetag::serde]
impl IPhysicalPlan for MaterializeCTERef {
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
        Ok(self.cte_schema.clone())
    }

    fn derive(&self, children: Vec<PhysicalPlan>) -> PhysicalPlan {
        assert!(children.is_empty());
        PhysicalPlan::new(MaterializeCTERef {
            plan_id: self.plan_id,
            stat_info: self.stat_info.clone(),
            cte_name: self.cte_name.clone(),
            cte_schema: self.cte_schema.clone(),
            meta: self.meta.clone(),
        })
    }

    fn formatter(&self) -> Result<Box<dyn PhysicalFormat + '_>> {
        Ok(MaterializeCTERefFormatter::create(self))
    }

    fn build_pipeline2(&self, builder: &mut PipelineBuilder) -> Result<()> {
        let receiver = builder.ctx.get_materialized_cte_receiver(&self.cte_name);
        builder.main_pipeline.add_source(
            |output_port| {
                CTESource::create(builder.ctx.clone(), output_port.clone(), receiver.clone())
            },
            builder.settings.get_max_threads()? as usize,
        )
    }
}

impl PhysicalPlanBuilder {
    pub(crate) async fn build_cte_consumer(
        &mut self,
        cte_consumer: &databend_common_sql::plans::MaterializedCTERef,
        stat_info: PlanStatsInfo,
    ) -> Result<PhysicalPlan> {
        let mut fields = Vec::new();
        let metadata = self.metadata.read();

        for index in &cte_consumer.output_columns {
            let column = metadata.column(*index);
            let mut data_type = column.data_type();
            fields.push(DataField::new(&index.to_string(), data_type));
        }
        let cte_schema = DataSchemaRefExt::create(fields);
        Ok(PhysicalPlan::new(MaterializeCTERef {
            plan_id: 0,
            stat_info: Some(stat_info),
            cte_name: cte_consumer.cte_name.clone(),
            cte_schema,
            meta: PhysicalPlanMeta::new("MaterializeCTERef"),
        }))
    }
}
