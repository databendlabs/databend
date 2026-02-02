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
use databend_common_expression::DataSchemaRef;
use databend_common_pipeline_transforms::TransformPipelineHelper;
use databend_common_pipeline_transforms::blocks::CompoundBlockOperator;
use databend_common_sql::evaluator::BlockOperator;
use databend_common_sql::optimizer::ir::SExpr;

use crate::physical_plans::IPhysicalPlan;
use crate::physical_plans::PhysicalPlan;
use crate::physical_plans::PhysicalPlanBuilder;
use crate::physical_plans::PhysicalPlanMeta;
use crate::physical_plans::explain::PlanStatsInfo;
use crate::physical_plans::format::MaterializedCTEFormatter;
use crate::physical_plans::format::PhysicalFormat;
use crate::pipelines::PipelineBuilder;
use crate::pipelines::processors::transforms::MaterializedCteSink;

#[derive(Clone, Debug, serde::Serialize, serde::Deserialize)]
pub struct MaterializedCTE {
    // A unique id of operator in a `PhysicalPlan` tree, only used for display.
    pub plan_id: u32,
    // Only used for explain
    pub stat_info: Option<PlanStatsInfo>,
    pub input: PhysicalPlan,
    pub cte_name: String,
    pub cte_output_columns: Option<Vec<usize>>,
    pub ref_count: usize,
    pub channel_size: Option<usize>,
    pub meta: PhysicalPlanMeta,
}

#[typetag::serde]
impl IPhysicalPlan for MaterializedCTE {
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
        self.input.output_schema()
    }

    fn children<'a>(&'a self) -> Box<dyn Iterator<Item = &'a PhysicalPlan> + 'a> {
        Box::new(std::iter::once(&self.input))
    }

    fn children_mut<'a>(&'a mut self) -> Box<dyn Iterator<Item = &'a mut PhysicalPlan> + 'a> {
        Box::new(std::iter::once(&mut self.input))
    }

    fn formatter(&self) -> Result<Box<dyn PhysicalFormat + '_>> {
        Ok(MaterializedCTEFormatter::create(self))
    }

    fn derive(&self, mut children: Vec<PhysicalPlan>) -> PhysicalPlan {
        assert_eq!(children.len(), 1);
        let input = children.pop().unwrap();
        PhysicalPlan::new(MaterializedCTE {
            plan_id: self.plan_id,
            stat_info: self.stat_info.clone(),
            input,
            cte_name: self.cte_name.clone(),
            cte_output_columns: self.cte_output_columns.clone(),
            ref_count: self.ref_count,
            channel_size: self.channel_size,
            meta: self.meta.clone(),
        })
    }

    fn build_pipeline2(&self, builder: &mut PipelineBuilder) -> Result<()> {
        self.input.build_pipeline(builder)?;

        let input_schema = self.input.output_schema()?;
        if let Some(output_columns) = &self.cte_output_columns {
            let mut projections = Vec::with_capacity(output_columns.len());
            for index in output_columns {
                projections.push(input_schema.index_of(index.to_string().as_str())?);
            }
            let num_input_columns = input_schema.num_fields();
            builder.main_pipeline.add_transformer(|| {
                CompoundBlockOperator::new(
                    vec![BlockOperator::Project {
                        projection: projections.clone(),
                    }],
                    builder.func_ctx.clone(),
                    num_input_columns,
                )
            });
        }

        builder.main_pipeline.try_resize(1)?;
        let tx = builder.ctx.get_materialized_cte_senders(
            &self.cte_name,
            self.ref_count,
            self.channel_size,
        );
        builder
            .main_pipeline
            .add_sink(|input| MaterializedCteSink::create(input, tx.clone()))
    }
}

impl PhysicalPlanBuilder {
    pub(crate) async fn build_materialized_cte(
        &mut self,
        s_expr: &SExpr,
        materialized_cte: &databend_common_sql::plans::MaterializedCTE,
        stat_info: PlanStatsInfo,
    ) -> Result<PhysicalPlan> {
        let required = self
            .cte_required_columns
            .get(&materialized_cte.cte_name)
            .unwrap()
            .clone();
        let cte_output_columns = Some(required.iter().copied().collect());
        let input = self.build_physical_plan(s_expr.child(0)?, required).await?;
        Ok(PhysicalPlan::new(MaterializedCTE {
            plan_id: 0,
            stat_info: Some(stat_info),
            input,
            cte_name: materialized_cte.cte_name.clone(),
            cte_output_columns,
            ref_count: materialized_cte.ref_count,
            channel_size: materialized_cte.channel_size,
            meta: PhysicalPlanMeta::new("MaterializedCTE"),
        }))
    }
}
