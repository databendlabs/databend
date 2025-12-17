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
use databend_common_sql::optimizer::ir::SExpr;
use databend_common_sql::ColumnSet;

use crate::physical_plans::explain::PlanStatsInfo;
use crate::physical_plans::IPhysicalPlan;
use crate::physical_plans::PhysicalPlan;
use crate::physical_plans::PhysicalPlanBuilder;
use crate::physical_plans::PhysicalPlanMeta;
use crate::pipelines::PipelineBuilder;
use crate::sessions::QueryContext;

/// This is a binary operator that executes its children in order (left to right), and returns the results of the right child
#[derive(Clone, Debug, serde::Serialize, serde::Deserialize)]
pub struct Sequence {
    // A unique id of operator in a `PhysicalPlan` tree, only used for display.
    pub plan_id: u32,
    // Only used for explain
    pub stat_info: Option<PlanStatsInfo>,
    pub left: PhysicalPlan,
    pub right: PhysicalPlan,
    pub meta: PhysicalPlanMeta,
}

impl IPhysicalPlan for Sequence {
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
        self.right.output_schema()
    }

    fn children<'a>(&'a self) -> Box<dyn Iterator<Item = &'a PhysicalPlan> + 'a> {
        Box::new(std::iter::once(&self.left).chain(std::iter::once(&self.right)))
    }

    fn children_mut<'a>(&'a mut self) -> Box<dyn Iterator<Item = &'a mut PhysicalPlan> + 'a> {
        Box::new(std::iter::once(&mut self.left).chain(std::iter::once(&mut self.right)))
    }

    fn derive(&self, mut children: Vec<PhysicalPlan>) -> PhysicalPlan {
        assert_eq!(children.len(), 2);
        let right = children.pop().unwrap();
        let left = children.pop().unwrap();
        PhysicalPlan::new(Sequence {
            plan_id: self.plan_id,
            stat_info: self.stat_info.clone(),
            left,
            right,
            meta: self.meta.clone(),
        })
    }

    fn build_pipeline2(&self, builder: &mut PipelineBuilder) -> Result<()> {
        // init builder for cte pipeline
        let sub_context = QueryContext::create_from(builder.ctx.as_ref());
        let sub_builder = PipelineBuilder::create(
            builder.func_ctx.clone(),
            builder.settings.clone(),
            sub_context,
        );

        // build cte pipeline
        let build_res = sub_builder.finalize(&self.left)?;

        // add cte pipeline to pipelines
        builder.pipelines.push(build_res.main_pipeline);
        builder.pipelines.extend(build_res.sources_pipelines);

        // build main pipeline
        self.right.build_pipeline(builder)
    }
}

impl PhysicalPlanBuilder {
    pub(crate) async fn build_sequence(
        &mut self,
        s_expr: &SExpr,
        _sequence: &databend_common_sql::plans::Sequence,
        stat_info: PlanStatsInfo,
        required: ColumnSet,
    ) -> Result<PhysicalPlan> {
        let left_side = self.build(s_expr.child(0)?, Default::default()).await?;
        let right_side = self.build(s_expr.child(1)?, required).await?;
        Ok(PhysicalPlan::new(Sequence {
            plan_id: 0,
            stat_info: Some(stat_info),
            left: left_side,
            right: right_side,
            meta: PhysicalPlanMeta::new("Sequence"),
        }))
    }
}

crate::register_physical_plan!(Sequence => crate::physical_plans::physical_sequence::Sequence);
