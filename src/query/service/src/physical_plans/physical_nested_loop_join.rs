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
use std::sync::Arc;

use databend_common_exception::Result;
use databend_common_expression::DataSchemaRef;
use databend_common_expression::DataSchemaRefExt;
use databend_common_expression::RemoteExpr;
use databend_common_functions::BUILTIN_FUNCTIONS;
use databend_common_pipeline::core::ProcessorPtr;
use databend_common_pipeline::sinks::Sinker;
use databend_common_sql::executor::cast_expr_to_non_null_boolean;
use databend_common_sql::optimizer::ir::SExpr;
use databend_common_sql::plans::JoinType;
use databend_common_sql::ColumnSet;
use databend_common_sql::ScalarExpr;
use itertools::Itertools;

use super::explain::PlanStatsInfo;
use super::format::NestedLoopJoinFormatter;
use super::format::PhysicalFormat;
use super::resolve_scalar;
use super::IPhysicalPlan;
use super::PhysicalPlan;
use super::PhysicalPlanBuilder;
use super::PhysicalPlanMeta;
use crate::pipelines::processors::transforms::LoopJoinState;
use crate::pipelines::processors::transforms::TransformLoopJoinLeft;
use crate::pipelines::processors::transforms::TransformLoopJoinRight;
use crate::pipelines::PipelineBuilder;

#[derive(Clone, Debug, serde::Serialize, serde::Deserialize)]
pub struct NestedLoopJoin {
    pub meta: PhysicalPlanMeta,
    pub left: PhysicalPlan,
    pub right: PhysicalPlan,
    pub conditions: Vec<RemoteExpr>,
    pub join_type: JoinType,
    pub output_schema: DataSchemaRef,

    // Only used for explain
    pub stat_info: Option<PlanStatsInfo>,
}

#[typetag::serde]
impl IPhysicalPlan for NestedLoopJoin {
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

    fn children<'a>(&'a self) -> Box<dyn Iterator<Item = &'a PhysicalPlan> + 'a> {
        Box::new([&self.left, &self.right].into_iter())
    }

    fn children_mut<'a>(&'a mut self) -> Box<dyn Iterator<Item = &'a mut PhysicalPlan> + 'a> {
        Box::new([&mut self.left, &mut self.right].into_iter())
    }

    fn formatter(&self) -> Result<Box<dyn PhysicalFormat + '_>> {
        Ok(NestedLoopJoinFormatter::create(self))
    }

    fn get_desc(&self) -> Result<String> {
        Ok(self
            .conditions
            .iter()
            .map(|x| x.as_expr(&BUILTIN_FUNCTIONS).sql_display())
            .join(" AND "))
    }

    fn derive(&self, mut children: Vec<PhysicalPlan>) -> PhysicalPlan {
        assert_eq!(children.len(), 2);
        let right_child = children.pop().unwrap();
        let left_child = children.pop().unwrap();

        PhysicalPlan::new(NestedLoopJoin {
            meta: self.meta.clone(),
            left: left_child,
            right: right_child,
            conditions: self.conditions.clone(),
            join_type: self.join_type,
            output_schema: self.output_schema.clone(),
            stat_info: self.stat_info.clone(),
        })
    }

    fn build_pipeline2(&self, builder: &mut PipelineBuilder) -> Result<()> {
        let state = Arc::new(LoopJoinState::new(builder.ctx.clone(), self));
        self.build_right(state.clone(), builder)?;
        self.build_left(state, builder)
    }
}

impl NestedLoopJoin {
    fn build_left(&self, state: Arc<LoopJoinState>, builder: &mut PipelineBuilder) -> Result<()> {
        self.left.build_pipeline(builder)?;

        let max_threads = builder.settings.get_max_threads()? as usize;
        builder.main_pipeline.try_resize(max_threads)?;
        builder.main_pipeline.add_transform(|input, output| {
            Ok(ProcessorPtr::create(TransformLoopJoinLeft::create(
                input,
                output,
                state.clone(),
            )))
        })?;

        match self.conditions.len() {
            0 => Ok(()),
            1 => {
                let expr =
                    cast_expr_to_non_null_boolean(self.conditions[0].as_expr(&BUILTIN_FUNCTIONS))?;
                let projections = (0..self.output_schema.num_fields()).collect();
                builder.main_pipeline.add_transform(
                    builder.filter_transform_builder(&[expr.as_remote_expr()], projections)?,
                )
            }
            _ => {
                let projections = (0..self.output_schema.num_fields()).collect();
                builder
                    .main_pipeline
                    .add_transform(builder.filter_transform_builder(&self.conditions, projections)?)
            }
        }
    }

    fn build_right(&self, state: Arc<LoopJoinState>, builder: &mut PipelineBuilder) -> Result<()> {
        let right_side_builder = builder.create_sub_pipeline_builder();

        let mut right_res = right_side_builder.finalize(&self.right)?;
        right_res.main_pipeline.add_sink(|input| {
            Ok(ProcessorPtr::create(Sinker::create(
                input,
                TransformLoopJoinRight::create(state.clone())?,
            )))
        })?;

        builder
            .pipelines
            .push(right_res.main_pipeline.finalize(None));
        builder.pipelines.extend(right_res.sources_pipelines);
        Ok(())
    }
}

impl PhysicalPlanBuilder {
    pub async fn build_loop_join(
        &mut self,
        join_type: JoinType,
        s_expr: &SExpr,
        left_required: ColumnSet,
        right_required: ColumnSet,
        conditions: Vec<ScalarExpr>,
    ) -> Result<PhysicalPlan> {
        let (left, right) = self
            .build_join_sides(s_expr, left_required, right_required)
            .await?;

        let left_schema = self.prepare_probe_schema(join_type, &left)?;
        let right_schema = self.prepare_build_schema(join_type, &right)?;

        let output_schema = DataSchemaRefExt::create(
            left_schema
                .fields()
                .iter()
                .chain(right_schema.fields())
                .cloned()
                .collect::<Vec<_>>(),
        );

        let conditions = conditions
            .iter()
            .map(|scalar| resolve_scalar(scalar, &output_schema))
            .collect::<Result<_>>()?;

        Ok(PhysicalPlan::new(NestedLoopJoin {
            left,
            right,
            meta: PhysicalPlanMeta::new("NestedLoopJoin"),
            conditions,
            join_type,
            output_schema,
            stat_info: Some(self.build_plan_stat_info(s_expr)?),
        }))
    }
}
