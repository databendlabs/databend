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
use databend_common_expression::DataSchema;
use databend_common_expression::DataSchemaRef;
use databend_common_expression::DataSchemaRefExt;
use databend_common_expression::RemoteExpr;
use databend_common_functions::BUILTIN_FUNCTIONS;
use databend_common_pipeline::core::ProcessorPtr;
use databend_common_pipeline_transforms::blocks::CompoundBlockOperator;
use databend_common_sql::ColumnSet;
use databend_common_sql::IndexType;
use databend_common_sql::ScalarExpr;
use databend_common_sql::TypeCheck;
use databend_common_sql::evaluator::BlockOperator;
use databend_common_sql::optimizer::ir::SExpr;
use itertools::Itertools;

use crate::physical_plans::PhysicalPlanBuilder;
use crate::physical_plans::explain::PlanStatsInfo;
use crate::physical_plans::format::PhysicalFormat;
use crate::physical_plans::format::UnionAllFormatter;
use crate::physical_plans::physical_plan::IPhysicalPlan;
use crate::physical_plans::physical_plan::PhysicalPlan;
use crate::physical_plans::physical_plan::PhysicalPlanMeta;
use crate::pipelines::PipelineBuilder;
use crate::pipelines::processors::transforms::TransformRecursiveCteSource;

#[derive(Clone, Debug, serde::Serialize, serde::Deserialize)]
pub struct UnionAll {
    meta: PhysicalPlanMeta,
    pub left: PhysicalPlan,
    pub right: PhysicalPlan,
    pub left_outputs: Vec<(IndexType, Option<RemoteExpr>)>,
    pub right_outputs: Vec<(IndexType, Option<RemoteExpr>)>,
    pub schema: DataSchemaRef,
    pub cte_scan_names: Vec<String>,

    // Only used for explain
    pub stat_info: Option<PlanStatsInfo>,
}

impl IPhysicalPlan for UnionAll {
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
        Ok(self.schema.clone())
    }

    fn children<'a>(&'a self) -> Box<dyn Iterator<Item = &'a PhysicalPlan> + 'a> {
        Box::new(std::iter::once(&self.left).chain(std::iter::once(&self.right)))
    }

    fn children_mut<'a>(&'a mut self) -> Box<dyn Iterator<Item = &'a mut PhysicalPlan> + 'a> {
        Box::new(std::iter::once(&mut self.left).chain(std::iter::once(&mut self.right)))
    }

    fn formatter(&self) -> Result<Box<dyn PhysicalFormat + '_>> {
        Ok(UnionAllFormatter::create(self))
    }

    fn get_desc(&self) -> Result<String> {
        Ok(self
            .left_outputs
            .iter()
            .zip(self.right_outputs.iter())
            .map(|(l, r)| format!("#{} <- #{}", l.0, r.0))
            .join(", "))
    }

    fn derive(&self, mut children: Vec<PhysicalPlan>) -> PhysicalPlan {
        assert_eq!(children.len(), 2);
        let right = children.pop().unwrap();
        let left = children.pop().unwrap();
        PhysicalPlan::new(UnionAll {
            meta: self.meta.clone(),
            left,
            right,
            left_outputs: self.left_outputs.clone(),
            right_outputs: self.right_outputs.clone(),
            schema: self.schema.clone(),
            cte_scan_names: self.cte_scan_names.clone(),
            stat_info: self.stat_info.clone(),
        })
    }

    fn build_pipeline2(&self, builder: &mut PipelineBuilder) -> Result<()> {
        if !self.cte_scan_names.is_empty() {
            return self.build_recursive_cte_source(builder);
        }

        self.left.build_pipeline(builder)?;
        self.project_input(self.left.output_schema()?, &self.left_outputs, builder)?;
        let left_sinks = builder.main_pipeline.take_sinks();

        self.right.build_pipeline(builder)?;
        self.project_input(self.right.output_schema()?, &self.right_outputs, builder)?;
        let right_sinks = builder.main_pipeline.take_sinks();

        let outputs = std::cmp::max(left_sinks.len(), right_sinks.len());
        let sequence_groups = vec![(left_sinks.len(), false), (right_sinks.len(), false)];

        builder.main_pipeline.extend_sinks(left_sinks);
        builder.main_pipeline.extend_sinks(right_sinks);

        let enable_parallel_union_all = builder.settings.get_enable_parallel_union_all()?
            || builder.settings.get_grouping_sets_to_union()?;
        match enable_parallel_union_all {
            true => builder.main_pipeline.resize(outputs, false),
            false => builder
                .main_pipeline
                .sequence_group(sequence_groups, outputs),
        }
    }
}

impl UnionAll {
    fn project_input(
        &self,
        schema: DataSchemaRef,
        projection: &[(IndexType, Option<RemoteExpr>)],
        builder: &mut PipelineBuilder,
    ) -> Result<()> {
        let mut expr_offset = schema.num_fields();
        let mut new_projection = Vec::with_capacity(projection.len());
        let mut exprs = Vec::with_capacity(projection.len());
        for (idx, expr) in projection {
            let Some(expr) = expr else {
                new_projection.push(schema.index_of(&idx.to_string())?);
                continue;
            };

            exprs.push(expr.as_expr(&BUILTIN_FUNCTIONS));
            new_projection.push(expr_offset);
            expr_offset += 1;
        }

        let mut operators = Vec::with_capacity(2);
        if !exprs.is_empty() {
            operators.push(BlockOperator::Map {
                exprs,
                projections: None,
            });
        }

        operators.push(BlockOperator::Project {
            projection: new_projection,
        });

        builder.main_pipeline.add_transform(|input, output| {
            Ok(ProcessorPtr::create(CompoundBlockOperator::create(
                input,
                output,
                schema.num_fields(),
                builder.func_ctx.clone(),
                operators.clone(),
            )))
        })
    }

    fn build_recursive_cte_source(&self, builder: &mut PipelineBuilder) -> Result<()> {
        let max_threads = builder.settings.get_max_threads()?;
        builder.main_pipeline.add_source(
            |output_port| {
                TransformRecursiveCteSource::try_create(
                    builder.ctx.clone(),
                    output_port.clone(),
                    self.clone(),
                )
            },
            1,
        )?;

        builder.main_pipeline.resize(max_threads as usize, true)
    }
}

impl PhysicalPlanBuilder {
    pub async fn build_union_all(
        &mut self,
        s_expr: &SExpr,
        union_all: &databend_common_sql::plans::UnionAll,
        required: ColumnSet,
        stat_info: PlanStatsInfo,
    ) -> Result<PhysicalPlan> {
        // 1. Prune unused Columns.
        // Use left's output columns as the offset indices
        // if the union has a CTE, the output columns are not filtered
        // otherwise, if the output columns of the union do not contain the columns used by the plan in the union, the expression will fail to obtain data.
        let (offset_indices, left_required, right_required) =
            if !union_all.cte_scan_names.is_empty() {
                let left: ColumnSet = union_all
                    .left_outputs
                    .iter()
                    .map(|(index, _)| *index)
                    .collect();
                let right: ColumnSet = union_all
                    .right_outputs
                    .iter()
                    .map(|(index, _)| *index)
                    .collect();

                let offset_indices: Vec<usize> = (0..union_all.left_outputs.len()).collect();
                (offset_indices, left, right)
            } else {
                let offset_indices: Vec<usize> = (0..union_all.left_outputs.len())
                    .filter(|index| required.contains(&union_all.output_indexes[*index]))
                    .collect();

                if offset_indices.is_empty() {
                    (
                        vec![0],
                        ColumnSet::from([union_all.left_outputs[0].0]),
                        ColumnSet::from([union_all.right_outputs[0].0]),
                    )
                } else {
                    offset_indices.iter().fold(
                        (vec![], ColumnSet::default(), ColumnSet::default()),
                        |(mut offset_indices, mut left, mut right), &index| {
                            left.insert(union_all.left_outputs[index].0);
                            right.insert(union_all.right_outputs[index].0);
                            offset_indices.push(index);
                            (offset_indices, left, right)
                        },
                    )
                }
            };

        // 2. Build physical plan.
        let left_plan = self.build(s_expr.child(0)?, left_required.clone()).await?;
        let right_plan = self.build(s_expr.child(1)?, right_required.clone()).await?;

        let left_schema = left_plan.output_schema()?;
        let right_schema = right_plan.output_schema()?;

        let left_outputs = process_outputs(&union_all.left_outputs, &offset_indices, &left_schema)?;
        let right_outputs =
            process_outputs(&union_all.right_outputs, &offset_indices, &right_schema)?;

        let mut fields = Vec::with_capacity(offset_indices.len());
        for offset in offset_indices {
            let index = union_all.output_indexes[offset];
            let data_type = if let Some(scalar_expr) = &union_all.left_outputs[offset].1 {
                let expr = scalar_expr
                    .type_check(left_schema.as_ref())?
                    .project_column_ref(|idx| left_schema.index_of(&idx.to_string()))?;
                expr.data_type().clone()
            } else {
                let col_index = union_all.left_outputs[offset].0;
                left_schema
                    .field_with_name(&col_index.to_string())?
                    .data_type()
                    .clone()
            };

            fields.push(DataField::new(&index.to_string(), data_type));
        }

        Ok(PhysicalPlan::new(UnionAll {
            meta: PhysicalPlanMeta::new("UnionAll"),
            left: left_plan,
            right: right_plan,
            left_outputs,
            right_outputs,
            schema: DataSchemaRefExt::create(fields),

            cte_scan_names: union_all.cte_scan_names.clone(),
            stat_info: Some(stat_info),
        }))
    }
}

fn process_outputs(
    outputs: &[(IndexType, Option<ScalarExpr>)],
    offset_indices: &[usize],
    schema: &DataSchema,
) -> Result<Vec<(IndexType, Option<RemoteExpr>)>> {
    let mut results = Vec::with_capacity(offset_indices.len());
    for index in offset_indices {
        let output = &outputs[*index];
        if let Some(scalar_expr) = &output.1 {
            let expr = scalar_expr
                .type_check(schema)?
                .project_column_ref(|idx| schema.index_of(&idx.to_string()))?;
            results.push((output.0, Some(expr.as_remote_expr())));
        } else {
            results.push((output.0, None));
        }
    }
    Ok(results)
}
