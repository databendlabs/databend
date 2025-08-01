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

use databend_common_catalog::table_context::TableContext;
use databend_common_exception::Result;
use databend_common_expression::RemoteExpr;
use databend_common_functions::BUILTIN_FUNCTIONS;
use databend_common_pipeline_core::processors::ProcessorPtr;
use databend_common_sql::evaluator::BlockOperator;
use databend_common_sql::evaluator::CompoundBlockOperator;
use databend_common_sql::executor::physical_plans::UnionAll;
use databend_common_sql::executor::PhysicalPlan;
use databend_common_sql::IndexType;

use crate::pipelines::PipelineBuilder;

impl PipelineBuilder {
    pub fn build_union_all(&mut self, union_all: &UnionAll) -> Result<()> {
        if !union_all.cte_scan_names.is_empty() {
            return self.build_recursive_cte_source(union_all);
        }

        self.build_union_all_children(&union_all.left, &union_all.left_outputs)?;
        let left_sinks = self.main_pipeline.take_sinks();
        self.build_union_all_children(&union_all.right, &union_all.right_outputs)?;
        let right_sinks = self.main_pipeline.take_sinks();

        let outputs = std::cmp::max(left_sinks.len(), right_sinks.len());
        let sequence_groups = vec![(left_sinks.len(), false), (right_sinks.len(), false)];

        self.main_pipeline.extend_sinks(left_sinks);
        self.main_pipeline.extend_sinks(right_sinks);

        let enable_parallel_union_all = self.ctx.get_settings().get_enable_parallel_union_all()?
            || self.ctx.get_settings().get_grouping_sets_to_union()?;
        match enable_parallel_union_all {
            true => self.main_pipeline.resize(outputs, false),
            false => self.main_pipeline.sequence_group(sequence_groups, outputs),
        }
    }

    fn build_union_all_children(
        &mut self,
        input: &PhysicalPlan,
        projection: &[(IndexType, Option<RemoteExpr>)],
    ) -> Result<()> {
        self.build_pipeline(input)?;
        let output_schema = input.output_schema()?;

        let mut expr_offset = output_schema.num_fields();
        let mut new_projection = Vec::with_capacity(projection.len());
        let mut exprs = Vec::with_capacity(projection.len());
        for (idx, expr) in projection {
            let Some(expr) = expr else {
                new_projection.push(output_schema.index_of(&idx.to_string())?);
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

        self.main_pipeline.add_transform(|input, output| {
            Ok(ProcessorPtr::create(CompoundBlockOperator::create(
                input,
                output,
                output_schema.num_fields(),
                self.func_ctx.clone(),
                operators.clone(),
            )))
        })
    }
}
