// Copyright 2022 Datafuse Labs.
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

use std::collections::BTreeSet;
use std::collections::HashMap;
use std::sync::Arc;

use common_datablocks::DataBlock;
use common_datavalues::Column;
use common_datavalues::ConstColumn;
use common_datavalues::DataSchemaRefExt;
use common_datavalues::DataValue;
use common_exception::Result;

use crate::pipelines::new::executor::PipelineExecutor;
use crate::pipelines::new::executor::PipelinePullingExecutor;
use crate::pipelines::new::processors::port::InputPort;
use crate::pipelines::new::processors::port::OutputPort;
use crate::pipelines::new::processors::processor::ProcessorPtr;
use crate::pipelines::new::processors::transforms::transform::Transform;
use crate::pipelines::new::processors::transforms::transform::Transformer;
use crate::pipelines::new::NewPipeline;
use crate::sessions::QueryContext;
use crate::sql::exec::ColumnID;
use crate::sql::exec::PhysicalPlan;
use crate::sql::exec::PhysicalScalar;
use crate::sql::exec::PipelineBuilder;

/// Rewrite a SExpr with correlated column reference replaced by
/// `ConstantExpr` in current context(i.e. applying row).
struct OuterRefRewriter {
    outer_columns: HashMap<ColumnID, DataValue>,
}

impl OuterRefRewriter {
    pub fn new(outer_columns: HashMap<ColumnID, DataValue>) -> Self {
        Self { outer_columns }
    }

    pub fn rewrite_physical_plan(&mut self, plan: &PhysicalPlan) -> Result<PhysicalPlan> {
        match plan {
            PhysicalPlan::Max1Row { input } => Ok(PhysicalPlan::Max1Row {
                input: Box::new(self.rewrite_physical_plan(input)?),
            }),
            PhysicalPlan::HashJoin {
                build,
                probe,
                build_keys,
                probe_keys,
                other_conditions,
                join_type,
            } => Ok(PhysicalPlan::HashJoin {
                build: Box::new(self.rewrite_physical_plan(build)?),
                probe: Box::new(self.rewrite_physical_plan(probe)?),

                build_keys: build_keys.clone(),
                probe_keys: probe_keys.clone(),
                other_conditions: other_conditions.clone(),
                join_type: join_type.clone(),
            }),
            PhysicalPlan::Limit {
                input,
                limit,
                offset,
            } => Ok(PhysicalPlan::Limit {
                input: Box::new(self.rewrite_physical_plan(input)?),
                limit: *limit,
                offset: *offset,
            }),
            PhysicalPlan::Sort { input, order_by } => Ok(PhysicalPlan::Sort {
                input: Box::new(self.rewrite_physical_plan(input)?),

                order_by: order_by.clone(),
            }),
            PhysicalPlan::Project { input, projections } => Ok(PhysicalPlan::Project {
                input: Box::new(self.rewrite_physical_plan(input)?),
                projections: projections.clone(),
            }),
            PhysicalPlan::TableScan { .. } => Ok(plan.clone()),
            PhysicalPlan::Filter { input, predicates } => Ok(PhysicalPlan::Filter {
                input: Box::new(self.rewrite_physical_plan(input)?),
                predicates: predicates
                    .iter()
                    .map(|pred| self.rewrite_physical_scalar(pred))
                    .collect::<Result<_>>()?,
            }),
            PhysicalPlan::EvalScalar { input, scalars } => Ok(PhysicalPlan::EvalScalar {
                input: Box::new(self.rewrite_physical_plan(input)?),
                scalars: scalars
                    .iter()
                    .map(|(scalar, id)| Ok((self.rewrite_physical_scalar(scalar)?, id.clone())))
                    .collect::<Result<_>>()?,
            }),
            PhysicalPlan::AggregatePartial {
                input,
                group_by,
                agg_funcs,
            } => Ok(PhysicalPlan::AggregatePartial {
                input: Box::new(self.rewrite_physical_plan(input)?),
                group_by: group_by.clone(),
                agg_funcs: agg_funcs.clone(),
            }),
            PhysicalPlan::AggregateFinal {
                input,
                group_by,
                agg_funcs,
                before_group_by_schema,
            } => Ok(PhysicalPlan::AggregateFinal {
                input: Box::new(self.rewrite_physical_plan(input)?),
                group_by: group_by.clone(),
                agg_funcs: agg_funcs.clone(),
                before_group_by_schema: before_group_by_schema.clone(),
            }),
            PhysicalPlan::CrossApply {
                input,
                subquery,
                correlated_columns,
            } => Ok(PhysicalPlan::CrossApply {
                input: Box::new(self.rewrite_physical_plan(input)?),
                subquery: Box::new(self.rewrite_physical_plan(subquery)?),
                correlated_columns: correlated_columns.clone(),
            }),
        }
    }

    fn rewrite_physical_scalar(&mut self, scalar: &PhysicalScalar) -> Result<PhysicalScalar> {
        match scalar {
            PhysicalScalar::Variable {
                column_id,
                data_type,
            } => {
                if let Some(value) = self.outer_columns.get(column_id) {
                    Ok(PhysicalScalar::Constant {
                        value: value.clone(),
                        data_type: data_type.clone(),
                    })
                } else {
                    Ok(scalar.clone())
                }
            }
            PhysicalScalar::Constant { .. } => Ok(scalar.clone()),
            PhysicalScalar::Function {
                name,
                args,
                return_type,
            } => {
                let args = args
                    .iter()
                    .map(|(arg, data_type)| {
                        Ok((self.rewrite_physical_scalar(arg)?, data_type.clone()))
                    })
                    .collect::<Result<Vec<_>>>()?;
                Ok(PhysicalScalar::Function {
                    name: name.clone(),
                    args,
                    return_type: return_type.clone(),
                })
            }
            PhysicalScalar::Cast { input, target } => Ok(PhysicalScalar::Cast {
                input: Box::new(self.rewrite_physical_scalar(input)?),
                target: target.clone(),
            }),
        }
    }
}

pub struct TransformApply {
    ctx: Arc<QueryContext>,
    outer_columns: BTreeSet<ColumnID>,

    subquery: PhysicalPlan,
}

impl TransformApply {
    pub fn create(
        input: Arc<InputPort>,
        output: Arc<OutputPort>,
        ctx: Arc<QueryContext>,
        outer_columns: BTreeSet<ColumnID>,
        subquery: PhysicalPlan,
    ) -> ProcessorPtr {
        Transformer::<Self>::create(input, output, Self {
            ctx,
            outer_columns,
            subquery,
        })
    }

    fn extract_outer_columns(
        &self,
        data_block: &DataBlock,
        row_index: usize,
    ) -> Result<HashMap<ColumnID, DataValue>> {
        let mut result = HashMap::new();
        for id in self.outer_columns.iter() {
            let column = data_block.try_column_by_name(id.as_str())?;
            let data_value = column.get(row_index);
            result.insert(id.clone(), data_value);
        }

        Ok(result)
    }

    // TODO(leiysky): this function may launch recursive tasks, which is very inefficient.
    // Even we would eliminate `Apply`s with decorrelation, still need to use a better implementation.
    fn execute_subquery(
        &self,
        outer_columns: HashMap<ColumnID, DataValue>,
    ) -> Result<Vec<DataBlock>> {
        let mut rewriter = OuterRefRewriter::new(outer_columns);
        let physical_plan = rewriter.rewrite_physical_plan(&self.subquery)?;

        let mut pipeline = NewPipeline::create();
        let mut pb = PipelineBuilder::new();
        pb.build_pipeline(
            QueryContext::create_from(self.ctx.clone()),
            &physical_plan,
            &mut pipeline,
        )?;

        let mut children = pb.pipelines;

        // Set max threads
        let settings = self.ctx.get_settings();
        pipeline.set_max_threads(settings.get_max_threads()? as usize);
        for pipeline in children.iter_mut() {
            pipeline.set_max_threads(settings.get_max_threads()? as usize);
        }

        let runtime = self.ctx.get_storage_runtime();
        let query_need_abort = self.ctx.query_need_abort();
        // Spawn sub-pipelines
        for pipeline in children {
            let executor =
                PipelineExecutor::create(runtime.clone(), query_need_abort.clone(), pipeline)?;
            executor.execute()?;
        }

        let mut executor =
            PipelinePullingExecutor::try_create(runtime, query_need_abort, pipeline)?;
        executor.start();
        let mut results = vec![];
        while let Some(result) = executor.pull_data()? {
            results.push(result);
        }

        Ok(results)
    }

    fn join_block(
        &self,
        input_block: &DataBlock,
        input_block_row_index: usize,
        join_block: DataBlock,
    ) -> Result<DataBlock> {
        let mut columns = Vec::with_capacity(input_block.schema().num_fields());
        let row = DataBlock::block_take_by_indices(input_block, &[input_block_row_index as u32])?;
        let output_num_rows = join_block.num_rows();
        let output_schema = DataSchemaRefExt::create(
            input_block
                .schema()
                .fields()
                .iter()
                .chain(join_block.schema().fields().iter())
                .cloned()
                .collect::<Vec<_>>(),
        );
        for column in row.columns() {
            let replicated_column = ConstColumn::new(column.clone(), output_num_rows);
            columns.push(replicated_column.arc());
        }
        for column in join_block.columns() {
            columns.push(column.clone());
        }

        Ok(DataBlock::create(output_schema, columns))
    }
}

impl Transform for TransformApply {
    const NAME: &'static str = "TransformCrossApply";

    fn transform(&mut self, data: DataBlock) -> Result<DataBlock> {
        let mut result_blocks = vec![];
        for row_index in 0..data.num_rows() {
            let outer_columns = self.extract_outer_columns(&data, row_index)?;
            let mut subquery_results = self.execute_subquery(outer_columns)?;
            subquery_results = subquery_results
                .into_iter()
                .map(|block| self.join_block(&data, row_index, block))
                .collect::<Result<Vec<_>>>()?;
            result_blocks.append(&mut subquery_results);
        }

        DataBlock::concat_blocks(&result_blocks)
    }
}
