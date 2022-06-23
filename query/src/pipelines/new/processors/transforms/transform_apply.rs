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

use std::collections::HashMap;
use std::sync::Arc;

use common_datablocks::DataBlock;
use common_datavalues::Column;
use common_datavalues::ConstColumn;
use common_datavalues::DataValue;
use common_exception::ErrorCode;
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
use crate::sql::exec::format_field_name;
use crate::sql::exec::PipelineBuilder;
use crate::sql::optimizer::ColumnSet;
use crate::sql::optimizer::SExpr;
use crate::sql::plans::AggregateFunction;
use crate::sql::plans::AndExpr;
use crate::sql::plans::CastExpr;
use crate::sql::plans::ComparisonExpr;
use crate::sql::plans::ConstantExpr;
use crate::sql::plans::FunctionCall;
use crate::sql::plans::OrExpr;
use crate::sql::plans::RelOperator;
use crate::sql::plans::Scalar;
use crate::sql::IndexType;
use crate::sql::MetadataRef;

/// Rewrite a SExpr with correlated column reference replaced by
/// `ConstantExpr` in current context(i.e. applying row).
struct OuterRefRewriter {
    outer_columns: HashMap<IndexType, DataValue>,
}

impl OuterRefRewriter {
    pub fn new(outer_columns: HashMap<IndexType, DataValue>) -> Self {
        Self { outer_columns }
    }

    pub fn rewrite(&mut self, s_expr: &SExpr) -> Result<SExpr> {
        match s_expr.plan().clone() {
            RelOperator::EvalScalar(mut plan) => {
                let input = self.rewrite(s_expr.child(0)?)?;

                for item in plan.items.iter_mut() {
                    let scalar = self.rewrite_scalar(&item.scalar)?;
                    item.scalar = scalar;
                }

                Ok(SExpr::create_unary(plan.into(), input))
            }
            RelOperator::Filter(mut plan) => {
                let input = self.rewrite(s_expr.child(0)?)?;

                for scalar in plan.predicates.iter_mut() {
                    *scalar = self.rewrite_scalar(scalar)?;
                }

                Ok(SExpr::create_unary(plan.into(), input))
            }
            RelOperator::Aggregate(mut plan) => {
                let input = self.rewrite(s_expr.child(0)?)?;

                for item in plan.group_items.iter_mut() {
                    let scalar = self.rewrite_scalar(&item.scalar)?;
                    item.scalar = scalar;
                }

                for item in plan.aggregate_functions.iter_mut() {
                    let scalar = self.rewrite_scalar(&item.scalar)?;
                    item.scalar = scalar;
                }

                Ok(SExpr::create_unary(plan.into(), input))
            }

            RelOperator::PhysicalHashJoin(mut plan) => {
                let probe_side = self.rewrite(s_expr.child(0)?)?;
                let build_side = self.rewrite(s_expr.child(1)?)?;
                for scalar in plan.build_keys.iter_mut() {
                    *scalar = self.rewrite_scalar(scalar)?;
                }
                for scalar in plan.probe_keys.iter_mut() {
                    *scalar = self.rewrite_scalar(scalar)?;
                }

                Ok(SExpr::create_binary(plan.into(), build_side, probe_side))
            }

            RelOperator::Max1Row(_)
            | RelOperator::Project(_)
            | RelOperator::Limit(_)
            | RelOperator::Sort(_) => Ok(SExpr::create_unary(
                s_expr.plan().clone(),
                self.rewrite(s_expr.child(0)?)?,
            )),

            RelOperator::PhysicalScan(_) => Ok(s_expr.clone()),

            RelOperator::CrossApply(plan) => {
                let left = self.rewrite(s_expr.child(0)?)?;
                let right = self.rewrite(s_expr.child(1)?)?;
                Ok(SExpr::create_binary(plan.into(), left, right))
            }

            RelOperator::LogicalGet(_)
            | RelOperator::LogicalInnerJoin(_)
            | RelOperator::Pattern(_) => Err(ErrorCode::LogicalError("Invalid plan type")),
        }
    }

    fn rewrite_scalar(&mut self, scalar: &Scalar) -> Result<Scalar> {
        match scalar {
            Scalar::BoundColumnRef(column_ref) => {
                if let Some(value) = self.outer_columns.get(&column_ref.column.index) {
                    // Replace outer column with given DataValue
                    Ok(ConstantExpr {
                        value: value.clone(),
                        data_type: column_ref.column.data_type.clone(),
                    }
                    .into())
                } else {
                    Ok(scalar.clone())
                }
            }

            Scalar::ConstantExpr(_) => Ok(scalar.clone()),

            Scalar::AndExpr(expr) => Ok(AndExpr {
                left: Box::new(self.rewrite_scalar(&expr.left)?),
                right: Box::new(self.rewrite_scalar(&expr.right)?),
                return_type: expr.return_type.clone(),
            }
            .into()),

            Scalar::OrExpr(expr) => Ok(OrExpr {
                left: Box::new(self.rewrite_scalar(&expr.left)?),
                right: Box::new(self.rewrite_scalar(&expr.right)?),
                return_type: expr.return_type.clone(),
            }
            .into()),

            Scalar::ComparisonExpr(expr) => Ok(ComparisonExpr {
                op: expr.op.clone(),
                left: Box::new(self.rewrite_scalar(&expr.left)?),
                right: Box::new(self.rewrite_scalar(&expr.right)?),
                return_type: expr.return_type.clone(),
            }
            .into()),

            Scalar::AggregateFunction(agg) => {
                let mut args = Vec::with_capacity(agg.args.len());
                for arg in agg.args.iter() {
                    let scalar = self.rewrite_scalar(arg)?;
                    args.push(scalar);
                }

                let expr: Scalar = AggregateFunction {
                    display_name: agg.display_name.clone(),
                    args,
                    func_name: agg.func_name.clone(),
                    distinct: agg.distinct,
                    params: agg.params.clone(),
                    return_type: agg.return_type.clone(),
                }
                .into();

                Ok(expr)
            }

            Scalar::FunctionCall(func) => {
                let mut args = Vec::with_capacity(func.arguments.len());
                for arg in func.arguments.iter() {
                    let scalar = self.rewrite_scalar(arg)?;
                    args.push(scalar);
                }

                let expr: Scalar = FunctionCall {
                    arguments: args,
                    func_name: func.func_name.clone(),
                    return_type: func.return_type.clone(),
                    arg_types: func.arg_types.clone(),
                }
                .into();

                Ok(expr)
            }

            Scalar::CastExpr(cast) => Ok(CastExpr {
                argument: Box::new(self.rewrite_scalar(&cast.argument)?),
                from_type: cast.from_type.clone(),
                target_type: cast.target_type.clone(),
            }
            .into()),

            Scalar::SubqueryExpr(_) => Err(ErrorCode::LogicalError(
                "Physical plan shouldn't contain subquery expression",
            )),
        }
    }
}

pub struct TransformApply {
    ctx: Arc<QueryContext>,
    metadata: MetadataRef,

    outer_columns: ColumnSet,

    subquery: SExpr,
}

impl TransformApply {
    pub fn create(
        input: Arc<InputPort>,
        output: Arc<OutputPort>,
        ctx: Arc<QueryContext>,
        metadata: MetadataRef,
        outer_columns: ColumnSet,
        subquery: SExpr,
    ) -> ProcessorPtr {
        Transformer::<Self>::create(input, output, Self {
            ctx,
            metadata,
            outer_columns,
            subquery,
        })
    }

    fn extract_outer_columns(
        &self,
        data_block: &DataBlock,
        row_index: usize,
    ) -> Result<HashMap<IndexType, DataValue>> {
        let mut result = HashMap::new();
        for index in self.outer_columns.iter() {
            let display_name = self.metadata.read().column(*index).name.clone();
            let name = format_field_name(display_name.as_str(), *index);
            let column = data_block.try_column_by_name(name.as_str())?;
            let data_value = column.get(row_index);
            result.insert(*index, data_value);
        }

        Ok(result)
    }

    // TODO(leiysky): this function may launch recursive tasks, which is very inefficient.
    // Even we would eliminate `Apply`s with decorrelation, still need to use a better implementation.
    fn execute_subquery(
        &self,
        outer_columns: HashMap<IndexType, DataValue>,
    ) -> Result<Vec<DataBlock>> {
        let mut rewriter = OuterRefRewriter::new(outer_columns);
        let s_expr = rewriter.rewrite(&self.subquery)?;

        let mut pipeline = NewPipeline::create();
        let mut pb = PipelineBuilder::new(
            self.ctx.clone(),
            vec![],
            self.metadata.clone(),
            s_expr.clone(),
        );
        pb.build_pipeline(
            QueryContext::create_from(self.ctx.clone()),
            &s_expr,
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
        let mut replicated_input_columns = Vec::with_capacity(input_block.schema().num_fields());
        let row = DataBlock::block_take_by_indices(input_block, &[input_block_row_index as u32])?;
        let output_num_rows = join_block.num_rows();
        for column in row.columns() {
            let replicated_column = ConstColumn::new(column.clone(), output_num_rows);
            replicated_input_columns.push(replicated_column.arc());
        }
        let mut result_block = join_block;
        for (field, column) in input_block
            .schema()
            .fields()
            .iter()
            .zip(replicated_input_columns.into_iter())
        {
            result_block = result_block.add_column(column, field.clone())?;
        }

        Ok(result_block)
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
