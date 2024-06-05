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

use std::sync::Arc;

use databend_common_catalog::table_context::TableContext;
use databend_common_exception::ErrorCode;
use databend_common_exception::Result;
use databend_common_expression::{BlockEntry, Expr};
use databend_common_expression::ColumnBuilder;
use databend_common_expression::DataBlock;
use databend_common_expression::Evaluator;
use databend_common_expression::Value;
use databend_common_functions::BUILTIN_FUNCTIONS;
use databend_common_pipeline_core::processors::OutputPort;
use databend_common_pipeline_core::processors::ProcessorPtr;
use databend_common_pipeline_sources::AsyncSource;
use databend_common_pipeline_sources::AsyncSourcer;
use databend_common_sql::executor::physical_plans::UnionAll;
use futures_util::TryStreamExt;
use databend_common_sql::IndexType;

use crate::pipelines::executor::ExecutorSettings;
use crate::pipelines::executor::PipelinePullingExecutor;
use crate::schedulers::build_query_pipeline_without_render_result_set;
use crate::sessions::QueryContext;
use crate::stream::PullingExecutorStream;

// The whole recursive cte as source.
pub struct TransformRecursiveCteSource {
    ctx: Arc<QueryContext>,
    union_plan: UnionAll,
    left_outputs: Vec<(IndexType, Option<Expr>)>,
    right_outputs: Vec<(IndexType, Option<Expr>)>,

    executor: Option<PipelinePullingExecutor>,
    recursive_step: usize,
    cte_name: String,
}

impl TransformRecursiveCteSource {
    pub fn try_create(
        ctx: Arc<QueryContext>,
        output_port: Arc<OutputPort>,
        union_plan: UnionAll,
    ) -> Result<ProcessorPtr> {
        let cte_name = union_plan.cte_name.as_ref().unwrap().clone();
        let left_outputs = union_plan
            .left_outputs
            .iter()
            .map(|(idx, remote_expr)| {
                if let Some(remote_expr) = remote_expr {
                    (*idx, Some(remote_expr.as_expr(&BUILTIN_FUNCTIONS)))
                } else {
                    (*idx, None)
                }
            })
            .collect::<Vec<_>>();
        let right_outputs = union_plan
            .right_outputs
            .iter()
            .map(|(idx, remote_expr)| {
                if let Some(remote_expr) = remote_expr {
                    (*idx, Some(remote_expr.as_expr(&BUILTIN_FUNCTIONS)))
                } else {
                    (*idx, None)
                }
            })
            .collect::<Vec<_>>();
        AsyncSourcer::create(ctx.clone(), output_port, TransformRecursiveCteSource {
            ctx,
            union_plan,
            left_outputs,
            right_outputs,
            executor: None,
            recursive_step: 0,
            cte_name,
        })
    }

    async fn build_union(&mut self) -> Result<()> {
        if self
            .ctx
            .get_settings()
            .get_max_cte_recursive_depth()
            .unwrap()
            < self.recursive_step
        {
            return Err(ErrorCode::Internal("Recursive depth is reached"));
        }
        let plan = if self.recursive_step == 0 {
            self.union_plan.left.clone()
        } else {
            self.union_plan.right.clone()
        };
        self.recursive_step += 1;
        let build_res = build_query_pipeline_without_render_result_set(&self.ctx, &plan).await?;
        let settings = ExecutorSettings::try_create(self.ctx.clone())?;
        let pulling_executor = PipelinePullingExecutor::from_pipelines(build_res, settings)?;
        self.ctx.set_executor(pulling_executor.get_inner())?;
        self.executor = Some(pulling_executor);
        Ok(())
    }

    fn project_block(&self, block: DataBlock, is_left: bool) -> Result<DataBlock> {
        let num_rows = block.num_rows();
        let left_schema = self.union_plan.left.output_schema()?;
        let right_schema = self.union_plan.right.output_schema()?;
        dbg!(&self.left_outputs);
        dbg!(&self.right_outputs);
        dbg!(&left_schema);
        dbg!(&right_schema);
        let func_ctx = self.ctx.get_function_context()?;
        let mut evaluator = Evaluator::new(&block, &func_ctx, &BUILTIN_FUNCTIONS);
        let columns = self
            .left_outputs
            .iter()
            .zip(self.right_outputs.iter())
            .map(|(left, right)| {
                if is_left {
                    if let Some(expr) = &left.1 {
                        let column =
                            BlockEntry::new(expr.data_type().clone(), evaluator.run(expr)?);
                        Ok(column)
                    } else {
                        Ok(block.get_by_offset(left_schema.index_of(&left.0.to_string())?).clone())
                    }
                } else {
                    if let Some(expr) = &right.1 {
                        let column =
                            BlockEntry::new(expr.data_type().clone(), evaluator.run(expr)?);
                        Ok(column)
                    } else {
                        if left.1.is_some() {
                            Ok(block.get_by_offset(right_schema.index_of(&right.0.to_string())?).clone())
                        } else {
                            self.check_type(&left.0.to_string(), &right.0.to_string(), &block)
                        }
                    }
                }
            })
            .collect::<Result<Vec<_>>>()?;
        Ok(DataBlock::new(columns, num_rows))
    }

    fn check_type(
        &self,
        left_name: &str,
        right_name: &str,
        block: &DataBlock,
    ) -> Result<BlockEntry> {
        let left_schema = self.union_plan.left.output_schema()?;
        let right_schema = self.union_plan.right.output_schema()?;
        let left_field = left_schema.field_with_name(left_name)?;
        let left_data_type = left_field.data_type();

        let right_field = right_schema.field_with_name(right_name)?;
        let right_data_type = right_field.data_type();

        let index = right_schema.index_of(right_name)?;

        if left_data_type == right_data_type {
            return Ok(block.get_by_offset(index).clone());
        }

        if left_data_type.remove_nullable() == right_data_type.remove_nullable() {
            let origin_column = block.get_by_offset(index).clone();
            let mut builder = ColumnBuilder::with_capacity(left_data_type, block.num_rows());
            let value = origin_column.value.as_ref();
            for idx in 0..block.num_rows() {
                let scalar = value.index(idx).unwrap();
                builder.push(scalar);
            }
            let col = builder.build();
            Ok(BlockEntry::new(left_data_type.clone(), Value::Column(col)))
        } else {
            Err(ErrorCode::IllegalDataType(
                "The data type on both sides of the union does not match",
            ))
        }
    }
}

#[async_trait::async_trait]
impl AsyncSource for TransformRecursiveCteSource {
    const NAME: &'static str = "TransformRecursiveCteSource";

    #[async_trait::unboxed_simple]
    #[async_backtrace::framed]
    async fn generate(&mut self) -> Result<Option<DataBlock>> {
        let mut res = None;
        if self.executor.is_none() {
            self.build_union().await?;
        }
        // Todo: improve here to make it streaming.
        let data = PullingExecutorStream::create(self.executor.take().unwrap())?
            .try_collect::<Vec<DataBlock>>()
            .await?;
        if data.is_empty() {
            self.ctx.update_recursive_cte_scan(&self.cte_name, vec![])?;
            return Ok(None);
        }
        let mut data = DataBlock::concat(&data)?;

        let row_size = data.num_rows();
        if row_size > 0 {
            dbg!(&data);
            data = self.project_block(data, self.recursive_step == 1)?;
            dbg!(&data);
            // Prepare the data of next round recursive.
            self.ctx
                .update_recursive_cte_scan(&self.cte_name, vec![data.clone()])?;
            res = Some(data);
        } else {
            self.ctx.update_recursive_cte_scan(&self.cte_name, vec![])?;
        }
        Ok(res)
    }
}
