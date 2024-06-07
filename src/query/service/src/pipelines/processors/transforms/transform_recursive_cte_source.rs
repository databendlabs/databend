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
use databend_common_expression::DataBlock;
use databend_common_expression::Expr;
use databend_common_functions::BUILTIN_FUNCTIONS;
use databend_common_pipeline_core::processors::OutputPort;
use databend_common_pipeline_core::processors::ProcessorPtr;
use databend_common_pipeline_sources::AsyncSource;
use databend_common_pipeline_sources::AsyncSourcer;
use databend_common_sql::executor::physical_plans::UnionAll;
use databend_common_sql::IndexType;
use futures_util::TryStreamExt;

use crate::pipelines::executor::ExecutorSettings;
use crate::pipelines::executor::PipelinePullingExecutor;
use crate::pipelines::processors::transforms::transform_merge_block::project_block;
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
        if self.ctx.get_settings().get_max_cte_recursive_depth()? < self.recursive_step {
            return Err(ErrorCode::Internal("Recursive depth is reached"));
        }
        let plan = if self.recursive_step == 0 {
            self.union_plan.left.clone()
        } else {
            self.union_plan.right.clone()
        };
        self.recursive_step += 1;
        self.ctx.clear_runtime_filter();
        let build_res = build_query_pipeline_without_render_result_set(&self.ctx, &plan).await?;
        let settings = ExecutorSettings::try_create(self.ctx.clone())?;
        let pulling_executor = PipelinePullingExecutor::from_pipelines(build_res, settings)?;
        self.ctx.set_executor(pulling_executor.get_inner())?;
        self.executor = Some(pulling_executor);
        Ok(())
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
            let func_ctx = self.ctx.get_function_context()?;
            data = project_block(
                &func_ctx,
                data,
                &self.union_plan.left.output_schema()?,
                &self.union_plan.right.output_schema()?,
                &self.left_outputs,
                &self.right_outputs,
                self.recursive_step == 1,
            )?;
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
