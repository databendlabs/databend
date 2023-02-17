// Copyright 2021 Datafuse Labs.
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

use common_exception::Result;
use common_expression::DataSchemaRef;
use common_meta_store::MetaStore;
use common_pipeline_core::pipe::Pipe;
use common_pipeline_core::pipe::PipeItem;
use common_pipeline_core::processors::port::InputPort;
use common_pipeline_core::processors::port::OutputPort;
use common_sql::MetadataRef;
use common_storages_result_cache::ResultCacheReader;
use common_storages_result_cache::WriteResultCacheSink;
use common_users::UserApiProvider;

use crate::interpreters::Interpreter;
use crate::pipelines::processors::TransformDummy;
use crate::pipelines::PipelineBuildResult;
use crate::schedulers::build_query_pipeline;
use crate::sessions::QueryContext;
use crate::sessions::TableContext;
use crate::sql::executor::PhysicalPlanBuilder;
use crate::sql::optimizer::SExpr;
use crate::sql::BindContext;

/// Interpret SQL query with ne&w SQL planner
pub struct SelectInterpreterV2 {
    ctx: Arc<QueryContext>,
    s_expr: SExpr,
    bind_context: BindContext,
    metadata: MetadataRef,
    ignore_result: bool,
}

impl SelectInterpreterV2 {
    pub fn try_create(
        ctx: Arc<QueryContext>,
        bind_context: BindContext,
        s_expr: SExpr,
        metadata: MetadataRef,
        ignore_result: bool,
    ) -> Result<Self> {
        Ok(SelectInterpreterV2 {
            ctx,
            s_expr,
            bind_context,
            metadata,
            ignore_result,
        })
    }

    pub async fn build_pipeline(&self) -> Result<PipelineBuildResult> {
        let mut builder = PhysicalPlanBuilder::new(self.metadata.clone(), self.ctx.clone());
        let physical_plan = builder.build(&self.s_expr).await?;
        build_query_pipeline(
            &self.ctx,
            &self.bind_context.columns,
            &physical_plan,
            self.ignore_result,
            false,
        )
        .await
    }

    fn append_write_cache(
        &self,
        build_res: &mut PipelineBuildResult,
        kv_store: Arc<MetaStore>,
    ) -> Result<()> {
        // 1. Duplicate the pipes.
        build_res.main_pipeline.duplicate(false)?;
        // 2. Reorder the pipes.
        let output_len = build_res.main_pipeline.output_len();
        debug_assert!(output_len % 2 == 0);
        let mut rule = vec![0; output_len];
        for (i, r) in rule.iter_mut().enumerate().take(output_len) {
            *r = if i % 2 == 0 {
                i / 2
            } else {
                output_len / 2 + i / 2
            };
        }
        build_res.main_pipeline.reorder_inputs(rule);

        // `output_len` / 2 for `TransformDummy`; 1 for `WriteResultCacheSink`.
        let mut items = Vec::with_capacity(output_len / 2 + 1);
        // 3. Add dummy transforms to the front pipes.
        for _ in 0..output_len / 2 {
            let input = InputPort::create();
            let output = OutputPort::create();
            items.push(PipeItem::create(
                TransformDummy::create(input.clone(), output.clone()),
                vec![input],
                vec![output],
            ));
        }

        // 4. Add WriteResultCacheSink (AsyncMpscSinker) to the back pipes.
        let mut sink_inputs = Vec::with_capacity(output_len / 2);
        for _ in 0..output_len / 2 {
            sink_inputs.push(InputPort::create());
        }
        items.push(PipeItem::create(
            WriteResultCacheSink::try_create(self.ctx.clone(), sink_inputs.clone(), kv_store)?,
            sink_inputs,
            vec![],
        ));
        build_res
            .main_pipeline
            .add_pipe(Pipe::create(output_len, output_len / 2 + 1, items));

        Ok(())
    }
}

#[async_trait::async_trait]
impl Interpreter for SelectInterpreterV2 {
    fn name(&self) -> &str {
        "SelectInterpreterV2"
    }

    fn schema(&self) -> DataSchemaRef {
        self.bind_context.output_schema()
    }

    /// This method will create a new pipeline
    /// The QueryPipelineBuilder will use the optimized plan to generate a Pipeline
    #[tracing::instrument(level = "debug", name = "select_interpreter_v2_execute", skip(self), fields(ctx.id = self.ctx.get_id().as_str()))]
    async fn execute2(&self) -> Result<PipelineBuildResult> {
        if self.ctx.get_settings().get_enable_query_result_cache()? {
            // 0. Need to build pipeline first to get the partitions.
            let mut build_res = self.build_pipeline().await?;

            // 1. Try to get result from cache.
            let kv_store = UserApiProvider::instance().get_meta_store_client();
            let cache_reader = ResultCacheReader::create(self.ctx.clone(), kv_store.clone());
            if let Some(blocks) = cache_reader.try_read_cached_result().await? {
                // 2. If found, return the result directly.
                return PipelineBuildResult::from_blocks(blocks);
            }
            // 3. If not found result in cache, build the pipeline and add a transform to write the result to cache.
            self.append_write_cache(&mut build_res, kv_store)?;
            Ok(build_res)
        } else {
            Ok(self.build_pipeline().await?)
        }
    }
}
