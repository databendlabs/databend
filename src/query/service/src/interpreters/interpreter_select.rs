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

use common_catalog::table::Table;
use common_exception::ErrorCode;
use common_exception::Result;
use common_expression::infer_table_schema;
use common_expression::DataSchemaRef;
use common_expression::TableSchemaRef;
use common_meta_store::MetaStore;
use common_pipeline_core::pipe::Pipe;
use common_pipeline_core::pipe::PipeItem;
use common_pipeline_core::processors::port::InputPort;
use common_pipeline_core::processors::port::OutputPort;
use common_pipeline_core::Pipeline;
use common_pipeline_transforms::processors::transforms::TransformDummy;
use common_sql::executor::PhysicalPlan;
use common_sql::parse_result_scan_args;
use common_sql::MetadataRef;
use common_storages_result_cache::gen_result_cache_key;
use common_storages_result_cache::ResultCacheReader;
use common_storages_result_cache::WriteResultCacheSink;
use common_users::UserApiProvider;

use crate::interpreters::Interpreter;
use crate::pipelines::PipelineBuildResult;
use crate::schedulers::build_query_pipeline;
use crate::sessions::QueryContext;
use crate::sessions::TableContext;
use crate::sql::executor::PhysicalPlanBuilder;
use crate::sql::optimizer::SExpr;
use crate::sql::BindContext;

/// Interpret SQL query with ne&w SQL planner
pub struct SelectInterpreter {
    ctx: Arc<QueryContext>,
    s_expr: SExpr,
    bind_context: BindContext,
    metadata: MetadataRef,
    formatted_ast: Option<String>,
    ignore_result: bool,
}

impl SelectInterpreter {
    pub fn try_create(
        ctx: Arc<QueryContext>,
        bind_context: BindContext,
        s_expr: SExpr,
        metadata: MetadataRef,
        formatted_ast: Option<String>,
        ignore_result: bool,
    ) -> Result<Self> {
        Ok(SelectInterpreter {
            ctx,
            s_expr,
            bind_context,
            metadata,
            formatted_ast,
            ignore_result,
        })
    }

    #[inline]
    #[async_backtrace::framed]
    pub async fn build_physical_plan(&self) -> Result<PhysicalPlan> {
        let mut builder = PhysicalPlanBuilder::new(self.metadata.clone(), self.ctx.clone());
        builder.build(&self.s_expr).await
    }

    #[async_backtrace::framed]
    pub async fn build_pipeline(&self, physical_plan: PhysicalPlan) -> Result<PipelineBuildResult> {
        build_query_pipeline(
            &self.ctx,
            &self.bind_context.columns,
            &physical_plan,
            self.ignore_result,
            false,
        )
        .await
    }

    /// Add pipelines for writing query result cache.
    fn add_result_cache(
        &self,
        key: &str,
        schema: TableSchemaRef,
        pipeline: &mut Pipeline,
        kv_store: Arc<MetaStore>,
    ) -> Result<()> {
        //              ┌─────────┐ 1  ┌─────────┐ 1
        //              │         ├───►│         ├───►Dummy───►Downstream
        // Upstream────►│Duplicate│ 2  │         │ 3
        //              │         ├───►│         ├───►Dummy───►Downstream
        //              └─────────┘    │         │
        //                             │ Shuffle │
        //              ┌─────────┐ 3  │         │ 2  ┌─────────┐
        //              │         ├───►│         ├───►│  Write  │
        // Upstream────►│Duplicate│ 4  │         │ 4  │ Result  │
        //              │         ├───►│         ├───►│  Cache  │
        //              └─────────┘    └─────────┘    └─────────┘

        // 1. Duplicate the pipes.
        pipeline.duplicate(false)?;
        // 2. Reorder the pipes.
        let output_len = pipeline.output_len();
        debug_assert!(output_len % 2 == 0);
        let mut rule = vec![0; output_len];
        for (i, r) in rule.iter_mut().enumerate().take(output_len) {
            *r = if i % 2 == 0 {
                i / 2
            } else {
                output_len / 2 + i / 2
            };
        }
        pipeline.reorder_inputs(rule);

        // `output_len` / 2 for `TransformDummy`; 1 for `WriteResultCacheSink`.
        let mut items = Vec::with_capacity(output_len / 2 + 1);
        // 3. Add `TransformDummy` to the front half pipes.
        for _ in 0..output_len / 2 {
            let input = InputPort::create();
            let output = OutputPort::create();
            items.push(PipeItem::create(
                TransformDummy::create(input.clone(), output.clone()),
                vec![input],
                vec![output],
            ));
        }

        // 4. Add `WriteResultCacheSink` (`AsyncMpscSinker`) to the back half pipes.
        let mut sink_inputs = Vec::with_capacity(output_len / 2);
        for _ in 0..output_len / 2 {
            sink_inputs.push(InputPort::create());
        }
        items.push(PipeItem::create(
            WriteResultCacheSink::try_create(
                self.ctx.clone(),
                key,
                schema,
                sink_inputs.clone(),
                kv_store,
            )?,
            sink_inputs,
            vec![],
        ));

        pipeline.add_pipe(Pipe::create(output_len, output_len / 2, items));

        Ok(())
    }

    fn result_scan_table(&self) -> Result<Option<Arc<dyn Table>>> {
        let r_lock = self.metadata.read();
        let tables = r_lock.tables();
        for t in tables {
            if t.name().eq_ignore_ascii_case("result_scan") {
                return if tables.len() > 1 {
                    Err(ErrorCode::Unimplemented(
                        "The current `RESULT_SCAN` only supports single table queries",
                    ))
                } else {
                    Ok(Some(t.table()))
                };
            }
        }
        Ok(None)
    }
}

#[async_trait::async_trait]
impl Interpreter for SelectInterpreter {
    fn name(&self) -> &str {
        "SelectInterpreterV2"
    }

    fn schema(&self) -> DataSchemaRef {
        self.bind_context.output_schema()
    }

    /// This method will create a new pipeline
    /// The QueryPipelineBuilder will use the optimized plan to generate a Pipeline
    #[tracing::instrument(level = "debug", name = "select_interpreter_execute", skip(self), fields(ctx.id = self.ctx.get_id().as_str()))]
    #[async_backtrace::framed]
    async fn execute2(&self) -> Result<PipelineBuildResult> {
        // 0. Need to build physical plan first to get the partitions.
        let physical_plan = self.build_physical_plan().await?;
        if self.ctx.get_settings().get_enable_query_result_cache()? && self.ctx.get_cacheable() {
            let key = gen_result_cache_key(self.formatted_ast.as_ref().unwrap());
            // 1. Try to get result from cache.
            let kv_store = UserApiProvider::instance().get_meta_store_client();

            // Execute `select * from result_scan(last_query_id)` multiple times
            // should return same result. Please consider the following scenarios:
            // 1) select * from t1;
            // 2) select * from result_scan(last_query_id()); --> returns result same as line 1
            // 3) insert into t1 values(2);
            // 4) select * from t1; --> result changed since we insert new data.
            // 5) select * from result_scan(last_query_id()); --> result same as line 2 cause cache
            // If we read cache for 5, we will see it returns same result as 1 and 2 cause the
            // generated result_cache_key are same for this statement, so here we fetch the previous
            // meta_key through related query_id and set this meta_key with current query_id.
            if let Some(t) = self.result_scan_table()? {
                let arg_query_id = parse_result_scan_args(&t.table_args().unwrap())?;
                let meta_key = self.ctx.get_result_cache_key(&arg_query_id);
                if let Some(meta_key) = meta_key {
                    self.ctx
                        .set_query_id_result_cache(self.ctx.get_id(), meta_key);
                }
                return self.build_pipeline(physical_plan).await;
            }

            let cache_reader = ResultCacheReader::create(
                self.ctx.clone(),
                &key,
                kv_store.clone(),
                self.ctx
                    .get_settings()
                    .get_query_result_cache_allow_inconsistent()?,
            );

            // 2. Check the cache.
            match cache_reader.try_read_cached_result().await {
                Ok(Some(blocks)) => {
                    // 2.0 update query_id -> result_cache_meta_key in session.
                    self.ctx
                        .set_query_id_result_cache(self.ctx.get_id(), cache_reader.get_meta_key());
                    // 2.1 If found, return the result directly.
                    return PipelineBuildResult::from_blocks(blocks);
                }
                Ok(None) => {
                    let mut build_res = self.build_pipeline(physical_plan).await?;
                    // 2.2 If not found result in cache, add pipelines to write the result to cache.
                    let schema = infer_table_schema(&self.schema())?;
                    self.add_result_cache(&key, schema, &mut build_res.main_pipeline, kv_store)?;
                    return Ok(build_res);
                }
                Err(e) => {
                    // 2.3 If an error occurs, turn back to the normal pipeline.
                    tracing::error!("Failed to read query result cache. {}", e);
                }
            }
        }
        // Not use query cache.
        self.build_pipeline(physical_plan).await
    }
}
