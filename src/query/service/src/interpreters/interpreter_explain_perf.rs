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
use std::time::Duration;

use databend_common_ast::ast::ExplainPerfFormat;
use databend_common_base::runtime::CpuStack;
use databend_common_base::runtime::CpuSummaryLevel;
use databend_common_base::runtime::LOW_CONFIDENCE_SAMPLES;
use databend_common_base::runtime::PerfConfig;
use databend_common_base::runtime::QueryPerf;
use databend_common_base::runtime::ThreadTracker;
use databend_common_base::runtime::cpu_folded_stacks;
use databend_common_base::runtime::summarize_cpu_stacks;
use databend_common_config::GlobalConfig;
use databend_common_exception::ErrorCode;
use databend_common_exception::Result;
use databend_common_expression::DataBlock;
use databend_common_expression::FromData;
use databend_common_expression::types::BooleanType;
use databend_common_expression::types::Float64Type;
use databend_common_expression::types::StringType;
use databend_common_expression::types::UInt64Type;
use databend_common_meta_store::MetaStoreProvider;
use databend_common_sql::Planner;
use databend_meta_plugin_semaphore::acquirer::Permit;
use databend_meta_runtime::DatabendRuntime;
use futures_util::TryStreamExt;

use crate::interpreters::Interpreter;
use crate::interpreters::InterpreterFactory;
use crate::interpreters::QueryFinishHooks;
use crate::pipelines::PipelineBuildResult;
use crate::schedulers::ServiceQueryExecutor;
use crate::sessions::QueryContext;
use crate::sessions::TableContextPerf;

/// `EXPLAIN PERF [CPU] <statement>` runs the statement and samples its call stacks on CPU time.
pub struct ExplainPerfInterpreter {
    pub sql: String,
    pub ctx: Arc<QueryContext>,
    pub format: ExplainPerfFormat,
    pub limit: Option<u64>,
}

/// The rows per level in the `table` format by default.
const DEFAULT_TABLE_LIMIT: usize = 20;
/// The stacks in the `folded` format by default.
const DEFAULT_FOLDED_LIMIT: usize = 100;

impl ExplainPerfInterpreter {
    pub fn try_create(
        sql: String,
        format: ExplainPerfFormat,
        limit: Option<u64>,
        ctx: Arc<QueryContext>,
    ) -> Self {
        Self {
            sql,
            ctx,
            format,
            limit,
        }
    }

    pub async fn perf(&self) -> Result<Vec<DataBlock>> {
        let _permit = self.acquire_semaphore().await?;
        let config = PerfConfig {
            perf_enabled: true,
            profiler_enabled: true,
            frequency: 99,
        };
        self.ctx.set_perf_config(config.clone());
        let perf_guard = QueryPerf::start(config.frequency)?;
        ThreadTracker::tracking_future(self.simulate_execute()).await?;

        let (_flag_guard, profiler_guard) = perf_guard;

        let node_id = GlobalConfig::instance().query.node_id.clone();
        let block = match self.format {
            ExplainPerfFormat::Html => {
                let dumped = QueryPerf::dump(&profiler_guard)?;
                let other_nodes = self.ctx.get_nodes_perf().lock().clone();
                let html = QueryPerf::pretty_display(node_id, dumped, other_nodes.into_iter())
                    .replace("{{SUMMARY_TABLE}}", "");
                DataBlock::new_from_columns(vec![StringType::from_data(vec![html])])
            }
            ExplainPerfFormat::Table => {
                let stacks = QueryPerf::stacks(&profiler_guard)?;
                let limit = self.limit.map_or(DEFAULT_TABLE_LIMIT, |x| x as usize);
                table_block(&stacks, limit, &node_id, config.frequency)
            }
            ExplainPerfFormat::Folded => {
                let stacks = QueryPerf::stacks(&profiler_guard)?;
                let limit = self.limit.map_or(DEFAULT_FOLDED_LIMIT, |x| x as usize);
                let (stacks, samples): (Vec<_>, Vec<_>) =
                    cpu_folded_stacks(&stacks, Some(limit)).into_iter().unzip();
                DataBlock::new_from_columns(vec![
                    StringType::from_data(stacks),
                    UInt64Type::from_data(samples),
                ])
            }
        };
        Ok(vec![block])
    }

    pub async fn acquire_semaphore(&self) -> Result<Permit> {
        let config = GlobalConfig::instance();
        let meta_conf = config.meta.to_meta_grpc_client_conf();
        let meta_store = MetaStoreProvider::new(meta_conf)
            .create_meta_store::<DatabendRuntime>()
            .await
            .map_err(|_e| ErrorCode::Internal("Failed to get meta store for explain perf"))?;
        let meta_key = "__fd_explain_perf";
        meta_store
            .new_acquired(
                meta_key,
                1,
                config.query.node_id.clone(),
                Duration::from_secs(3),
            )
            .await
            .map_err(|_e| ErrorCode::Internal("Failed to acquire semaphore for explain perf"))
    }

    pub async fn simulate_execute(&self) -> Result<()> {
        let mut planner = Planner::new_with_query_executor(
            self.ctx.clone(),
            Arc::new(ServiceQueryExecutor::new(QueryContext::create_from(
                self.ctx.as_ref(),
            ))),
        );
        let previous_query_lineage = self.ctx.get_query_lineage();
        let result = async {
            let (plan, _extras) = planner.plan_sql(&self.sql).await?;
            let interpreter = InterpreterFactory::get(self.ctx.clone(), &plan).await?;
            let mut stream = interpreter
                .execute_with_hooks(self.ctx.clone(), QueryFinishHooks::nested_with_hooks())
                .await?;
            while stream.try_next().await?.is_some() {}
            Ok(())
        }
        .await;
        self.ctx.attach_query_lineage(previous_query_lineage);
        result
    }
}

/// The summary row, then the leaf functions and the Databend sites with the most samples.
fn table_block(stacks: &[CpuStack], limit: usize, node_id: &str, frequency: i32) -> DataBlock {
    let rows = summarize_cpu_stacks(stacks, limit);
    let note = |level: CpuSummaryLevel| match level {
        CpuSummaryLevel::Summary => Some(format!(
            "Sampled at {frequency} Hz on node {node_id}, other cluster nodes are not included. \
             'function' rows rank the functions by self_samples, the samples in the function \
             itself. 'site' rows rank Databend functions by the self_samples of the stacks whose \
             innermost Databend frame they are, i.e. including the library code they call. \
             total_samples also count all callees. share is of all samples by self_samples. Up to \
             {limit} rows per level."
        )),
        _ => None,
    };

    DataBlock::new_from_columns(vec![
        StringType::from_data(rows.iter().map(|x| x.level.as_str().to_string()).collect()),
        StringType::from_opt_data(rows.iter().map(|x| x.function.clone()).collect()),
        UInt64Type::from_data(rows.iter().map(|x| x.self_samples).collect()),
        UInt64Type::from_data(rows.iter().map(|x| x.total_samples).collect()),
        Float64Type::from_data(rows.iter().map(|x| x.share).collect()),
        BooleanType::from_data(
            rows.iter()
                .map(|x| x.self_samples < LOW_CONFIDENCE_SAMPLES)
                .collect(),
        ),
        StringType::from_opt_data(rows.iter().map(|x| note(x.level)).collect()),
    ])
}

#[async_trait::async_trait]
impl Interpreter for ExplainPerfInterpreter {
    fn name(&self) -> &str {
        "ExplainPerfInterpreter"
    }

    fn is_ddl(&self) -> bool {
        false
    }

    fn execute2(&self) -> futures::future::BoxFuture<'_, Result<PipelineBuildResult>> {
        Box::pin(async move {
            let data_blocks = self.perf().await?;
            PipelineBuildResult::from_blocks(data_blocks)
        })
    }
}
