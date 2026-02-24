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

use databend_common_base::runtime::QueryPerf;
use databend_common_base::runtime::ThreadTracker;
use databend_common_catalog::table_context::TableContext;
use databend_common_config::GlobalConfig;
use databend_common_exception::ErrorCode;
use databend_common_exception::Result;
use databend_common_expression::DataBlock;
use databend_common_expression::FromData;
use databend_common_expression::types::StringType;
use databend_common_meta_store::MetaStoreProvider;
use databend_common_sql::Planner;
use databend_meta_plugin_semaphore::acquirer::Permit;
use databend_meta_runtime::DatabendRuntime;
use futures_util::StreamExt;

use crate::interpreters::Interpreter;
use crate::interpreters::InterpreterFactory;
use crate::pipelines::PipelineBuildResult;
use crate::schedulers::ServiceQueryExecutor;
use crate::sessions::QueryContext;

// this could be got from the sql as a parameter if future needed
const FREQUENCY: i32 = 99;

pub struct ExplainPerfInterpreter {
    pub sql: String,
    pub ctx: Arc<QueryContext>,
}

impl ExplainPerfInterpreter {
    pub fn try_create(sql: String, ctx: Arc<QueryContext>) -> Result<Self> {
        Ok(Self { sql, ctx })
    }

    pub async fn perf(&self) -> Result<Vec<DataBlock>> {
        // perf need to acquire a distributed semaphore to ensure thread local flag
        // indicate correctly
        let _permit = self.acquire_semaphore().await?;

        let perf_guard = QueryPerf::start(FREQUENCY)?;
        ThreadTracker::tracking_future(self.simulate_execute()).await?;

        // collect the profiling data from the profiler guard and other nodes report
        let (_flag_guard, profiler_guard) = perf_guard;

        let node_id = GlobalConfig::instance().query.node_id.clone();
        let dumped = QueryPerf::dump(&profiler_guard)?;
        let other_nodes = self.ctx.get_nodes_perf().lock().clone();
        let html = QueryPerf::pretty_display(node_id, dumped, other_nodes.into_iter());

        let html = StringType::from_data(vec![html]);
        Ok(vec![DataBlock::new_from_columns(vec![html])])
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
        let (plan, _extras) = planner.plan_sql(&self.sql).await?;
        let interpreter = InterpreterFactory::get(self.ctx.clone(), &plan).await?;
        let mut data_stream = interpreter.execute(self.ctx.clone()).await?;
        while data_stream.next().await.is_some() {}
        Ok(())
    }
}

#[async_trait::async_trait]
impl Interpreter for ExplainPerfInterpreter {
    fn name(&self) -> &str {
        "ExplainPerfInterpreter"
    }

    fn is_ddl(&self) -> bool {
        false
    }

    async fn execute2(&self) -> Result<PipelineBuildResult> {
        let data_blocks = self.perf().await?;
        PipelineBuildResult::from_blocks(data_blocks)
    }
}
