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

use databend_common_base::runtime::QueryTrace;
use databend_common_base::runtime::ThreadTracker;
use databend_common_base::runtime::TraceCollector;
use databend_common_catalog::table_context::TableContext;
use databend_common_config::GlobalConfig;
use databend_common_exception::Result;
use databend_common_expression::DataBlock;
use databend_common_expression::FromData;
use databend_common_expression::types::StringType;
use databend_common_sql::Planner;
use fastrace::prelude::*;
use futures_util::StreamExt;

use crate::interpreters::Interpreter;
use crate::interpreters::InterpreterFactory;
use crate::pipelines::PipelineBuildResult;
use crate::schedulers::ServiceQueryExecutor;
use crate::sessions::QueryContext;

pub struct ExplainTraceInterpreter {
    pub sql: String,
    pub ctx: Arc<QueryContext>,
}

impl ExplainTraceInterpreter {
    pub fn try_create(sql: String, ctx: Arc<QueryContext>) -> Result<Self> {
        Ok(Self { sql, ctx })
    }

    pub async fn trace(&self) -> Result<Vec<DataBlock>> {
        // Set trace flag to enable trace collection in distributed nodes
        self.ctx.set_trace_flag(true);

        // Create trace collector and set it as the reporter
        let collector = Arc::new(std::sync::Mutex::new(TraceCollector::new()));
        let collector_clone = collector.clone();

        // Set the collector as the fastrace reporter
        fastrace::set_reporter(
            collector_clone.lock().unwrap().clone(),
            fastrace::collector::Config::default(),
        );

        // Mark that EXPLAIN TRACE has set up the reporter (to prevent overwriting)
        self.ctx.set_explain_trace_reporter_set(true);

        // Create root span context and set trace_parent for distributed tracing
        let root_context = SpanContext::random();
        self.ctx
            .set_trace_parent(Some(root_context.encode_w3c_traceparent()));

        // Create root span AFTER setting reporter so it gets collected
        let root = Span::root("EXPLAIN TRACE", root_context);

        // Execute the query - child spans will be created via #[fastrace::trace] macros
        // which use Span::enter_with_local_parent()
        let result = ThreadTracker::tracking_future(self.simulate_execute().in_span(root)).await;

        // Flush fastrace to ensure all spans are collected
        fastrace::flush();

        // Check for errors after flush
        result?;

        // Collect spans from the collector
        let spans = collector.lock().unwrap().get_spans();
        log::info!("EXPLAIN TRACE: collected {} local spans", spans.len());

        // Collect trace data from other nodes
        let node_id = GlobalConfig::instance().query.node_id.clone();
        let other_nodes = self.ctx.get_nodes_trace().lock().clone();
        log::info!(
            "EXPLAIN TRACE: collected traces from {} remote nodes",
            other_nodes.len()
        );

        // Convert remote traces to the format expected by merge_jaeger_traces
        let remote_traces: Vec<(String, String)> = other_nodes.into_iter().collect();

        // Merge all traces into a single Jaeger JSON
        let merged_trace = QueryTrace::merge_jaeger_traces(spans, &node_id, &remote_traces);

        let trace_data = StringType::from_data(vec![merged_trace]);
        Ok(vec![DataBlock::new_from_columns(vec![trace_data])])
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
impl Interpreter for ExplainTraceInterpreter {
    fn name(&self) -> &str {
        "ExplainTraceInterpreter"
    }

    fn is_ddl(&self) -> bool {
        false
    }

    async fn execute2(&self) -> Result<PipelineBuildResult> {
        let data_blocks = self.trace().await?;
        PipelineBuildResult::from_blocks(data_blocks)
    }
}
