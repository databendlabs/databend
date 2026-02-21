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

use databend_common_ast::ast::ExplainTraceOptions;
use databend_common_ast::ast::TraceFilter;
use databend_common_ast::ast::TraceFilterOp;
use databend_common_ast::ast::TraceLevel;
use databend_common_base::runtime::CollectorGuard;
use databend_common_base::runtime::GlobalTraceReporter;
use databend_common_base::runtime::QueryTrace;
use databend_common_base::runtime::ThreadTracker;
use databend_common_base::runtime::TraceCollector;
use databend_common_base::runtime::TraceFilterOptions;
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
    pub options: ExplainTraceOptions,
    pub ctx: Arc<QueryContext>,
}

impl ExplainTraceInterpreter {
    pub fn try_create(
        sql: String,
        options: ExplainTraceOptions,
        ctx: Arc<QueryContext>,
    ) -> Result<Self> {
        Ok(Self { sql, options, ctx })
    }

    pub async fn trace(&self) -> Result<Vec<DataBlock>> {
        // Flush any pending spans from previous queries
        fastrace::flush();

        // Set trace flag to enable trace collection in distributed nodes
        self.ctx.set_trace_flag(true);

        // Build filter options for collection-time filtering
        let filter_options = self.build_filter_options();

        // Store filter options in context for remote nodes
        self.ctx.set_trace_filter_options(filter_options.clone());

        // Create trace collector with filter options
        let collector = Arc::new(std::sync::Mutex::new(TraceCollector::with_filter(
            filter_options,
        )));

        // Register the collector with GlobalTraceReporter instead of replacing the reporter
        // This preserves the original tracing configuration (OTLP, StructLog, etc.)
        let collector_id = GlobalTraceReporter::instance().register_collector(collector.clone());

        // Use RAII guard to ensure collector is unregistered even on panic
        let _collector_guard = CollectorGuard::new(collector_id);

        // Mark that EXPLAIN TRACE has set up the reporter (to prevent overwriting)
        self.ctx.set_explain_trace_reporter_set(true);

        // Create root span context and set trace_parent for distributed tracing
        let root_context = SpanContext::random();
        let root_trace_id = root_context.trace_id;
        self.ctx
            .set_trace_parent(Some(root_context.encode_w3c_traceparent()));

        // Create root span AFTER setting reporter so it gets collected
        let root = Span::root("EXPLAIN TRACE", root_context);

        // Execute the query - child spans will be created via #[fastrace::trace] macros
        // which use Span::enter_with_local_parent()
        let result = ThreadTracker::tracking_future(self.simulate_execute().in_span(root)).await;

        // Flush fastrace to ensure all spans are collected
        fastrace::flush();

        // Check for errors after flush (collector will be unregistered by guard on drop)
        result?;

        // Collect spans from the collector (already filtered during collection)
        // Filter by trace ID to only include spans from this query
        let all_spans = collector.lock().unwrap().get_spans();
        let spans: Vec<_> = all_spans
            .into_iter()
            .filter(|span| span.trace_id == root_trace_id)
            .collect();

        // Collect trace data from other nodes
        let node_id = GlobalConfig::instance().query.node_id.clone();
        let other_nodes = self.ctx.get_nodes_trace().lock().clone();

        // Convert remote traces to the format expected by merge_jaeger_traces
        let remote_traces: Vec<(String, String)> = other_nodes.into_iter().collect();

        // Convert root_trace_id to hex string for filtering remote traces
        let root_trace_id_hex = format!("{:032x}", root_trace_id.0);

        // Merge all traces into a single Jaeger JSON, filtering by trace ID
        let merged_trace =
            QueryTrace::merge_jaeger_traces(spans, &node_id, &remote_traces, &root_trace_id_hex);

        let trace_data = StringType::from_data(vec![merged_trace]);
        Ok(vec![DataBlock::new_from_columns(vec![trace_data])])
    }

    /// Build filter options from EXPLAIN TRACE options for collection-time filtering
    fn build_filter_options(&self) -> TraceFilterOptions {
        let mut options = TraceFilterOptions::new();

        // Apply LEVEL filter
        if matches!(self.options.level, TraceLevel::High) {
            options = options.with_high_level_only(true);
        }

        // Apply FILTER condition (only duration filters can be applied during collection)
        if let Some(filter) = &self.options.filter {
            self.apply_duration_filter_to_options(&mut options, filter);
        }

        options
    }

    /// Extract duration filter from TraceFilter and apply to TraceFilterOptions
    fn apply_duration_filter_to_options(
        &self,
        options: &mut TraceFilterOptions,
        filter: &TraceFilter,
    ) {
        match filter {
            TraceFilter::Duration { op, threshold_us } => {
                match op {
                    TraceFilterOp::Gt | TraceFilterOp::Gte => {
                        // DURATION > X or DURATION >= X means min_duration
                        options.min_duration_us = Some(*threshold_us);
                    }
                    TraceFilterOp::Lt | TraceFilterOp::Lte => {
                        // DURATION < X or DURATION <= X means max_duration
                        options.max_duration_us = Some(*threshold_us);
                    }
                    TraceFilterOp::Eq => {
                        // DURATION = X means both min and max
                        options.min_duration_us = Some(*threshold_us);
                        options.max_duration_us = Some(*threshold_us);
                    }
                }
            }
            TraceFilter::And(a, b) => {
                self.apply_duration_filter_to_options(options, a);
                self.apply_duration_filter_to_options(options, b);
            }
            TraceFilter::Or(_, _) => {
                // OR filters are complex, skip collection-time filtering
            }
            TraceFilter::Name { .. } => {
                // Name filters can't be applied during collection
            }
        }
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
        // Drop the data stream explicitly to ensure all cleanup happens
        // The on_finished callbacks (including wait_shutdown) will complete
        // before the stream is fully dropped, ensuring all remote traces are received
        drop(data_stream);

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
