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
use fastrace::collector::SpanRecord;
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

        // Apply filtering based on options
        let filtered_spans = self.filter_spans(spans);
        log::info!(
            "EXPLAIN TRACE: {} spans after filtering",
            filtered_spans.len()
        );

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
        let merged_trace =
            QueryTrace::merge_jaeger_traces(filtered_spans, &node_id, &remote_traces);

        let trace_data = StringType::from_data(vec![merged_trace]);
        Ok(vec![DataBlock::new_from_columns(vec![trace_data])])
    }

    /// Filter spans based on LEVEL and FILTER options
    fn filter_spans(&self, spans: Vec<SpanRecord>) -> Vec<SpanRecord> {
        spans
            .into_iter()
            .filter(|span| {
                // Apply LEVEL filter
                if !self.pass_level_filter(span) {
                    return false;
                }

                // Apply FILTER condition
                if let Some(filter) = &self.options.filter {
                    if !self.apply_filter(span, filter) {
                        return false;
                    }
                }

                true
            })
            .collect()
    }

    /// Check if span passes the LEVEL filter
    fn pass_level_filter(&self, span: &SpanRecord) -> bool {
        match self.options.level {
            TraceLevel::All => true,
            TraceLevel::High => {
                // Exclude processor-level spans
                let name = &span.name;

                // Exclude spans ending with ::process or ::async_process
                if name.ends_with("::process") || name.ends_with("::async_process") {
                    return false;
                }

                // Exclude ProcessorAsyncTask spans
                if name.contains("ProcessorAsyncTask") {
                    return false;
                }

                true
            }
        }
    }

    /// Apply a filter condition to a span
    fn apply_filter(&self, span: &SpanRecord, filter: &TraceFilter) -> bool {
        match filter {
            TraceFilter::Duration { op, threshold_us } => {
                // span.duration_ns is in nanoseconds, convert to microseconds
                let duration_us = span.duration_ns / 1_000;
                match op {
                    TraceFilterOp::Gt => duration_us > *threshold_us,
                    TraceFilterOp::Gte => duration_us >= *threshold_us,
                    TraceFilterOp::Lt => duration_us < *threshold_us,
                    TraceFilterOp::Lte => duration_us <= *threshold_us,
                    TraceFilterOp::Eq => duration_us == *threshold_us,
                }
            }
            TraceFilter::Name { pattern, negated } => {
                let matches = self.like_match(&span.name, pattern);
                if *negated { !matches } else { matches }
            }
            TraceFilter::And(a, b) => self.apply_filter(span, a) && self.apply_filter(span, b),
            TraceFilter::Or(a, b) => self.apply_filter(span, a) || self.apply_filter(span, b),
        }
    }

    /// Simple LIKE pattern matching with % wildcard
    fn like_match(&self, text: &str, pattern: &str) -> bool {
        // Convert SQL LIKE pattern to regex-like matching
        let parts: Vec<&str> = pattern.split('%').collect();

        if parts.len() == 1 {
            // No wildcards, exact match
            return text == pattern;
        }

        let mut pos = 0;
        for (i, part) in parts.iter().enumerate() {
            if part.is_empty() {
                continue;
            }

            if let Some(found_pos) = text[pos..].find(part) {
                if i == 0 && found_pos != 0 {
                    // First part must match at the beginning if pattern doesn't start with %
                    return false;
                }
                pos += found_pos + part.len();
            } else {
                return false;
            }
        }

        // If pattern doesn't end with %, the last part must match at the end
        if !pattern.ends_with('%') && !parts.last().unwrap_or(&"").is_empty() {
            return text.ends_with(parts.last().unwrap());
        }

        true
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
