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

use std::env;

use databend_common_catalog::table_context::TableContext;
use databend_common_expression::DataBlock;
use databend_query::interpreters::InterpreterFactory;
use databend_query::interpreters::interpreter_plan_sql;
use databend_query::test_kits::TestFixture;
use fastrace::collector::SpanRecord;
use fastrace::future::FutureExt;
use fastrace::prelude::*;
use futures_util::TryStreamExt;

use super::infra::TraceDebugRuntimeFlavor;
use super::infra::build_trace_debug_otlp_export;
use super::infra::persist_trace_debug_output;
use super::infra::run_future_in_named_thread;
use super::infra::setup_trace_debug_fixture;
use super::infra::trace_capture_handle;
use super::infra::trace_test_lock;

#[derive(Debug, Clone)]
struct DirectTraceDebugConfig {
    sqls: Vec<String>,
}

impl DirectTraceDebugConfig {
    fn from_env() -> Self {
        Self {
            sqls: parse_direct_trace_debug_sqls(),
        }
    }
}

fn parse_direct_trace_debug_sqls() -> Vec<String> {
    const DEFAULT_SQLS: &[&str] = &["select number from numbers(10)", "show databases"];

    env::var("DATABEND_DIRECT_TRACE_DEBUG_SQLS")
        .ok()
        .map(|sqls| {
            sqls.split(";;")
                .map(str::trim)
                .filter(|sql| !sql.is_empty())
                .map(ToOwned::to_owned)
                .collect::<Vec<_>>()
        })
        .filter(|sqls| !sqls.is_empty())
        .unwrap_or_else(|| DEFAULT_SQLS.iter().map(|sql| sql.to_string()).collect())
}

#[derive(Debug)]
enum DirectExecutionOutcome {
    Succeeded { blocks: usize, rows: usize },
    Failed { stage: &'static str, error: String },
}

async fn dump_direct_sql_trace(
    fixture: &TestFixture,
    sql: &str,
) -> anyhow::Result<Vec<SpanRecord>> {
    let capture = trace_capture_handle();
    capture.reset();

    let ctx = fixture.new_query_ctx().await?;
    let query_id = ctx.get_id();
    let sql_text = sql.to_string();
    let query_id_for_span = query_id.clone();
    let sql_text_for_span = sql_text.clone();
    let root = Span::root("db.query", SpanContext::random())
        .with_property(|| ("db.system", "databend"))
        .with_property(|| ("db.statement", sql_text_for_span.clone()))
        .with_property(|| ("query_id", query_id.clone()))
        .with_property(|| ("databend.trace.entry", "direct"));

    async move {
        let (plan, _, _queue_guard) = match interpreter_plan_sql(ctx.clone(), sql, true, None).await
        {
            Ok(plan_res) => plan_res,
            Err(err) => {
                let outcome = DirectExecutionOutcome::Failed {
                    stage: "plan_sql",
                    error: err.to_string(),
                };
                record_direct_execution_outcome(&query_id_for_span, &outcome);
                return outcome;
            }
        };

        let interpreter = match InterpreterFactory::get(ctx.clone(), &plan).await {
            Ok(interpreter) => interpreter,
            Err(err) => {
                let outcome = DirectExecutionOutcome::Failed {
                    stage: "build_interpreter",
                    error: err.to_string(),
                };
                record_direct_execution_outcome(&query_id_for_span, &outcome);
                return outcome;
            }
        };

        let stream = match interpreter.execute(ctx.clone()).await {
            Ok(stream) => stream,
            Err(err) => {
                let outcome = DirectExecutionOutcome::Failed {
                    stage: "execute",
                    error: err.to_string(),
                };
                record_direct_execution_outcome(&query_id_for_span, &outcome);
                return outcome;
            }
        };

        let outcome = match stream.try_collect::<Vec<DataBlock>>().await {
            Ok(blocks) => DirectExecutionOutcome::Succeeded {
                blocks: blocks.len(),
                rows: blocks.iter().map(DataBlock::num_rows).sum(),
            },
            Err(err) => DirectExecutionOutcome::Failed {
                stage: "collect_stream",
                error: err.to_string(),
            },
        };
        record_direct_execution_outcome(&query_id_for_span, &outcome);
        outcome
    }
    .in_span(root)
    .await;

    fastrace::flush();
    Ok(capture.snapshot())
}

fn record_direct_execution_outcome(query_id: &str, outcome: &DirectExecutionOutcome) {
    let span = LocalSpan::enter_with_local_parent("databend.trace_debug.execution");
    let span = span.with_property(|| ("query_id", query_id.to_string()));

    match outcome {
        DirectExecutionOutcome::Succeeded { blocks, rows } => {
            let _span = span
                .with_property(|| ("span.status_code", "ok"))
                .with_property(|| ("databend.query.status", "succeeded"))
                .with_property(|| ("databend.query.blocks", blocks.to_string()))
                .with_property(|| ("databend.query.rows", rows.to_string()));
        }
        DirectExecutionOutcome::Failed { stage, error } => {
            let _span = span
                .with_property(|| ("span.status_code", "error"))
                .with_property(|| ("span.status_description", error.clone()))
                .with_property(|| ("databend.query.status", "failed"))
                .with_property(|| ("databend.query.stage", (*stage).to_string()))
                .with_property(|| ("exception.message", error.clone()));
        }
    }
}

#[test]
#[ignore = "debug utility: dumps parser/planner/interpreter/pipeline tracing without HTTP"]
fn test_dump_direct_sql_trace_debug() -> anyhow::Result<()> {
    let _guard = trace_test_lock().lock().unwrap();
    run_future_in_named_thread(
        "trace-debug-direct",
        TraceDebugRuntimeFlavor::MultiThread,
        || async move {
            let config = DirectTraceDebugConfig::from_env();
            let fixture = setup_trace_debug_fixture().await?;
            let _capture = trace_capture_handle();
            let mut spans = Vec::new();

            for sql in &config.sqls {
                spans.extend(dump_direct_sql_trace(&fixture, sql).await?);
            }

            let export = build_trace_debug_otlp_export(&spans);
            let _path = persist_trace_debug_output("direct", &export)?;

            Ok(())
        },
    )
}
