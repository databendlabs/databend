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

use databend_common_ast::ast::ExplainPerfFormat;
use databend_common_base::base::convert_byte_size;
use databend_common_base::runtime::AllocProfile;
use databend_common_base::runtime::AllocStack;
use databend_common_base::runtime::AllocSummaryLevel;
use databend_common_base::runtime::LOW_CONFIDENCE_SAMPLES;
use databend_common_base::runtime::QueryPerf;
use databend_common_base::runtime::SAMPLE_INTERVAL;
use databend_common_base::runtime::ThreadTracker;
use databend_common_base::runtime::alloc_flamegraph;
use databend_common_base::runtime::alloc_folded_stacks;
use databend_common_base::runtime::summarize_alloc_stacks;
use databend_common_config::GlobalConfig;
use databend_common_exception::ErrorCode;
use databend_common_exception::Result;
use databend_common_expression::DataBlock;
use databend_common_expression::FromData;
use databend_common_expression::types::BooleanType;
use databend_common_expression::types::Float64Type;
use databend_common_expression::types::StringType;
use databend_common_expression::types::UInt64Type;
use databend_common_sql::Planner;
use futures_util::TryStreamExt;

use crate::interpreters::Interpreter;
use crate::interpreters::InterpreterFactory;
use crate::interpreters::QueryFinishHooks;
use crate::pipelines::PipelineBuildResult;
use crate::schedulers::ServiceQueryExecutor;
use crate::sessions::QueryContext;

/// `EXPLAIN PERF MEMORY <statement>` runs the statement and samples its allocations on this node.
///
/// The samples are attributed to plan nodes and call stacks. They show how much each plan node
/// allocates, including memory freed soon after, not the memory alive at a given moment.
pub struct ExplainMemoryInterpreter {
    sql: String,
    ctx: Arc<QueryContext>,
    format: ExplainPerfFormat,
    limit: Option<u64>,
}

/// The allocation sites per plan node in the HTML report.
const HTML_SITES_PER_PLAN: usize = 5;
/// The allocation sites per plan node in the `table` format by default.
const DEFAULT_TABLE_LIMIT: usize = 5;
/// The stacks in the `folded` format by default.
const DEFAULT_FOLDED_LIMIT: usize = 100;

impl ExplainMemoryInterpreter {
    pub fn try_create(
        sql: String,
        format: ExplainPerfFormat,
        limit: Option<u64>,
        ctx: Arc<QueryContext>,
    ) -> Self {
        ExplainMemoryInterpreter {
            sql,
            ctx,
            format,
            limit,
        }
    }

    async fn explain_memory(&self) -> Result<Vec<DataBlock>> {
        let profile = AllocProfile::create();

        let mut payload = ThreadTracker::new_tracking_payload();
        payload.alloc_profile = Some(profile.clone());
        ThreadTracker::tracking_future_with_payload(self.execute(), Some(Arc::new(payload)))
            .await?;

        let stacks = profile.stacks();
        let node_id = GlobalConfig::instance().query.node_id.clone();
        let block = match self.format {
            ExplainPerfFormat::Html => html_block(&stacks, node_id)?,
            ExplainPerfFormat::Table => {
                let limit = self.limit.map_or(DEFAULT_TABLE_LIMIT, |x| x as usize);
                table_block(&stacks, limit, &node_id)
            }
            ExplainPerfFormat::Folded => {
                let limit = self.limit.map_or(DEFAULT_FOLDED_LIMIT, |x| x as usize);
                folded_block(&stacks, limit)
            }
        };
        Ok(vec![block])
    }

    async fn execute(&self) -> Result<()> {
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

fn html_block(stacks: &[AllocStack], node_id: String) -> Result<DataBlock> {
    let svg = alloc_flamegraph(stacks, "Sampled allocations by plan node")
        .map_err(ErrorCode::Internal)?;
    let html = QueryPerf::pretty_display(node_id, svg, std::iter::empty())
        .replace("Query Performance Report", "Query Memory Allocation Report")
        .replace("{{PERF_COUNTERS_TABLE}}", &summary_html(stacks));
    Ok(DataBlock::new_from_columns(vec![StringType::from_data(
        vec![html],
    )]))
}

/// The summary row, then each plan node from the largest followed by its largest allocation sites.
fn table_block(stacks: &[AllocStack], limit: usize, node_id: &str) -> DataBlock {
    let rows = summarize_alloc_stacks(stacks, limit);
    let note = |level: AllocSummaryLevel| match level {
        AllocSummaryLevel::Summary => Some(format!(
            "Estimated from one sample every ~{} allocated on node {node_id}, other cluster nodes \
             are not included. bytes is the allocation volume, including memory freed soon after, \
             not the memory alive at a given moment. share is of all bytes for 'plan' rows and of \
             the plan node for 'site' rows. A site is the innermost Databend function of the call \
             stacks. Up to {limit} sites per plan node.",
            convert_byte_size(SAMPLE_INTERVAL as f64),
        )),
        _ => None,
    };

    DataBlock::new_from_columns(vec![
        StringType::from_data(rows.iter().map(|x| x.level.as_str().to_string()).collect()),
        StringType::from_opt_data(rows.iter().map(|x| x.plan_node.clone()).collect()),
        StringType::from_opt_data(rows.iter().map(|x| x.site.clone()).collect()),
        UInt64Type::from_data(rows.iter().map(|x| x.bytes).collect()),
        UInt64Type::from_data(rows.iter().map(|x| x.samples).collect()),
        Float64Type::from_data(rows.iter().map(|x| x.share).collect()),
        BooleanType::from_data(
            rows.iter()
                .map(|x| x.samples < LOW_CONFIDENCE_SAMPLES)
                .collect(),
        ),
        StringType::from_opt_data(rows.iter().map(|x| note(x.level)).collect()),
    ])
}

fn folded_block(stacks: &[AllocStack], limit: usize) -> DataBlock {
    let lines = alloc_folded_stacks(stacks, Some(limit));
    DataBlock::new_from_columns(vec![
        StringType::from_data(lines.iter().map(|(stack, _)| stack.clone()).collect()),
        UInt64Type::from_data(lines.iter().map(|(_, bytes)| *bytes).collect()),
        UInt64Type::from_data(
            lines
                .iter()
                .map(|(_, bytes)| bytes / SAMPLE_INTERVAL as u64)
                .collect(),
        ),
    ])
}

/// The plan node totals and, for each plan node, the Databend functions allocating the most.
fn summary_html(stacks: &[AllocStack]) -> String {
    let mut rows = String::new();
    let mut sites = String::new();
    let mut plan = None;
    let flush = |plan: Option<String>, sites: &mut String, rows: &mut String| {
        if let Some(plan) = plan {
            rows.push_str(&format!("{plan}<td>{sites}</td></tr>\n"));
        }
        sites.clear();
    };

    for row in summarize_alloc_stacks(stacks, HTML_SITES_PER_PLAN) {
        match row.level {
            AllocSummaryLevel::Summary => {}
            AllocSummaryLevel::Plan => {
                flush(plan.take(), &mut sites, &mut rows);
                plan = Some(format!(
                    "<tr><td>{}</td><td>{}</td><td>{:.1}%</td>",
                    escape_html(row.plan_node.as_deref().unwrap_or_default()),
                    convert_byte_size(row.bytes as f64),
                    row.share * 100.0,
                ));
            }
            AllocSummaryLevel::Site => {
                // Long symbols are cut to the cell width, the full symbol is shown on hover.
                let site = escape_html(row.site.as_deref().unwrap_or_default());
                sites.push_str(&format!(
                    "<div title=\"{site}\" style=\"white-space:nowrap;overflow:hidden;text-overflow:ellipsis;\"><b>{}</b> {site}</div>",
                    convert_byte_size(row.bytes as f64),
                ));
            }
        }
    }
    flush(plan.take(), &mut sites, &mut rows);

    format!(
        r#"<div style="max-width:1200px;margin-left:auto;margin-right:auto;margin-bottom:30px;font-family:monospace;font-size:13px;">
<h3>Allocated bytes by plan node</h3>
<p>Estimated from one sample every ~{} allocated on this node. Memory freed soon after allocation is included, so the numbers show allocation volume, not memory alive at a given moment. Long allocation sites are cut, hover them for the full symbol.</p>
<table border="1" cellpadding="6" cellspacing="0" style="border-collapse:collapse;width:100%;table-layout:fixed;margin-bottom:20px;">
<colgroup><col style="width:22%;"><col style="width:12%;"><col style="width:8%;"><col style="width:58%;"></colgroup>
<tr style="background:#e0e0e0;"><th>Plan Node</th><th>Allocated</th><th>Share</th><th>Top allocation sites</th></tr>
{rows}</table>
</div>"#,
        convert_byte_size(SAMPLE_INTERVAL as f64),
    )
}

fn escape_html(s: &str) -> String {
    s.replace('&', "&amp;")
        .replace('<', "&lt;")
        .replace('>', "&gt;")
        .replace('"', "&quot;")
}

#[async_trait::async_trait]
impl Interpreter for ExplainMemoryInterpreter {
    fn name(&self) -> &str {
        "ExplainMemoryInterpreter"
    }

    fn is_ddl(&self) -> bool {
        false
    }

    fn execute2(&self) -> futures::future::BoxFuture<'_, Result<PipelineBuildResult>> {
        Box::pin(async move {
            let data_blocks = self.explain_memory().await?;
            PipelineBuildResult::from_blocks(data_blocks)
        })
    }
}
