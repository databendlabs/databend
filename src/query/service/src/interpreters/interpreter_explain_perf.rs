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

use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

use databend_common_base::base::convert_number_size;
use databend_common_base::runtime::PerfConfig;
use databend_common_base::runtime::PerfEvent;
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
use databend_common_pipeline::core::always_callback;
use databend_common_sql::Planner;
use databend_meta_plugin_semaphore::acquirer::Permit;
use databend_meta_runtime::DatabendRuntime;

use crate::interpreters::Interpreter;
use crate::interpreters::InterpreterFactory;
use crate::interpreters::QueryFinishHooks;
use crate::pipelines::PipelineBuildResult;
use crate::pipelines::executor::ExecutorSettings;
use crate::pipelines::executor::PipelineCompleteExecutor;
use crate::pipelines::executor::PipelinePullingExecutor;
use crate::schedulers::ServiceQueryExecutor;
use crate::sessions::QueryContext;

pub struct ExplainPerfInterpreter {
    pub sql: String,
    pub ctx: Arc<QueryContext>,
    pub events: Vec<PerfEvent>,
}

impl ExplainPerfInterpreter {
    pub fn try_create(
        sql: String,
        event_names: Vec<String>,
        ctx: Arc<QueryContext>,
    ) -> Result<Self> {
        let events = if event_names.is_empty() {
            PerfEvent::defaults()
        } else {
            let mut resolved = Vec::with_capacity(event_names.len());
            for name in &event_names {
                match PerfEvent::from_name(name) {
                    Some(e) => resolved.push(e),
                    None => {
                        return Err(ErrorCode::SyntaxException(format!(
                            "Unknown perf event: '{name}'. Valid events: {}",
                            PerfEvent::all_names().collect::<Vec<_>>().join(", ")
                        )));
                    }
                }
            }
            resolved
        };
        Ok(Self { sql, ctx, events })
    }

    pub async fn perf(&self) -> Result<Vec<DataBlock>> {
        // PerfCounters are only supported with QueryPipelineExecutor。
        let config_enable_queries_executor = GlobalConfig::instance()
            .query
            .common
            .enable_queries_executor;
        if config_enable_queries_executor {
            return Err(ErrorCode::Unimplemented(
                "EXPLAIN PERF with hardware performance counters is not supported under QueriesPipelineExecutor.",
            ));
        }

        let _permit = self.acquire_semaphore().await?;
        let config = PerfConfig {
            profiler_enabled: true,
            events: self.events.clone(),
            frequency: 99,
        };
        self.ctx.set_perf_config(config.clone());
        let perf_guard = QueryPerf::start(config.frequency)?;
        ThreadTracker::tracking_future(self.simulate_execute()).await?;

        let (_flag_guard, profiler_guard) = perf_guard;

        let node_id = GlobalConfig::instance().query.node_id.clone();
        let dumped = QueryPerf::dump(&profiler_guard)?;
        let other_nodes = self.ctx.get_nodes_perf().lock().clone();
        let mut html = QueryPerf::pretty_display(node_id, dumped, other_nodes.into_iter());

        let hw_counters_html = self.build_hw_counters_html();
        if !hw_counters_html.is_empty() {
            html = html.replacen("{{PERF_COUNTERS_TABLE}}", &hw_counters_html, 1);
        }
        html = html.replace("{{PERF_COUNTERS_TABLE}}", "");

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
        let mut build_res = interpreter.execute2().await?;

        if build_res.main_pipeline.is_empty() {
            return Ok(());
        }

        let ctx = self.ctx.clone();
        build_res.main_pipeline.set_on_finished(always_callback(
            QueryFinishHooks::nested_with_hooks().into_callback(ctx.clone()),
        ));

        let settings = self.ctx.get_settings();
        build_res.set_max_threads(settings.get_max_threads()? as usize);
        let settings = ExecutorSettings::try_create(self.ctx.clone())?;

        if build_res.main_pipeline.is_complete_pipeline()? {
            let mut pipelines = build_res.sources_pipelines;
            pipelines.push(build_res.main_pipeline);
            let executor = PipelineCompleteExecutor::from_pipelines(pipelines, settings)?;
            ctx.set_executor(executor.get_inner())?;
            executor.execute()?;
        } else {
            let mut executor = PipelinePullingExecutor::from_pipelines(build_res, settings)?;
            ctx.set_executor(executor.get_inner())?;
            executor.start();
            while (executor.pull_data()?).is_some() {}
        }
        Ok(())
    }

    fn build_hw_counters_html(&self) -> String {
        let events = &self.events;
        let mut sections = Vec::new();

        let local_node_id = GlobalConfig::instance().query.node_id.clone();
        let all_nodes = self.ctx.get_nodes_perf_counters();
        // Sort so the local (coordinator) node appears first.
        let mut nodes: Vec<_> = all_nodes.into_iter().collect();
        nodes.sort_by_key(|(id, _)| if id == &local_node_id { 0 } else { 1 });

        for (node_id, node_counters) in &nodes {
            let entries: Vec<_> = node_counters
                .counters
                .iter()
                .filter(|(_, c)| !c.is_empty())
                .map(|(name, c)| (name.clone(), c))
                .collect();
            if !entries.is_empty() {
                sections.push(Self::build_node_table(
                    node_id,
                    events,
                    &entries,
                    node_counters.multiplexed,
                ));
            }
        }

        if sections.is_empty() {
            return String::new();
        }

        format!(
            r#"<div style="max-width:1200px;margin-left:auto;margin-right:auto;margin-bottom:30px;font-family:monospace;">
<h3>Hardware Performance Counters</h3>
{}
</div>"#,
            sections.join("\n")
        )
    }

    fn build_node_table(
        node_id: &str,
        events: &[PerfEvent],
        entries: &[(String, &HashMap<PerfEvent, u64>)],
        multiplexed: bool,
    ) -> String {
        let mut header = "<th>Plan Node</th>".to_string();
        for event in events {
            header.push_str(&format!("<th>{}</th>", event.display_name()));
        }
        let has_cycles = events.contains(&PerfEvent::CpuCycles);
        let has_insns = events.contains(&PerfEvent::Instructions);
        let has_misses = events.contains(&PerfEvent::CacheMisses);
        let has_refs = events.contains(&PerfEvent::CacheReferences);
        if has_cycles && has_insns {
            header.push_str("<th>IPC</th>");
        }
        if has_misses && has_refs {
            header.push_str("<th>Cache Miss Rate</th>");
        }

        let mut rows = String::new();
        let mut totals: HashMap<PerfEvent, u64> = events.iter().map(|e| (*e, 0u64)).collect();

        for (name, counters) in entries {
            let mut row = format!("<td>{}</td>", name);
            for event in events {
                let val = counters.get(event).copied().unwrap_or(0);
                *totals.entry(*event).or_insert(0) += val;
                row.push_str(&format!("<td>{}</td>", convert_number_size(val as f64)));
            }
            Self::append_derived_metrics(
                &mut row, counters, has_cycles, has_insns, has_misses, has_refs,
            );
            rows.push_str(&format!("<tr>{}</tr>\n", row));
        }

        // Total row
        let mut total_row = "<td>TOTAL</td>".to_string();
        for event in events {
            total_row.push_str(&format!(
                "<td>{}</td>",
                convert_number_size(*totals.get(event).unwrap_or(&0) as f64)
            ));
        }
        Self::append_derived_metrics(
            &mut total_row,
            &totals,
            has_cycles,
            has_insns,
            has_misses,
            has_refs,
        );

        let warning = if multiplexed {
            r#"<p style="color:#cc6600;font-weight:bold;">&#9888; Kernel counter multiplexing detected: values are estimated. Consider reducing the number of perf events for accurate measurements.</p>"#
        } else {
            ""
        };

        format!(
            r#"<h4 style="color:#4a90e2;">Node: {node_id}</h4>
{warning}
<table border="1" cellpadding="6" cellspacing="0" style="border-collapse:collapse;width:100%;margin-bottom:20px;">
<tr style="background:#e0e0e0;">{header}</tr>
{rows}
<tr style="background:#f0f0f0;font-weight:bold;">{total_row}</tr>
</table>"#
        )
    }

    fn append_derived_metrics(
        row: &mut String,
        counters: &HashMap<PerfEvent, u64>,
        has_cycles: bool,
        has_insns: bool,
        has_misses: bool,
        has_refs: bool,
    ) {
        if has_cycles && has_insns {
            let cycles = counters.get(&PerfEvent::CpuCycles).copied().unwrap_or(0);
            let insns = counters.get(&PerfEvent::Instructions).copied().unwrap_or(0);
            let ipc = if cycles > 0 {
                format!("{:.2}", insns as f64 / cycles as f64)
            } else {
                "-".into()
            };
            row.push_str(&format!("<td>{}</td>", ipc));
        }
        if has_misses && has_refs {
            let misses = counters.get(&PerfEvent::CacheMisses).copied().unwrap_or(0);
            let refs = counters
                .get(&PerfEvent::CacheReferences)
                .copied()
                .unwrap_or(0);
            let rate = if refs > 0 {
                format!("{:.2}%", misses as f64 / refs as f64 * 100.0)
            } else {
                "-".into()
            };
            row.push_str(&format!("<td>{}</td>", rate));
        }
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
