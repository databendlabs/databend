// Copyright 2026 Datafuse Labs.
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

use std::path::PathBuf;
use std::sync::Arc;

use databend_common_exception::ErrorCode;
use databend_common_exception::Result;
use databend_common_sql::optimizer::CollectStatisticsOptimizer;
use databend_common_sql::optimizer::Optimizer;
use databend_common_sql::optimizer::OptimizerContext;
use databend_common_sql::optimizer::StatisticsTraceCollector;
use databend_common_sql::plans::Plan;
use databend_common_sql_test_support::TestSuite;
use databend_enterprise_query::test_kits::context::EESetup;
use databend_query::sessions::QueryContext;
use databend_query::sessions::TableContextSettings;
use databend_query::test_kits::TestFixture;
use databend_query::test_kits::execute_command;

use crate::sql::planner::optimizer::test_utils::raw_plan;

#[tokio::test(flavor = "multi_thread", worker_threads = 1)]
async fn test_collect_statistics_trace_json() -> Result<()> {
    let fixture = TestFixture::setup_with_custom(EESetup::new()).await?;
    let ctx = fixture.new_query_ctx().await?;
    ctx.get_settings().set_enable_auto_materialize_cte(0)?;

    let case = StatisticsTraceJsonCase {
        sql_file: "view_materialized_cte_join.sql",
        json_file: "view_materialized_cte_join.json",
        setup_sqls: &[
            "SET GLOBAL enable_experimental_row_access_policy = 1",
            "SET enable_experimental_row_access_policy = 1",
            "SET enable_table_snapshot_stats = 1",
            "DROP VIEW IF EXISTS statistics_trace_recent_orders",
            "DROP TABLE IF EXISTS statistics_trace_customers",
            "DROP TABLE IF EXISTS statistics_trace_orders",
            "DROP ROW ACCESS POLICY IF EXISTS statistics_trace_region_policy",
            "DROP FUNCTION IF EXISTS trace_region_norm",
            "CREATE FUNCTION trace_region_norm (STRING) RETURNS STRING LANGUAGE javascript HANDLER = 'trace_region_norm' AS $$
                export function trace_region_norm(s) {
                    return s == null ? null : s.toLowerCase();
                }
            $$",
            "CREATE ROW ACCESS POLICY statistics_trace_region_policy AS (region STRING) RETURNS BOOLEAN -> region != 'AFRICA'",
            "CREATE OR REPLACE TABLE statistics_trace_orders(o_orderkey INT, o_custkey INT, o_total DOUBLE)",
            "INSERT INTO statistics_trace_orders VALUES (1, 10, 12.5), (2, 10, 4.0), (3, 20, 33.0), (4, 30, 18.0)",
            "CREATE OR REPLACE TABLE statistics_trace_customers(c_custkey INT, c_region STRING)",
            "INSERT INTO statistics_trace_customers VALUES (10, 'ASIA'), (20, 'EUROPE'), (30, 'AFRICA')",
            "ALTER TABLE statistics_trace_customers ADD ROW ACCESS POLICY statistics_trace_region_policy ON (c_region)",
            "ANALYZE TABLE statistics_trace_orders",
            "ANALYZE TABLE statistics_trace_customers",
            "CREATE OR REPLACE VIEW statistics_trace_recent_orders AS SELECT o_orderkey, o_custkey, o_total FROM statistics_trace_orders WHERE o_total > 10",
        ],
        cleanup_sqls: &[
            "DROP VIEW IF EXISTS statistics_trace_recent_orders",
            "DROP TABLE IF EXISTS statistics_trace_customers",
            "DROP TABLE IF EXISTS statistics_trace_orders",
            "DROP ROW ACCESS POLICY IF EXISTS statistics_trace_region_policy",
            "DROP FUNCTION IF EXISTS trace_region_norm",
        ],
    };

    case.run(&ctx).await?;
    Ok(())
}

struct StatisticsTraceJsonCase {
    sql_file: &'static str,
    json_file: &'static str,
    setup_sqls: &'static [&'static str],
    cleanup_sqls: &'static [&'static str],
}

impl StatisticsTraceJsonCase {
    async fn run(&self, ctx: &Arc<QueryContext>) -> Result<()> {
        for sql in self.setup_sqls {
            execute_command(ctx.clone(), sql).await?;
        }
        ctx.clear_tables_cache();
        ctx.clear_table_meta_timestamps_cache();

        let result = self.compare_collected_json(ctx).await;
        let cleanup = self.cleanup(ctx).await;
        result?;
        cleanup
    }

    async fn cleanup(&self, ctx: &Arc<QueryContext>) -> Result<()> {
        for sql in self.cleanup_sqls {
            execute_command(ctx.clone(), sql).await?;
        }
        Ok(())
    }

    async fn compare_collected_json(&self, ctx: &Arc<QueryContext>) -> Result<()> {
        let sql = self.read_sql()?;
        let actual = collect_statistics_trace(ctx, &sql).await?;

        if std::env::var_os("UPDATE_GOLDENFILES").is_some() {
            return self.write_json(&actual);
        }

        let expected = self.read_json()?;
        assert_eq!(
            expected,
            actual,
            "collected statistics trace JSON should match {}\nexpected:\n{}\nactual:\n{}",
            self.json_file,
            serde_json::to_string_pretty(&expected)?,
            serde_json::to_string_pretty(&actual)?,
        );
        Ok(())
    }

    fn read_sql(&self) -> Result<String> {
        self.read_fixture("sql", self.sql_file)
    }

    fn read_json(&self) -> Result<serde_json::Value> {
        let path = self.fixture_path("traces", self.json_file);
        let json = self.read_fixture("traces", self.json_file)?;
        serde_json::from_str(&json).map_err(|err| {
            ErrorCode::Internal(format!(
                "invalid statistics trace JSON fixture {}: {err}",
                path.display()
            ))
        })
    }

    fn write_json(&self, value: &serde_json::Value) -> Result<()> {
        let path = self.fixture_path("traces", self.json_file);
        let mut json = serde_json::to_string_pretty(value)?;
        json.push('\n');
        std::fs::write(&path, json).map_err(|err| {
            ErrorCode::Internal(format!(
                "failed to write statistics trace JSON fixture {}: {err}",
                path.display()
            ))
        })
    }

    fn read_fixture(&self, kind: &str, file: &str) -> Result<String> {
        let path = self.fixture_path(kind, file);
        std::fs::read_to_string(&path).map_err(|err| {
            ErrorCode::Internal(format!(
                "failed to read statistics trace {kind} fixture {}: {err}",
                path.display()
            ))
        })
    }

    fn fixture_path(&self, kind: &str, file: &str) -> PathBuf {
        TestSuite::optimizer_data_dir()
            .join("statistics_trace")
            .join(kind)
            .join(file)
    }
}

async fn collect_statistics_trace(ctx: &Arc<QueryContext>, sql: &str) -> Result<serde_json::Value> {
    let plan = raw_plan(ctx, sql).await?;
    let Plan::Query {
        metadata, s_expr, ..
    } = plan
    else {
        return Err(ErrorCode::Internal(
            "expected SELECT to produce a query plan",
        ));
    };

    let settings = ctx.get_settings();
    let opt_ctx = OptimizerContext::new(ctx.clone(), metadata).with_settings(&settings)?;
    let trace_collector = StatisticsTraceCollector::default();
    let mut optimizer =
        CollectStatisticsOptimizer::new(opt_ctx).with_trace_collector(trace_collector.clone());

    let _ = optimizer.optimize(&s_expr).await?;
    trace_collector
        .take()
        .ok_or_else(|| ErrorCode::Internal("statistics trace was not collected"))
}
