// Copyright 2026 Datafuse Labs
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

use databend_common_exception::Result;
use databend_common_sql::Planner;
use databend_common_sql::plans::JoinType;
use databend_common_sql::plans::Plan;
use databend_query::physical_plans::AggregateFinal;
use databend_query::physical_plans::AggregatePartial;
use databend_query::physical_plans::HashJoin;
use databend_query::physical_plans::PhysicalPlan;
use databend_query::physical_plans::PhysicalPlanBuilder;
use databend_query::sessions::TableContextSettings;
use databend_query::test_kits::TestFixture;

// A SQL result or EXPLAIN's bloom flag cannot detect missing statistics: the
// runtime selectivity gate also needs build_table_rows. Bind and build real SQL
// and inspect that denominator, using distinct table sizes to identify its source.
#[tokio::test(flavor = "multi_thread")]
async fn test_runtime_filter_min_max_build_statistics() -> Result<()> {
    let fixture = TestFixture::setup().await?;
    for sql in [
        "CREATE TABLE rf_stats_probe (k UInt64) ENGINE=FUSE",
        "CREATE TABLE rf_stats_build (g UInt64, k UInt64 NULL, v UInt64 NOT NULL) ENGINE=FUSE",
        "CREATE TABLE rf_stats_other (g UInt64) ENGINE=FUSE",
        "INSERT INTO rf_stats_probe SELECT number FROM numbers(101)",
        "INSERT INTO rf_stats_build SELECT number % 3, \
         if(number % 4 = 0, NULL, number), number FROM numbers(17)",
        "INSERT INTO rf_stats_other SELECT number FROM numbers(5)",
    ] {
        fixture.execute_command(sql).await?;
    }

    let cases = [
        (
            "existing base column",
            "SELECT k FROM rf_stats_build WHERE g = 1",
            Some(17),
            None,
        ),
        (
            "min nullable input",
            "SELECT g, MIN(k) AS k FROM rf_stats_build GROUP BY g",
            Some(17),
            Some("min"),
        ),
        (
            "max non-nullable input",
            "SELECT g, MAX(v) AS k FROM rf_stats_build GROUP BY g",
            Some(17),
            Some("max"),
        ),
        (
            "not min/max",
            "SELECT g, SUM(v) AS k FROM rf_stats_build GROUP BY g",
            None,
            None,
        ),
        (
            "computed argument",
            "SELECT g, MIN(k + 1) AS k FROM rf_stats_build GROUP BY g",
            None,
            None,
        ),
        (
            "multiple input tables",
            "SELECT b.g, MIN(b.k) AS k FROM rf_stats_build b \
             INNER JOIN rf_stats_other o ON b.g = o.g GROUP BY b.g",
            None,
            None,
        ),
        (
            "computed result",
            "SELECT g, MIN(k) + 1 AS k FROM rf_stats_build GROUP BY g",
            None,
            None,
        ),
    ];

    for (name, build_sql, expected_rows, min_max) in cases {
        let ctx = fixture.new_query_ctx().await?;
        let settings = ctx.get_settings();
        // Fix only the join direction: this test inspects the build table's
        // statistics, not the optimizer's choice of which side to build.
        settings.set_setting("disable_join_reorder".to_string(), "1".to_string())?;
        let sql =
            format!("SELECT p.k FROM rf_stats_probe p LEFT SEMI JOIN ({build_sql}) b ON p.k = b.k");
        let (plan, _) = Planner::new(ctx.clone()).plan_sql(&sql).await?;
        let Plan::Query {
            s_expr,
            metadata,
            bind_context,
            ..
        } = plan
        else {
            panic!("{name}: expected query plan");
        };
        let physical = PhysicalPlanBuilder::new(metadata, ctx, false)
            .build_query(s_expr.planned()?, bind_context.column_set())
            .await?;
        let join = find_plan::<HashJoin>(&physical).expect("expected outer hash join");
        assert_eq!(join.join_type, JoinType::LeftSemi, "{name}");
        assert_eq!(join.runtime_filter.filters.len(), 1, "{name}");
        if let Some(function) = min_max {
            let final_ =
                find_plan::<AggregateFinal>(&join.build).expect("expected final aggregate");
            let partial =
                find_plan::<AggregatePartial>(&final_.input).expect("expected partial aggregate");
            let final_func = &final_.agg_funcs[0];
            let partial_func = &partial.agg_funcs[0];
            assert_eq!(final_func.sig.name, function, "{name}");
            // Main's Final still names the original argument, not a state column.
            assert_eq!(final_func.arg_indices, partial_func.arg_indices, "{name}");
            assert_ne!(final_func.arg_indices, vec![partial_func.output_column]);
            assert_eq!(
                final_func.sig.return_type.remove_nullable(),
                final_func.sig.args[0].remove_nullable(),
                "{name}: MIN/MAX may add nullability, not change the value type"
            );
        }
        assert_eq!(
            join.runtime_filter.filters[0].build_table_rows, expected_rows,
            "{name}: bloom selectivity needs statistics from rf_stats_build, not probe/other"
        );
    }
    Ok(())
}

fn find_plan<T: 'static>(plan: &PhysicalPlan) -> Option<&T> {
    plan.as_any()
        .downcast_ref::<T>()
        .or_else(|| plan.children().find_map(find_plan::<T>))
}
