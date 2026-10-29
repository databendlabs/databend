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

use databend_common_exception::Result;

use super::table_statistics;
use crate::framework::LiteTableContext;
use crate::framework::golden::open_golden_file;
use crate::framework::golden::write_case_title;

struct RuntimeFilterCase {
    name: &'static str,
    description: &'static str,
    sql: &'static str,
}

/// Large probe-side tables and a small build-side table, so the join order and the
/// build sides are stable.
const TABLES: [(&str, u64); 4] = [
    (
        "CREATE TABLE rf_fact (k INT NOT NULL, v INT NOT NULL, s VARCHAR NOT NULL)",
        1_000_000,
    ),
    (
        "CREATE TABLE rf_fact2 (k INT NOT NULL, v INT NOT NULL)",
        1_000_000,
    ),
    (
        "CREATE TABLE rf_mid (k INT NOT NULL, j INT NOT NULL)",
        100_000,
    ),
    (
        "CREATE TABLE rf_dim (k INT NOT NULL, name VARCHAR NOT NULL)",
        100,
    ),
];

async fn setup_context() -> Result<Arc<LiteTableContext>> {
    let ctx = LiteTableContext::create().await?;
    for (ddl, rows) in TABLES {
        ctx.register_table_sql_with_stats(
            ddl,
            Some(table_statistics(rows)),
            HashMap::new(),
            HashMap::new(),
        )
        .await?;
    }
    Ok(ctx)
}

async fn write_case(file: &mut impl std::io::Write, case: &RuntimeFilterCase) -> Result<()> {
    let ctx = setup_context().await?;
    let raw_plan = ctx.bind_sql(case.sql).await?;
    let optimized_plan = ctx.optimize_plan(raw_plan.clone()).await?;

    write_case_title(file, case.name, case.description)?;
    writeln!(file, "sql: {}", case.sql)?;
    writeln!(file, "raw_plan:")?;
    writeln!(
        file,
        "{}",
        raw_plan.format_indent(Default::default(), &ctx.stat_context()?)?
    )?;
    writeln!(file, "optimized_plan:")?;
    writeln!(
        file,
        "{}",
        optimized_plan.format_indent(Default::default(), &ctx.stat_context()?)?
    )?;
    writeln!(file)?;
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 1)]
async fn test_runtime_filter_push_down() -> Result<()> {
    let mut file = open_golden_file("optimizer", "runtime_filter.txt")?;

    let cases = [
        RuntimeFilterCase {
            name: "inner_join_column_key",
            description: "The join records one runtime filter and the probe-side scan receives it.",
            sql: "SELECT f.v FROM rf_fact f JOIN rf_dim d ON f.k = d.k WHERE d.name = 'a'",
        },
        RuntimeFilterCase {
            name: "expression_probe_key",
            description: "A single-column expression probe key reaches the scan unchanged.",
            sql: "SELECT f.v FROM rf_fact f JOIN rf_dim d ON f.k + 1 = d.k",
        },
        RuntimeFilterCase {
            name: "derived_column_probe_key",
            description: "A probe key computed by a projection is rewritten to its definition at the scan.",
            sql: "SELECT f.v FROM (SELECT k + 1 AS k1, v FROM rf_fact) f JOIN rf_dim d ON f.k1 = d.k",
        },
        RuntimeFilterCase {
            name: "union_all_probe_side",
            description: "The runtime filter is copied to both inputs of UNION ALL.",
            sql: "SELECT f.v FROM (SELECT k, v FROM rf_fact UNION ALL SELECT k, v FROM rf_fact2) f
JOIN rf_dim d ON f.k = d.k",
        },
        RuntimeFilterCase {
            name: "aggregate_group_key",
            description: "A runtime filter on a group key passes the aggregate.",
            sql: "SELECT f.s FROM (SELECT k, sum(v) AS s FROM rf_fact GROUP BY k) f
JOIN rf_dim d ON f.k = d.k",
        },
        RuntimeFilterCase {
            name: "aggregate_result_is_not_pushed",
            description: "A runtime filter on an aggregate result stops at the aggregate and is removed.",
            sql: "SELECT f.k FROM (SELECT k, sum(v) AS s FROM rf_fact GROUP BY k) f
JOIN rf_dim d ON f.s = d.k",
        },
        RuntimeFilterCase {
            name: "window_partition_key",
            description: "A runtime filter on a window partition key passes the window.",
            sql: "SELECT f.rn FROM (SELECT k, row_number() OVER (PARTITION BY k ORDER BY v) AS rn FROM rf_fact) f
JOIN rf_dim d ON f.k = d.k",
        },
        RuntimeFilterCase {
            name: "window_without_partition_is_not_pushed",
            description: "Removing rows below a window without that partition key changes the window results, so the filter is removed.",
            sql: "SELECT f.rn FROM (SELECT k, row_number() OVER (ORDER BY v) AS rn FROM rf_fact) f
JOIN rf_dim d ON f.k = d.k",
        },
        RuntimeFilterCase {
            name: "limit_is_not_pushed",
            description: "Removing rows below ORDER BY ... LIMIT changes which rows the limit keeps, so the filter is removed.",
            sql: "SELECT f.v FROM (SELECT k, v FROM rf_fact ORDER BY v LIMIT 1000000) f
JOIN rf_dim d ON f.k = d.k",
        },
        RuntimeFilterCase {
            name: "equivalence_across_inner_joins",
            description: "Join equivalences carry the runtime filter of the top join to every scan with an equal key.",
            sql: "SELECT f.v FROM rf_fact f JOIN rf_mid m ON f.k = m.k JOIN rf_dim d ON m.k = d.k",
        },
        RuntimeFilterCase {
            name: "null_supplying_side_of_left_join",
            description: "A NULL-rejecting key passes into the null-supplying side of a lower LEFT JOIN.",
            sql: "SELECT m.j FROM rf_mid m LEFT JOIN rf_fact f ON m.j = f.v JOIN rf_dim d ON f.k = d.k",
        },
        RuntimeFilterCase {
            name: "null_accepting_key_stays_above_left_join",
            description: "COALESCE maps the NULL-extended rows to a matchable key, so the filter must not enter the null-supplying side.",
            sql: "SELECT m.j FROM rf_mid m LEFT JOIN rf_fact f ON m.j = f.v JOIN rf_dim d ON coalesce(f.k, 0) = d.k",
        },
        RuntimeFilterCase {
            name: "null_safe_equality_has_no_runtime_filter",
            description: "A null-safe equality matches NULL keys, which a runtime filter would remove.",
            sql: "SELECT f.v FROM rf_fact f JOIN rf_dim d ON f.k IS NOT DISTINCT FROM d.k",
        },
    ];

    for case in &cases {
        write_case(&mut file, case).await?;
    }
    Ok(())
}
