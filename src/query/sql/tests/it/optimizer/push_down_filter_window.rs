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

use std::io::Write;

use databend_common_exception::Result;

use crate::framework::golden::SqlTestCase;
use crate::framework::golden::open_golden_file;
use crate::framework::golden::setup_context;
use crate::framework::golden::write_case_header;

const T_TABLE: &str = "CREATE TABLE t(k INT, v INT, w INT)";
const D_TABLE: &str = "CREATE TABLE d(k INT NOT NULL)";

#[tokio::test(flavor = "multi_thread", worker_threads = 1)]
async fn test_push_down_filter_window() -> Result<()> {
    let mut file = open_golden_file("optimizer", "push_down_filter_window.txt")?;

    let cases = [
        SqlTestCase {
            name: "direct_partition_key",
            description: "A predicate on the partition key removes whole partitions.",
            setup_sqls: &[T_TABLE],
            sql: "SELECT * FROM (SELECT k, v, row_number() OVER (PARTITION BY k ORDER BY v) AS rn FROM t) q WHERE q.k = 1",
        },
        SqlTestCase {
            name: "partition_expression",
            description: "A predicate on the partition expression has one value per partition.",
            setup_sqls: &[T_TABLE],
            sql: "SELECT * FROM (SELECT k, v, row_number() OVER (PARTITION BY k % 2 ORDER BY v) AS rn FROM t) q WHERE q.k % 2 = 1",
        },
        SqlTestCase {
            name: "partition_expression_through_column_alias",
            description: "The outer query reads k through an alias; the partition expression still matches by column.",
            setup_sqls: &[T_TABLE],
            sql: "SELECT * FROM (SELECT k AS kk, v, row_number() OVER (PARTITION BY k % 2 ORDER BY v) AS rn FROM t) q WHERE q.kk % 2 = 1",
        },
        SqlTestCase {
            name: "partition_expression_dependency",
            description: "Rows with different k share a k % 2 partition, so a predicate on k stays above the window.",
            setup_sqls: &[T_TABLE],
            sql: "SELECT * FROM (SELECT k, v, row_number() OVER (PARTITION BY k % 2 ORDER BY v) AS rn FROM t) q WHERE q.k = 3",
        },
        SqlTestCase {
            name: "partition_expression_with_other_column",
            description: "Combined with a non-partition column, the partition expression no longer has one value per partition.",
            setup_sqls: &[T_TABLE],
            sql: "SELECT * FROM (SELECT k, v, w, row_number() OVER (PARTITION BY k % 2 ORDER BY v) AS rn FROM t) q WHERE q.k % 2 + q.w = 1",
        },
        SqlTestCase {
            name: "partition_expression_and_dependency_conjuncts",
            description: "Each conjunct is checked separately: only the one on the partition expression moves.",
            setup_sqls: &[T_TABLE],
            sql: "SELECT * FROM (SELECT k, v, row_number() OVER (PARTITION BY k % 2 ORDER BY v) AS rn FROM t) q WHERE q.k % 2 = 1 AND q.k = 3",
        },
        SqlTestCase {
            name: "partition_expression_also_order_key",
            description: "The existing restriction on order columns still applies when the partition expression reads one.",
            setup_sqls: &[T_TABLE],
            sql: "SELECT * FROM (SELECT k, v, row_number() OVER (PARTITION BY k % 2 ORDER BY k) AS rn FROM t) q WHERE q.k % 2 = 1",
        },
        SqlTestCase {
            name: "partition_expression_also_argument",
            description: "The existing restriction on window function arguments still applies when the partition expression reads one.",
            setup_sqls: &[T_TABLE],
            sql: "SELECT * FROM (SELECT k, v, sum(k) OVER (PARTITION BY k % 2 ORDER BY v) AS s FROM t) q WHERE q.k % 2 = 1",
        },
        SqlTestCase {
            name: "partition_key_below_top_n",
            description: "A filter on the partition key reaches the scan below the per-partition top-n of QUALIFY.",
            setup_sqls: &[T_TABLE],
            sql: "SELECT * FROM (SELECT k, v, row_number() OVER (PARTITION BY k ORDER BY v) AS rn FROM t QUALIFY rn = 1) q WHERE q.k = 1",
        },
        SqlTestCase {
            name: "partition_expression_below_top_n",
            description: "A filter on the partition expression reaches the scan below the per-partition top-n of QUALIFY.",
            setup_sqls: &[T_TABLE],
            sql: "SELECT * FROM (SELECT k, v, row_number() OVER (PARTITION BY k % 2 ORDER BY v) AS rn FROM t QUALIFY rn = 1) q WHERE q.k % 2 = 1",
        },
        SqlTestCase {
            name: "partition_expression_dependency_above_top_n",
            description: "A filter on k could change which row is first in a k % 2 partition, so it stays above the window and its top-n.",
            setup_sqls: &[T_TABLE],
            sql: "SELECT * FROM (SELECT k, v, row_number() OVER (PARTITION BY k % 2 ORDER BY v) AS rn FROM t QUALIFY rn = 1) q WHERE q.k = 3",
        },
        SqlTestCase {
            name: "runtime_filter_below_top_n",
            description: "Runtime filters are planned after QUALIFY sets the per-partition top-n. A runtime filter on the partition key removes whole partitions, so it passes the top-n sort.",
            setup_sqls: &[T_TABLE, D_TABLE],
            sql: "SELECT q.v FROM (SELECT k, v, row_number() OVER (PARTITION BY k ORDER BY v DESC) AS rn FROM t QUALIFY rn = 1) q JOIN d ON q.k = d.k",
        },
        SqlTestCase {
            name: "window_group_shared_partition_expression",
            description: "The predicate has one value per partition of every window in the group, and moves as written below the group.",
            setup_sqls: &[T_TABLE],
            sql: "SELECT * FROM (SELECT k, v, row_number() OVER (PARTITION BY k % 2 ORDER BY v) AS rn, rank() OVER (PARTITION BY k % 2 ORDER BY v) AS rk FROM t) q WHERE q.k % 2 = 1",
        },
        SqlTestCase {
            name: "window_group_shared_partition_key_and_expression",
            description: "Both windows partition by k and k % 2, so predicates on either have one value per partition of the group.",
            setup_sqls: &[T_TABLE],
            sql: "SELECT * FROM (SELECT k, v, row_number() OVER (PARTITION BY k, k % 2 ORDER BY v) AS rn, rank() OVER (PARTITION BY k, k % 2 ORDER BY v) AS rk FROM t) q WHERE q.k = 1 AND q.k % 2 = 1",
        },
        SqlTestCase {
            name: "window_group_stateful_scalar",
            description: "The group evaluates running_difference over its input rows, so even a predicate on the shared partition expression stays above it.",
            setup_sqls: &[T_TABLE],
            sql: "SELECT * FROM (SELECT k, sum(running_difference(v)) OVER (PARTITION BY k % 2) AS s, count(*) OVER (PARTITION BY k % 2) AS n FROM t) q WHERE q.k % 2 = 1",
        },
        SqlTestCase {
            name: "stacked_windows_different_partition_expressions",
            description: "A predicate on the lower window's partition expression can split the partitions of the upper window, so it stays above both.",
            setup_sqls: &[T_TABLE],
            sql: "SELECT * FROM (SELECT k, v, row_number() OVER (PARTITION BY k % 2 ORDER BY v) AS rn, row_number() OVER (PARTITION BY k % 3 ORDER BY v) AS rn3 FROM t) q WHERE q.k % 2 = 1",
        },
    ];

    for case in &cases {
        let ctx = setup_context(case).await?;
        let raw_plan = ctx.bind_sql(case.sql).await?;
        let optimized_plan = ctx.optimize_plan(raw_plan).await?;
        write_case_header(&mut file, case)?;
        writeln!(file, "optimized_plan:")?;
        writeln!(
            file,
            "{}",
            optimized_plan.format_indent(Default::default(), &ctx.stat_context()?)?
        )?;
        writeln!(file)?;
    }

    Ok(())
}
