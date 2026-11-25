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

use databend_common_catalog::table_context::TableContextSettings;
use databend_common_exception::Result;

use crate::framework::golden::SqlTestCase;
use crate::framework::golden::open_golden_file;
use crate::framework::golden::setup_context;
use crate::framework::golden::write_case_header;

const TABLE: &str = "CREATE TABLE t(k INT NOT NULL, v INT)";

#[tokio::test(flavor = "multi_thread", worker_threads = 1)]
async fn test_simplify_outer_join() -> Result<()> {
    let mut file = open_golden_file("optimizer", "simplify_outer_join.txt")?;
    let cases = [
        SqlTestCase {
            name: "inner_on_through_alias",
            description: "The upper ON equality simplifies the inner LEFT JOIN through a projection, without adding NULL filters.",
            setup_sqls: &[TABLE],
            sql: "SELECT a.k, q.w FROM t a JOIN
    (SELECT c.k AS match_key, b.v AS w FROM t b LEFT JOIN t c ON b.k = c.k) q
    ON a.k = q.match_key",
        },
        SqlTestCase {
            name: "conversion_exposes_next_inner_on",
            description: "After simplifying the middle LEFT JOIN, its ON also constrains its formerly preserved input.",
            setup_sqls: &[TABLE],
            sql: "SELECT a.k, q.w FROM t a JOIN
    (SELECT c.k AS match_key, b.w FROM
        (SELECT x.v AS w, y.k AS match_key FROM t x LEFT JOIN t y ON x.k = y.k) b
        LEFT JOIN t c ON b.match_key = c.k) q
    ON a.k = q.match_key",
        },
        SqlTestCase {
            name: "where_equality_exposes_inner_before_inference",
            description: "A WHERE equality provides the same necessary non-NULL condition as ON.",
            setup_sqls: &[TABLE],
            sql: "SELECT a.k, q.w FROM t a CROSS JOIN
    (SELECT c.k AS match_key, b.v AS w FROM t b LEFT JOIN t c ON b.k = c.k) q
    WHERE a.k = q.match_key",
        },
        SqlTestCase {
            name: "full_reduces_to_left_from_filter",
            description: "Requiring the left key of FULL JOIN removes only right-unmatched rows.",
            setup_sqls: &[TABLE],
            sql: "SELECT q.lk, q.rk FROM
    (SELECT b.k AS lk, c.k AS rk FROM t b FULL JOIN t c ON b.k = c.k) q
    WHERE q.lk IS NOT NULL",
        },
        SqlTestCase {
            name: "union_maps_each_branch",
            description: "A requirement on a UNION ALL output reaches both branches using their own column bindings.",
            setup_sqls: &[TABLE],
            sql: "SELECT a.k, q.w FROM t a JOIN
    (SELECT c.k AS match_key, b.v AS w FROM t b LEFT JOIN t c ON b.k = c.k
     UNION ALL
     SELECT e.k AS match_key, d.v AS w FROM t d LEFT JOIN t e ON d.k = e.k) q
    ON a.k = q.match_key",
        },
        SqlTestCase {
            name: "outer_on_keeps_preserved_input",
            description: "An upper LEFT JOIN ON condition cannot reject NULL keys on its preserved input.",
            setup_sqls: &[TABLE],
            sql: "SELECT a.k, q.w FROM
    (SELECT c.k AS match_key, b.v AS w FROM t b LEFT JOIN t c ON b.k = c.k) q
    LEFT JOIN t a ON q.match_key = a.k",
        },
        SqlTestCase {
            name: "null_safe_on_does_not_reject_null",
            description: "The upper NULL-safe equality can still match a NULL-extended row.",
            setup_sqls: &[TABLE],
            sql: "SELECT a.v, q.w FROM t a JOIN
    (SELECT c.k AS match_key, b.v AS w FROM t b LEFT JOIN t c ON b.k = c.k) q
    ON a.v IS NOT DISTINCT FROM q.match_key",
        },
        SqlTestCase {
            name: "top_n_is_a_barrier",
            description: "Rejecting NULL after TopN must not remove rows before TopN chooses its candidates.",
            setup_sqls: &[TABLE],
            sql: "SELECT a.k, q.w FROM t a JOIN
    (SELECT c.k AS match_key, b.v AS w FROM t b LEFT JOIN t c ON b.k = c.k
     ORDER BY b.v LIMIT 2) q
    ON a.k = q.match_key",
        },
        SqlTestCase {
            name: "coalesce_may_match_a_null_extended_row",
            description: "A nullable input is not required when the projected join key replaces NULL by a value.",
            setup_sqls: &[TABLE],
            sql: "SELECT a.k, q.w FROM t a JOIN
    (SELECT coalesce(c.k, 0) AS match_key, b.v AS w FROM t b LEFT JOIN t c ON b.k = c.k) q
    ON a.k = q.match_key",
        },
        SqlTestCase {
            name: "row_dependent_projection_is_a_barrier",
            description: "Even when the join key is a direct alias, removing rows before another projection item could change its result.",
            setup_sqls: &[TABLE],
            sql: "SELECT a.k, q.d FROM t a JOIN
    (SELECT c.k AS match_key, running_difference(b.v) AS d FROM t b LEFT JOIN t c ON b.k = c.k) q
    ON a.k = q.match_key",
        },
        SqlTestCase {
            name: "window_partition_key",
            description: "A requirement on the partition key removes whole partitions and reaches the lower outer join through the unbounded sort.",
            setup_sqls: &[TABLE],
            sql: "SELECT q.match_key, q.rn FROM t a JOIN
    (SELECT c.k AS match_key, row_number() OVER (PARTITION BY c.k ORDER BY b.v) AS rn
     FROM t b LEFT JOIN t c ON b.k = c.k) q
    ON a.k = q.match_key",
        },
        SqlTestCase {
            name: "window_group_common_partition_key",
            description: "All windows in the group partition on the required key.",
            setup_sqls: &[TABLE],
            sql: "SELECT q.match_key, q.rn, q.rk FROM t a JOIN
    (SELECT c.k AS match_key, row_number() OVER (PARTITION BY c.k ORDER BY b.v) AS rn,
            rank() OVER (PARTITION BY c.k ORDER BY b.k) AS rk
     FROM t b LEFT JOIN t c ON b.k = c.k) q
    ON a.k = q.match_key",
        },
        SqlTestCase {
            name: "computed_window_partition_is_not_a_direct_key",
            description: "A dependency of the partition key is not itself a partition key. COALESCE puts unmatched rows in a partition with matching rows.",
            setup_sqls: &[TABLE],
            sql: "SELECT q.match_key, q.rn FROM t a JOIN
    (SELECT c.k AS match_key, row_number() OVER (PARTITION BY coalesce(c.k, 0) ORDER BY b.v) AS rn
     FROM t b LEFT JOIN t c ON b.k = c.k) q
    ON a.k = q.match_key",
        },
        SqlTestCase {
            name: "window_group_with_unpartitioned_window",
            description: "A key that partitions one window cannot be propagated through an unpartitioned window alongside it.",
            setup_sqls: &[TABLE],
            sql: "SELECT q.match_key, q.rn, q.grn FROM t a JOIN
    (SELECT c.k AS match_key, row_number() OVER (PARTITION BY c.k ORDER BY b.v) AS rn,
            row_number() OVER (ORDER BY b.k) AS grn
     FROM t b LEFT JOIN t c ON b.k = c.k) q
    ON a.k = q.match_key",
        },
        SqlTestCase {
            name: "aggregate_group_key",
            description: "A requirement on the ordinary group key removes an entire group without changing any surviving COUNT(*).",
            setup_sqls: &[TABLE],
            sql: "SELECT q.match_key, q.n FROM t a JOIN
    (SELECT c.k AS match_key, count(*) AS n FROM t b LEFT JOIN t c ON b.k = c.k GROUP BY c.k) q
    ON a.k = q.match_key",
        },
        SqlTestCase {
            name: "aggregate_result_is_not_an_input_requirement",
            description: "Rejecting NULL MAX does not let us remove individual inputs that contribute to COUNT(*).",
            setup_sqls: &[TABLE],
            sql: "SELECT q.g, q.n FROM t a JOIN
    (SELECT b.v AS g, max(c.k) AS match_key, count(*) AS n
     FROM t b LEFT JOIN t c ON b.k = c.k GROUP BY b.v) q
    ON a.k = q.match_key",
        },
        SqlTestCase {
            name: "grouping_sets_is_a_barrier",
            description: "The early pass does not propagate a grouping-set key into the input, where it may contribute to other sets.",
            setup_sqls: &[TABLE],
            sql: "SELECT q.match_key, q.n FROM t a JOIN
    (SELECT c.k AS match_key, count(*) AS n FROM t b LEFT JOIN t c ON b.k = c.k
     GROUP BY GROUPING SETS ((c.k), ())) q
    ON a.k = q.match_key",
        },
    ];

    for case in &cases {
        let ctx = setup_context(case).await?;
        let raw = ctx.bind_sql(case.sql).await?;
        write_case_header(&mut file, case)?;
        writeln!(file, "raw_plan:")?;
        writeln!(
            file,
            "{}",
            raw.format_indent(Default::default(), &ctx.stat_context()?)?
        )?;
        let optimized = ctx.optimize_plan(raw).await?;
        writeln!(file, "optimized_plan:")?;
        writeln!(
            file,
            "{}",
            optimized.format_indent(Default::default(), &ctx.stat_context()?)?
        )?;
        if matches!(
            case.name,
            "inner_on_through_alias"
                | "where_equality_exposes_inner_before_inference"
                | "window_partition_key"
                | "window_group_common_partition_key"
                | "aggregate_group_key"
        ) {
            ctx.get_settings()
                .set_optimizer_skip_list("SimplifyOuterJoinOptimizer".to_string())?;
            let control = ctx.optimize_plan(ctx.bind_sql(case.sql).await?).await?;
            writeln!(file, "optimized_without_early_pass:")?;
            writeln!(
                file,
                "{}",
                control.format_indent(Default::default(), &ctx.stat_context()?)?
            )?;
        }
        writeln!(file)?;
    }
    Ok(())
}
