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

#[tokio::test(flavor = "multi_thread", worker_threads = 1)]
async fn test_decorrelated_window_ordering() -> Result<()> {
    let mut file = open_golden_file("optimizer", "decorrelate_window.txt")?;
    let cases = [
        SqlTestCase {
            name: "global_window_gains_partition_sort",
            description: "Decorrelation must sort by the new outer partition before SUM, not only before the LIMIT row_number window.",
            setup_sqls: &[],
            sql: "SELECT (SELECT sum(t1.a + t2.b) OVER () FROM (VALUES (10), (20)) t2(b) LIMIT 1) FROM (VALUES (1), (2)) t1(a)",
        },
        SqlTestCase {
            name: "partitioned_window_rebuilds_exchange",
            description: "An existing window partition exchange must include the outer correlation key.",
            setup_sqls: &[],
            sql: "SELECT (SELECT sum(t1.a + t2.b) OVER (PARTITION BY t2.k) FROM (VALUES (1, 10), (1, 20)) t2(k, b) LIMIT 1) FROM (VALUES (1), (2)) t1(a)",
        },
        SqlTestCase {
            name: "ordered_window_gains_partition_exchange",
            description: "An ORDER BY-only window gains an outer partition exchange while retaining DESC NULLS LAST.",
            setup_sqls: &[],
            sql: "SELECT (SELECT sum(t1.a + t2.b) OVER (ORDER BY t2.b DESC NULLS LAST ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) FROM (VALUES (10), (20), (NULL)) t2(b) ORDER BY t2.b DESC NULLS LAST LIMIT 1 OFFSET 1) FROM (VALUES (1), (2)) t1(a)",
        },
        SqlTestCase {
            name: "partitioned_ordered_window_rebuilds_sort",
            description: "Rebuild the window sort with both original and correlated partition keys, preserving the window ordering.",
            setup_sqls: &[],
            sql: "SELECT (SELECT sum(t1.a + t2.b) OVER (PARTITION BY t2.k ORDER BY t2.b DESC NULLS LAST ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) FROM (VALUES (1, 10), (1, 20), (2, 100), (2, 200)) t2(k, b) ORDER BY t2.k, t2.b DESC NULLS LAST LIMIT 1 OFFSET 1) FROM (VALUES (1), (2)) t1(a)",
        },
        SqlTestCase {
            name: "uncorrelated_window_keeps_global_input",
            description: "An uncorrelated global window needs no outer partition sort.",
            setup_sqls: &[],
            sql: "SELECT (SELECT sum(t2.b) OVER () FROM (VALUES (10), (20)) t2(b) LIMIT 1) FROM (VALUES (1), (2)) t1(a)",
        },
    ];

    for case in &cases {
        let ctx = setup_context(case).await?;
        ctx.set_cluster_node_num(1);
        let raw_plan = ctx.bind_sql(case.sql).await?;
        let optimized_plan = ctx.optimize_plan(raw_plan.clone()).await?;

        write_case_header(&mut file, case)?;
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
    }
    Ok(())
}
