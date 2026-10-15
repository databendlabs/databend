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

use databend_common_exception::Result;

use crate::framework::golden::SqlTestCase;
use crate::framework::golden::open_golden_file;
use crate::framework::golden::setup_context;
use crate::framework::golden::write_case_header;

async fn write_optimized_case(file: &mut impl std::io::Write, case: &SqlTestCase) -> Result<()> {
    let ctx = setup_context(case).await?;
    let raw_plan = ctx.bind_sql(case.sql).await?;
    let optimized_plan = ctx.optimize_plan(raw_plan).await?;

    write_case_header(file, case)?;
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
async fn test_push_down_filter_window_top_n_outcomes() -> Result<()> {
    let mut file = open_golden_file("optimizer", "push_down_filter_window_top_n.txt")?;

    let cases = [
        SqlTestCase {
            name: "single_ranking_window_pushes_top",
            description: "A lone ranking window filtered by QUALIFY should get the top-n.",
            setup_sqls: &[T_TABLE],
            sql: "SELECT k, row_number() OVER (PARTITION BY k ORDER BY v DESC) AS r FROM t QUALIFY r = 1",
        },
        SqlTestCase {
            name: "aggregate_after_top_window_blocks_top",
            description: "An aggregate window evaluated after the top-n window in the same group needs every row, so the top-n must not be pushed.",
            setup_sqls: &[T_TABLE],
            sql: "SELECT k, count(*) OVER (PARTITION BY k) AS c, row_number() OVER (PARTITION BY k ORDER BY v DESC) AS r FROM t QUALIFY r = 1",
        },
        SqlTestCase {
            name: "aggregate_before_top_window_pushes_top",
            description: "An aggregate window evaluated before the top-n window has already seen every row, so the top-n can be pushed.",
            setup_sqls: &[T_TABLE],
            sql: "SELECT k, sum(w) OVER (PARTITION BY k ORDER BY v, w) AS s, row_number() OVER (PARTITION BY k ORDER BY v DESC) AS r FROM t QUALIFY r = 1",
        },
        SqlTestCase {
            name: "ranking_different_order_blocks_top",
            description: "A ranking window with a different order needs rows outside the top-n, so the top-n must not be pushed.",
            setup_sqls: &[T_TABLE],
            sql: "SELECT k, row_number() OVER (PARTITION BY k ORDER BY v DESC) AS r, rank() OVER (PARTITION BY k ORDER BY w) AS rk FROM t QUALIFY r = 1",
        },
    ];

    for case in &cases {
        write_optimized_case(&mut file, case).await?;
    }

    Ok(())
}

const T_TABLE: &str = "CREATE TABLE t(k INT, v INT, w INT)";
