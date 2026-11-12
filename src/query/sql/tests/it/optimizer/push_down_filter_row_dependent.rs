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
async fn test_filter_push_down_row_dependent_barriers() -> Result<()> {
    let mut file = open_golden_file("optimizer", "push_down_filter_row_dependent.txt")?;

    let cases = [
        SqlTestCase {
            name: "filter_stays_above_running_difference_projection",
            description: "running_difference depends on the preceding rows, so a filter on another column stays above its projection.",
            setup_sqls: &[T_TABLE],
            sql: "SELECT * FROM (SELECT k, running_difference(v) AS d FROM t) s WHERE k = 1",
        },
        SqlTestCase {
            name: "filter_passes_row_local_volatile_projection",
            description: "rand() is evaluated per row, so a filter on another column still passes its projection.",
            setup_sqls: &[T_TABLE],
            sql: "SELECT * FROM (SELECT k, rand() AS r FROM t) s WHERE k = 1",
        },
        SqlTestCase {
            name: "filter_stays_above_running_difference_join_condition",
            description: "The join evaluates running_difference on its input, so a filter on that input stays above the join.",
            setup_sqls: &[T_TABLE, U_TABLE],
            sql: "SELECT * FROM t JOIN u ON running_difference(t.v) = u.v WHERE t.k = 1",
        },
        SqlTestCase {
            name: "filter_stays_above_running_difference_filter",
            description: "A filter stays above a filter whose predicate depends on the preceding rows.",
            setup_sqls: &[T_TABLE],
            sql: "SELECT * FROM (SELECT k, v FROM t WHERE running_difference(v) > 0) s WHERE k = 1",
        },
        SqlTestCase {
            name: "filter_stays_out_of_converted_single_side",
            description: "A filter on the scalar subquery result stays above the converted single join, so it cannot hide a second match. The outer filter and its inferred copy are still pushed down.",
            setup_sqls: &[T_TABLE, U_TABLE],
            sql: "SELECT * FROM (SELECT t.k, (SELECT u.v FROM u WHERE u.k = t.k) AS sv FROM t) s WHERE s.sv IS NOT NULL AND s.k = 1",
        },
    ];

    for case in &cases {
        write_optimized_case(&mut file, case).await?;
    }

    Ok(())
}

const T_TABLE: &str = "CREATE TABLE t(k INT NOT NULL, v BIGINT NOT NULL)";
const U_TABLE: &str = "CREATE TABLE u(k INT NOT NULL, v BIGINT NOT NULL)";
