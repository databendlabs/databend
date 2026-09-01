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
    let optimized_plan = ctx.optimize_plan(raw_plan.clone()).await?;

    write_case_header(file, case)?;
    writeln!(file, "raw_plan:")?;
    writeln!(file, "{}", raw_plan.format_indent(Default::default())?)?;
    writeln!(file, "optimized_plan:")?;
    writeln!(
        file,
        "{}",
        optimized_plan.format_indent(Default::default())?
    )?;
    writeln!(file)?;

    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 1)]
async fn test_planning_context_optimizer_outcomes() -> Result<()> {
    let mut file = open_golden_file("optimizer", "planning_context.txt")?;
    let setup_sqls = &[LEFT_ROWS_TABLE, RIGHT_ROWS_TABLE];
    let cases = [
        SqlTestCase {
            name: "issue_reproduction_keeps_left_join",
            description: "The statement-dependent OR predicate must keep null-extended rows and the left join.",
            setup_sqls: &[],
            sql: "SELECT l.id
FROM (SELECT 1 AS id UNION ALL SELECT 2 AS id) AS l
LEFT JOIN (SELECT 1 AS id) AS r ON l.id = r.id
WHERE r.id = 1
   OR now() > TIMESTAMP '2020-01-01 00:00:00'
ORDER BY l.id",
        },
        SqlTestCase {
            name: "immutable_null_rejecting_predicate_uses_inner_join",
            description: "A context-independent null-rejecting predicate should still convert the outer join.",
            setup_sqls,
            sql: "SELECT l.id
FROM planning_left_rows AS l
LEFT JOIN planning_right_rows AS r ON l.id = r.id
WHERE r.id > 0",
        },
    ];

    for case in &cases {
        write_optimized_case(&mut file, case).await?;
    }

    Ok(())
}

const LEFT_ROWS_TABLE: &str = "CREATE TABLE planning_left_rows(id UInt64)";
const RIGHT_ROWS_TABLE: &str =
    "CREATE TABLE planning_right_rows(id UInt64, ts Nullable(Timestamp))";
