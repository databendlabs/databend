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
async fn test_outer_join_to_anti_optimizer_outcomes() -> Result<()> {
    let mut file = open_golden_file("optimizer", "outer_join_to_anti.txt")?;

    let cases = [
        SqlTestCase {
            name: "regular_right_key_becomes_left_anti",
            description: "A NULL filter on a regular right equi-key should become a left anti join.",
            setup_sqls: &[LEFT_TABLE, RIGHT_TABLE],
            sql: "SELECT l.k
FROM outer_to_anti_left AS l
LEFT JOIN outer_to_anti_right AS r ON l.k = r.k
WHERE r.k IS NULL",
        },
        SqlTestCase {
            name: "right_outputs_are_reconstructed_as_nulls",
            description: "Right outputs observed above the rewrite should remain available as typed NULL expressions.",
            setup_sqls: &[LEFT_TABLE, RIGHT_TABLE],
            sql: "SELECT l.k, r.k, r.payload
FROM outer_to_anti_left AS l
LEFT JOIN outer_to_anti_right AS r ON l.k = r.k
WHERE r.k IS NULL",
        },
        SqlTestCase {
            name: "remaining_filter_and_limit_optimize_through_reconstruction",
            description: "Multiple equi-conditions, a remaining left filter, and LIMIT should continue optimizing around the NULL reconstruction.",
            setup_sqls: &[LEFT_TABLE, RIGHT_TABLE],
            sql: "SELECT l.k, r.payload
FROM outer_to_anti_left AS l
LEFT JOIN outer_to_anti_right AS r ON l.k = r.k AND l.item = r.item
WHERE r.k IS NULL AND l.keep > 0
LIMIT 10",
        },
        SqlTestCase {
            name: "nullable_payload_test_keeps_left_outer",
            description: "A nullable non-key payload can be NULL on matched rows, so it must not become an anti join.",
            setup_sqls: &[LEFT_TABLE, RIGHT_TABLE],
            sql: "SELECT l.k, r.payload
FROM outer_to_anti_left AS l
LEFT JOIN outer_to_anti_right AS r ON l.k = r.k
WHERE r.payload IS NULL",
        },
        SqlTestCase {
            name: "right_outer_exclusion_becomes_anti",
            description: "A NULL filter on a regular left equi-key should become an anti join; later join commutation may canonicalize it to a left anti join with swapped inputs.",
            setup_sqls: &[LEFT_TABLE, RIGHT_TABLE],
            sql: "SELECT r.k
FROM outer_to_anti_left AS l
RIGHT JOIN outer_to_anti_right AS r ON l.k = r.k
WHERE l.k IS NULL",
        },
        SqlTestCase {
            name: "left_outputs_are_reconstructed_as_nulls",
            description: "Left outputs observed above a right anti rewrite should remain available as typed NULL expressions.",
            setup_sqls: &[LEFT_TABLE, RIGHT_TABLE],
            sql: "SELECT l.k, l.keep, r.k
FROM outer_to_anti_left AS l
RIGHT JOIN outer_to_anti_right AS r ON l.k = r.k
WHERE l.k IS NULL",
        },
        SqlTestCase {
            name: "right_filter_and_limit_optimize_through_reconstruction",
            description: "A remaining right filter and LIMIT should continue optimizing around left-side NULL reconstruction.",
            setup_sqls: &[LEFT_TABLE, RIGHT_TABLE],
            sql: "SELECT l.keep, r.k
FROM outer_to_anti_left AS l
RIGHT JOIN outer_to_anti_right AS r ON l.k = r.k AND l.item = r.item
WHERE l.k IS NULL AND r.payload > 0
LIMIT 10",
        },
        SqlTestCase {
            name: "nullable_left_payload_test_keeps_outer_join",
            description: "A nullable non-key left payload can be NULL on matched rows, so the right outer join must remain an outer join even if later canonicalized with swapped inputs.",
            setup_sqls: &[LEFT_TABLE, RIGHT_TABLE],
            sql: "SELECT l.keep, r.k
FROM outer_to_anti_left AS l
RIGHT JOIN outer_to_anti_right AS r ON l.k = r.k
WHERE l.keep IS NULL",
        },
        SqlTestCase {
            name: "null_safe_condition_keeps_left_outer",
            description: "A null-safe condition is not a regular equi-key in the raw plan and must remain a left outer join.",
            setup_sqls: &[LEFT_TABLE, RIGHT_TABLE],
            sql: "SELECT l.k, r.k
FROM outer_to_anti_left AS l
LEFT JOIN outer_to_anti_right AS r ON l.k IS NOT DISTINCT FROM r.k
WHERE r.k IS NULL",
        },
    ];

    for case in &cases {
        write_optimized_case(&mut file, case).await?;
    }

    Ok(())
}

const LEFT_TABLE: &str = "CREATE TABLE outer_to_anti_left
(
    k INTEGER,
    item INTEGER,
    keep INTEGER
)";

const RIGHT_TABLE: &str = "CREATE TABLE outer_to_anti_right
(
    k INTEGER,
    item INTEGER,
    payload INTEGER
)";
