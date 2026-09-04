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
async fn test_push_down_rank_limit_aggregate_outcomes() -> Result<()> {
    let mut file = open_golden_file("optimizer", "push_down_rank_limit_aggregate.txt")?;

    let cases = [
        SqlTestCase {
            name: "single_key_direct_scan_pushes_order",
            description: "A limited aggregate ordered by its only group key should push the same ordering into a direct scan.",
            setup_sqls: &[EVENTS_TABLE],
            sql: "SELECT category, count(*) FROM events GROUP BY category ORDER BY category LIMIT 3",
        },
        SqlTestCase {
            name: "single_key_filter_scan_pushes_descending_order",
            description: "A row-preserving Filter between the aggregate and scan should allow descending scan ordering to be pushed through.",
            setup_sqls: &[EVENTS_TABLE],
            sql: "SELECT category, count(*) FROM events WHERE value > 0 GROUP BY category ORDER BY category DESC LIMIT 3",
        },
        SqlTestCase {
            name: "single_key_eval_scalar_scan_pushes_order",
            description: "An EvalScalar that computes an aggregate argument should allow group-key ordering to reach the scan.",
            setup_sqls: &[EVENTS_TABLE],
            sql: "SELECT category, sum(value * value) FROM events GROUP BY category ORDER BY category LIMIT 3",
        },
        SqlTestCase {
            name: "multiple_group_keys_do_not_push_scan_order",
            description: "Rank limits with multiple group keys must not install scan ordering for the runtime single-key filter.",
            setup_sqls: &[EVENTS_TABLE],
            sql: "SELECT category, value, count(*) FROM events GROUP BY category, value ORDER BY category LIMIT 3",
        },
        SqlTestCase {
            name: "join_input_does_not_push_scan_order",
            description: "A row-changing Join below the aggregate must prevent rank-limit ordering from reaching either scan.",
            setup_sqls: &[EVENTS_TABLE, CATEGORIES_TABLE],
            sql: "SELECT events.category, count(*) FROM events JOIN categories ON events.category = categories.category GROUP BY events.category ORDER BY events.category LIMIT 3",
        },
        SqlTestCase {
            name: "preordered_scan_keeps_existing_order",
            description: "A scan already ordered by a subquery must keep that ordering instead of being overwritten by the outer aggregate rank limit.",
            setup_sqls: &[EVENTS_TABLE],
            sql: "SELECT category, count(*) FROM (SELECT category, value FROM events ORDER BY value) ordered_events GROUP BY category ORDER BY category LIMIT 3",
        },
    ];

    for case in &cases {
        write_optimized_case(&mut file, case).await?;
    }

    Ok(())
}

const EVENTS_TABLE: &str = "CREATE TABLE events(category INT, value INT)";
const CATEGORIES_TABLE: &str = "CREATE TABLE categories(category INT, name STRING)";
