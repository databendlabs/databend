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
use databend_common_sql::optimizer::ir::StatContext;

use crate::framework::golden::SqlTestCase;
use crate::framework::golden::open_golden_file;
use crate::framework::golden::setup_context;
use crate::framework::golden::write_case_header;

const SETUP_SQLS: &[&str] = &[
    "CREATE TABLE sa_outer(id INT, k INT NULL)",
    "CREATE TABLE sa_inner(k INT NULL, v INT)",
];

async fn write_optimized_case(file: &mut impl std::io::Write, case: &SqlTestCase) -> Result<()> {
    let ctx = setup_context(case).await?;
    ctx.set_cluster_node_num(1);

    let raw_plan = ctx.bind_sql(case.sql).await?;
    let optimized_plan = ctx.optimize_plan(raw_plan.clone()).await?;

    write_case_header(file, case)?;
    writeln!(file, "raw_plan:")?;
    writeln!(
        file,
        "{}",
        raw_plan.format_indent(Default::default(), &StatContext::default())?
    )?;
    writeln!(file, "optimized_plan:")?;
    writeln!(
        file,
        "{}",
        optimized_plan.format_indent(Default::default(), &StatContext::default())?
    )?;
    writeln!(file)?;

    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 1)]
async fn test_decorrelate_scalar_aggregate_subquery() -> Result<()> {
    let mut file = open_golden_file("optimizer", "decorrelate_scalar_aggregate.txt")?;

    let cases = [
        SqlTestCase {
            name: "count_plus_one_is_filled",
            description: "count(*) + 1 is evaluated above a LEFT join, after count is filled with 0 for outer values without input rows.",
            setup_sqls: SETUP_SQLS,
            sql: "SELECT o.id, (SELECT count(*) + 1 FROM sa_inner i WHERE i.k = o.k) FROM sa_outer o",
        },
        SqlTestCase {
            name: "having_rejection_keeps_outer_row",
            description: "A HAVING that rejects the aggregate row becomes a match flag; the subquery result is NULL instead of the outer row being removed.",
            setup_sqls: SETUP_SQLS,
            sql: "SELECT o.id, (SELECT count(*) FROM sa_inner i WHERE i.k = o.k HAVING count(*) = 0) FROM sa_outer o",
        },
        SqlTestCase {
            name: "filled_count_in_filter_becomes_inner_join",
            description: "A filter that rejects the filled value still lets the LEFT join be converted to an INNER join.",
            setup_sqls: SETUP_SQLS,
            sql: "SELECT o.id FROM sa_outer o WHERE (SELECT count(*) FROM sa_inner i WHERE i.k = o.k) > 0",
        },
        SqlTestCase {
            name: "null_on_empty_input_keeps_generic_plan",
            description: "0.2 * avg(..) is NULL on empty input, so the generic plan is already correct and is kept.",
            setup_sqls: SETUP_SQLS,
            sql: "SELECT o.id FROM sa_outer o WHERE o.id > (SELECT 0.2 * avg(v) FROM sa_inner i WHERE i.k = o.k)",
        },
        SqlTestCase {
            name: "limit_zero_keeps_generic_plan_without_count_fixup",
            description: "Under LIMIT 0 the subquery never returns a row, so the generic plan is kept and count is not fixed up to 0.",
            setup_sqls: SETUP_SQLS,
            sql: "SELECT o.id, (SELECT count(*) FROM sa_inner i WHERE i.k = o.k LIMIT 0) FROM sa_outer o",
        },
        SqlTestCase {
            name: "nested_correlated_scalar_aggregate_keeps_generic_plan",
            description: "A correlated scalar aggregate nested in the aggregate input isn't rewritten, because its groups also disappear for outer values without input.",
            setup_sqls: SETUP_SQLS,
            sql: "SELECT o.id, (SELECT count(*) FROM (SELECT count(*) c FROM sa_inner i WHERE i.k = o.k) s) FROM sa_outer o",
        },
    ];

    for case in &cases {
        write_optimized_case(&mut file, case).await?;
    }

    Ok(())
}
