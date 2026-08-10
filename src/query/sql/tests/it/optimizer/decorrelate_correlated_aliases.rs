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
    ctx.set_cluster_node_num(1);

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
async fn test_decorrelate_correlated_alias_regressions() -> Result<()> {
    let mut file = open_golden_file("optimizer", "decorrelate_correlated_aliases.txt")?;

    let cases = [
        SqlTestCase {
            name: "nested_filter_alias_reaches_limit_rewrite",
            description: "Filter-derived correlated aliases must remain visible while rewriting a deeper correlated LIMIT subtree.",
            setup_sqls: &[],
            sql: r#"
        SELECT *
        FROM (VALUES (1, 1)) AS t1(a, b)
        WHERE EXISTS (
            SELECT 1
            FROM (
                SELECT t2.a
                FROM (VALUES (1, 1)) AS t2(a, b)
                WHERE t2.b = t1.b
                LIMIT 1
            ) AS s
            WHERE s.a = t1.a
        )
    "#,
        },
        SqlTestCase {
            name: "nested_filter_alias_survives_deeper_join_rewrite",
            description: "A deeper join remap must override a stale filter-local alias instead of collapsing the correlated predicate into a self-equality.",
            setup_sqls: &[],
            sql: r#"
        SELECT *
        FROM (VALUES (1, 1)) AS t1(a, b)
        WHERE EXISTS (
            SELECT 1
            FROM (
                SELECT t2.a
                FROM (VALUES (1, 1)) AS t2(a, b)
                JOIN (VALUES (1)) AS t3(c)
                  ON t2.a = t1.a
                WHERE t2.b = t1.b
            ) AS s
            WHERE s.a = t1.a
        )
    "#,
        },
    ];

    for case in &cases {
        write_optimized_case(&mut file, case).await?;
    }

    Ok(())
}
