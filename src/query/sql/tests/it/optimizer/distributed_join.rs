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

use std::collections::HashMap;

use databend_common_catalog::table_context::TableContextSettings;
use databend_common_exception::Result;

use super::table_statistics;
use crate::framework::LiteTableContext;
use crate::framework::golden::SqlTestCase;
use crate::framework::golden::open_golden_file;
use crate::framework::golden::write_case_header;

async fn write_distributed_case(
    file: &mut impl std::io::Write,
    case: &SqlTestCase,
    probe_rows: u64,
    build_rows: u64,
) -> Result<()> {
    let ctx = LiteTableContext::create().await?;
    ctx.set_cluster_node_num(2);
    ctx.set_table_warehouse_distribution(true);
    let settings = ctx.get_settings();
    settings.set_setting("disable_join_reorder".to_string(), "1".to_string())?;
    ctx.register_table_sql_with_stats(
        BIG_TABLE,
        Some(table_statistics(probe_rows)),
        HashMap::new(),
        HashMap::new(),
    )
    .await?;
    ctx.register_table_sql_with_stats(
        SMALL_TABLE,
        Some(table_statistics(build_rows)),
        HashMap::new(),
        HashMap::new(),
    )
    .await?;

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
async fn test_serial_build_distribution() -> Result<()> {
    let mut file = open_golden_file("optimizer", "distributed_join.txt")?;
    let cases = [
        (
            SqlTestCase {
                name: "small_serial_build_is_broadcast",
                description: "A small scalar aggregate on the build side should be broadcast without merging the distributed probe.",
                setup_sqls: &[],
                sql: "SELECT b.k, s.max_k
FROM big_table AS b
LEFT JOIN (SELECT max(k) AS max_k FROM small_table) AS s
ON b.k = s.max_k",
            },
            227_000_000,
            24,
        ),
        (
            SqlTestCase {
                name: "cross_join_small_serial_build_is_broadcast",
                description: "A small scalar aggregate should also be broadcast for a cross join without merging the distributed probe.",
                setup_sqls: &[],
                sql: "SELECT b.k, s.max_k
FROM big_table AS b
CROSS JOIN (SELECT max(k) AS max_k FROM small_table) AS s",
            },
            227_000_000,
            24,
        ),
        (
            SqlTestCase {
                name: "large_serial_build_remains_serial",
                description: "A Serial build that is not smaller than the probe should not be broadcast unconditionally.",
                setup_sqls: &[],
                sql: "SELECT b.k, s.k
FROM big_table AS b
LEFT JOIN (
    SELECT k, row_number() OVER () AS row_num
    FROM small_table
) AS s
ON b.k = s.k",
            },
            1_000,
            1_000,
        ),
    ];

    for (case, probe_rows, build_rows) in &cases {
        write_distributed_case(&mut file, case, *probe_rows, *build_rows).await?;
    }

    Ok(())
}

const BIG_TABLE: &str = "CREATE TABLE big_table (k BIGINT)";
const SMALL_TABLE: &str = "CREATE TABLE small_table (k BIGINT)";
