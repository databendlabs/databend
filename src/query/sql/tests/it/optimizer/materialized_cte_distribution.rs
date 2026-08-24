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

use databend_common_catalog::table_context::TableContextSettings;
use databend_common_exception::Result;

use crate::framework::LiteTableContext;
use crate::framework::golden::SqlTestCase;
use crate::framework::golden::open_golden_file;
use crate::framework::golden::write_case_header;

async fn write_optimized_case(
    file: &mut impl std::io::Write,
    case: &SqlTestCase,
    grouping_sets_to_union: bool,
) -> Result<()> {
    let ctx = LiteTableContext::create().await?;
    ctx.set_table_warehouse_distribution(true);
    ctx.set_cluster_node_num(3);
    for setup_sql in case.setup_sqls {
        ctx.register_setup_sql(setup_sql).await?;
    }
    if grouping_sets_to_union {
        ctx.get_settings()
            .set_setting("grouping_sets_to_union".to_string(), "1".to_string())?;
    }

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
async fn test_materialized_cte_distribution_optimizer_outcomes() -> Result<()> {
    let mut file = open_golden_file("optimizer", "materialized_cte_distribution.txt")?;

    let merge_backed = SqlTestCase {
        name: "merge_backed_scalar_producer_is_redistributed",
        description: "A scalar grouping-set MCTE backed by Merge should be redistributed after the final aggregate.",
        setup_sqls: &[MCTE_INPUT_TABLE],
        sql: "SELECT a, b, sum(v)
FROM mcte_input
GROUP BY ROLLUP(a, b)",
    };
    write_optimized_case(&mut file, &merge_backed, true).await?;

    let dummy_scan = SqlTestCase {
        name: "dummy_scan_serial_producer_is_not_redistributed",
        description: "Seriality from DummyTableScan is not evidence of a Merge-backed MCTE producer.",
        setup_sqls: &[],
        sql: "WITH c AS (SELECT 1 AS x)
SELECT * FROM c
UNION ALL
SELECT * FROM c",
    };
    write_optimized_case(&mut file, &dummy_scan, false).await?;

    Ok(())
}

const MCTE_INPUT_TABLE: &str = "CREATE TABLE mcte_input
(
    a INTEGER,
    b INTEGER,
    v INTEGER
)";
