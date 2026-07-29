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

use databend_common_exception::Result;
use databend_common_sql::FormatOptions;

use super::table_statistics;
use crate::framework::LiteTableContext;
use crate::framework::golden::SqlTestCase;
use crate::framework::golden::open_golden_file;
use crate::framework::golden::write_case_header;

async fn write_optimized_case(file: &mut impl std::io::Write, case: &SqlTestCase) -> Result<()> {
    let ctx = LiteTableContext::create().await?;
    ctx.register_table_sql_with_stats(
        PROVEN_EMPTY_TABLE,
        Some(table_statistics(0)),
        HashMap::new(),
        HashMap::new(),
    )
    .await?;
    ctx.register_table_sql(ESTIMATED_EMPTY_TABLE).await?;

    let raw_plan = ctx.bind_sql(case.sql).await?;
    let optimized_plan = ctx.optimize_plan(raw_plan.clone()).await?;
    let format_options = FormatOptions { verbose: true };

    write_case_header(file, case)?;
    writeln!(file, "raw_plan:")?;
    writeln!(file, "{}", raw_plan.format_indent(format_options.clone())?)?;
    writeln!(file, "optimized_plan:")?;
    writeln!(file, "{}", optimized_plan.format_indent(format_options)?)?;
    writeln!(file)?;

    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 1)]
async fn test_outer_join_empty_cardinality_optimizer_outcome() -> Result<()> {
    let mut file = open_golden_file("optimizer", "outer_join_empty_cardinality.txt")?;
    let case = SqlTestCase {
        name: "commute_proven_empty_probe_to_build",
        description: "When both LEFT JOIN inputs estimate to zero, commute the proven-empty probe input to the hash-build side.",
        setup_sqls: &[],
        sql: "SELECT p.k, e.k
FROM proven_empty AS p
LEFT JOIN estimated_empty AS e ON p.k = e.k",
    };
    write_optimized_case(&mut file, &case).await?;

    Ok(())
}

const PROVEN_EMPTY_TABLE: &str = "CREATE TABLE proven_empty(k BIGINT)";
const ESTIMATED_EMPTY_TABLE: &str = "CREATE TABLE estimated_empty(k BIGINT)";
