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
use std::io::Write;

use databend_common_catalog::BasicColumnStatistics;
use databend_common_catalog::TableStatistics;
use databend_common_exception::Result;
use databend_common_statistics::Datum;

use crate::framework::LiteTableContext;
use crate::framework::golden::open_golden_file;
use crate::framework::golden::write_case_title;

fn column(ndv: u64, null_count: u64) -> BasicColumnStatistics {
    BasicColumnStatistics {
        min: Some(Datum::Int(0)),
        max: Some(Datum::Int(ndv as i64 - 1)),
        ndv: Some(ndv),
        null_count,
        in_memory_size: 8_000_000,
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 1)]
async fn test_split_grouped_count_distinct() -> Result<()> {
    let mut file = open_golden_file("optimizer", "split_grouped_count_distinct.txt")?;
    let ctx = LiteTableContext::create().await?;
    ctx.register_table_sql_with_stats(
        "CREATE TABLE t(g BIGINT NOT NULL, h BIGINT NOT NULL, x BIGINT NULL)",
        Some(TableStatistics {
            num_rows: Some(1_000_000),
            number_of_blocks: Some(1),
            number_of_segments: Some(1),
            ..Default::default()
        }),
        HashMap::from([
            ("g".to_string(), column(10, 0)),
            ("h".to_string(), column(100_000, 0)),
            ("x".to_string(), column(1000, 100)),
        ]),
        HashMap::new(),
    )
    .await?;

    for (name, description, sql) in [
        (
            "few_groups",
            "10 groups over 1M rows: deduplicate (g, x) first, then count x, which skips NULL.",
            "SELECT g, count(DISTINCT x) FROM t GROUP BY g",
        ),
        (
            "many_groups",
            "100K groups over 1M rows keep small sets: unchanged.",
            "SELECT h, count(DISTINCT x) FROM t GROUP BY h",
        ),
        (
            "other_aggregate",
            "Another aggregate would be computed over the pairs: unchanged.",
            "SELECT g, count(DISTINCT x), count(*) FROM t GROUP BY g",
        ),
    ] {
        let plan = ctx.optimize_plan(ctx.bind_sql(sql).await?).await?;
        write_case_title(&mut file, name, description)?;
        writeln!(file, "sql: {sql}")?;
        writeln!(
            file,
            "{}",
            plan.format_indent(Default::default(), &ctx.stat_context()?)?
        )?;
    }
    Ok(())
}
