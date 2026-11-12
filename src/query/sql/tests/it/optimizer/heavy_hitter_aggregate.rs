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
use std::sync::Arc;

use databend_common_catalog::BasicColumnStatistics;
use databend_common_catalog::TableStatistics;
use databend_common_catalog::table_context::TableContextSettings;
use databend_common_exception::Result;
use databend_common_statistics::Datum;

use crate::framework::LiteTableContext;
use crate::framework::golden::SqlTestCase;
use crate::framework::golden::open_golden_file;
use crate::framework::golden::write_case_header;

const EVENTS_TABLE: &str = "CREATE TABLE events(item BIGINT NOT NULL, name VARCHAR NULL, value BIGINT NOT NULL, score DOUBLE NOT NULL)";
const FEW_ITEMS_TABLE: &str = "CREATE TABLE few_items(item BIGINT NOT NULL)";
const SMALL_TABLE: &str = "CREATE TABLE small(item BIGINT NOT NULL)";

fn table_statistics(rows: u64) -> TableStatistics {
    TableStatistics {
        num_rows: Some(rows),
        data_size: Some(rows.saturating_mul(32)),
        data_size_compressed: None,
        index_size: None,
        bloom_index_size: None,
        ngram_index_size: None,
        inverted_index_size: None,
        vector_index_size: None,
        virtual_column_size: None,
        number_of_blocks: Some(16),
        number_of_segments: Some(1),
    }
}

fn int_statistics(ndv: u64, rows: u64) -> BasicColumnStatistics {
    BasicColumnStatistics {
        min: Some(Datum::Int(0)),
        max: Some(Datum::Int(ndv as i64 - 1)),
        ndv: Some(ndv),
        null_count: 0,
        in_memory_size: rows * 8,
    }
}

async fn setup(ctx: &Arc<LiteTableContext>) -> Result<()> {
    let rows = 1_000_000;
    ctx.register_table_sql_with_stats(
        EVENTS_TABLE,
        Some(table_statistics(rows)),
        HashMap::from([
            ("item".to_string(), int_statistics(500_000, rows)),
            ("value".to_string(), int_statistics(rows, rows)),
            ("name".to_string(), BasicColumnStatistics {
                min: Some(Datum::Bytes(b"a".to_vec())),
                max: Some(Datum::Bytes(b"z".to_vec())),
                ndv: Some(200_000),
                null_count: 1000,
                in_memory_size: rows * 16,
            }),
        ]),
        HashMap::new(),
    )
    .await?;
    ctx.register_table_sql_with_stats(
        FEW_ITEMS_TABLE,
        Some(table_statistics(rows)),
        HashMap::from([("item".to_string(), int_statistics(100, rows))]),
        HashMap::new(),
    )
    .await?;
    ctx.register_table_sql_with_stats(
        SMALL_TABLE,
        Some(table_statistics(1000)),
        HashMap::from([("item".to_string(), int_statistics(1000, 1000))]),
        HashMap::new(),
    )
    .await
}

/// Plans `case` with the rewrite enabled and `settings` applied.
async fn write_optimized_case(
    file: &mut impl std::io::Write,
    case: &SqlTestCase,
    settings: &[(&str, &str)],
) -> Result<()> {
    let ctx = LiteTableContext::create().await?;
    setup(&ctx).await?;
    let ctx_settings = ctx.get_settings();
    ctx_settings.set_setting("enable_heavy_hitter_aggregate".to_string(), "1".to_string())?;
    for (name, value) in settings {
        ctx_settings.set_setting(name.to_string(), value.to_string())?;
    }

    let raw_plan = ctx.bind_sql(case.sql).await?;
    let optimized_plan = ctx.optimize_plan(raw_plan.clone()).await?;

    write_case_header(file, case)?;
    writeln!(file, "raw_plan:")?;
    writeln!(
        file,
        "{}",
        raw_plan.format_indent(Default::default(), &ctx.stat_context()?)?
    )?;
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
async fn test_heavy_hitter_aggregate_outcomes() -> Result<()> {
    let mut file = open_golden_file("optimizer", "heavy_hitter_aggregate.txt")?;

    let cases = [
        SqlTestCase {
            name: "top_count_single_key",
            description: "GROUP BY one key ORDER BY COUNT(*) DESC LIMIT n aggregates only the rows of candidate keys, found by a cross-joined summary of the same scan with fresh columns.",
            setup_sqls: &[],
            sql: "SELECT item, count(*) AS c FROM events GROUP BY item ORDER BY c DESC LIMIT 10",
        },
        SqlTestCase {
            name: "top_count_filter_offset_and_tie_breaker",
            description: "A filter is cloned into the summary input, the summary keeps limit + offset keys, and later sort keys only break ties.",
            setup_sqls: &[],
            sql: "SELECT item, count(*) AS c FROM events WHERE value > 10 GROUP BY item ORDER BY c DESC, item LIMIT 10 OFFSET 5",
        },
        SqlTestCase {
            name: "top_count_nullable_string_key_with_other_aggregates",
            description: "Other aggregates of the candidate groups stay exact because every row of a candidate key passes the filter.",
            setup_sqls: &[],
            sql: "SELECT name, count(*), sum(value) FROM events GROUP BY name ORDER BY count(*) DESC LIMIT 3",
        },
        SqlTestCase {
            name: "top_count_computed_key",
            description: "A computed group key is computed again by the cloned EvalScalar of the summary input.",
            setup_sqls: &[],
            sql: "SELECT item % 100000 AS k, count(*) AS c FROM events GROUP BY k ORDER BY c DESC LIMIT 5",
        },
        SqlTestCase {
            name: "top_count_sort_with_limit",
            description: "Without the fused TopN, a Sort with a limit matches as well.",
            setup_sqls: &[],
            sql: "SELECT item, count(*) AS c FROM events GROUP BY item ORDER BY c DESC LIMIT 10",
        },
        SqlTestCase {
            name: "ascending_count_is_not_rewritten",
            description: "The least frequent keys cannot be bounded by a heavy hitter summary.",
            setup_sqls: &[],
            sql: "SELECT item, count(*) AS c FROM events GROUP BY item ORDER BY c LIMIT 10",
        },
        SqlTestCase {
            name: "order_by_sum_is_not_rewritten",
            description: "Only COUNT(*) is bounded by the summary.",
            setup_sqls: &[],
            sql: "SELECT item, sum(value) AS s FROM events GROUP BY item ORDER BY s DESC LIMIT 10",
        },
        SqlTestCase {
            name: "count_of_nullable_column_is_not_rewritten",
            description: "COUNT of a nullable column does not count the rows the summary counts.",
            setup_sqls: &[],
            sql: "SELECT item, count(name) AS c FROM events GROUP BY item ORDER BY c DESC LIMIT 10",
        },
        SqlTestCase {
            name: "having_is_not_rewritten",
            description: "A filter between the aggregate and the TopN is not supported.",
            setup_sqls: &[],
            sql: "SELECT item, count(*) AS c FROM events GROUP BY item HAVING c > 1 ORDER BY c DESC LIMIT 10",
        },
        SqlTestCase {
            name: "multiple_group_keys_are_not_rewritten",
            description: "The summary supports a single group key.",
            setup_sqls: &[],
            sql: "SELECT item, value, count(*) AS c FROM events GROUP BY item, value ORDER BY c DESC LIMIT 10",
        },
        SqlTestCase {
            name: "float_key_is_not_rewritten",
            description: "Float keys are not supported because their equality differs from their bytes.",
            setup_sqls: &[],
            sql: "SELECT score, count(*) AS c FROM events GROUP BY score ORDER BY c DESC LIMIT 10",
        },
        SqlTestCase {
            name: "large_limit_is_not_rewritten",
            description: "limit + offset above 1000 is not rewritten.",
            setup_sqls: &[],
            sql: "SELECT item, count(*) AS c FROM events GROUP BY item ORDER BY c DESC LIMIT 1000 OFFSET 1",
        },
        SqlTestCase {
            name: "few_groups_are_not_rewritten",
            description: "A key with at most capacity distinct values never needs more groups than the summary.",
            setup_sqls: &[],
            sql: "SELECT item, count(*) AS c FROM few_items GROUP BY item ORDER BY c DESC LIMIT 10",
        },
        SqlTestCase {
            name: "small_input_is_not_rewritten",
            description: "An input with at most capacity rows never needs more groups than the summary.",
            setup_sqls: &[],
            sql: "SELECT item, count(*) AS c FROM small GROUP BY item ORDER BY c DESC LIMIT 10",
        },
        SqlTestCase {
            name: "join_input_is_not_rewritten",
            description: "The input is scanned twice, so it must be one table scan.",
            setup_sqls: &[],
            sql: "SELECT e.item, count(*) AS c FROM events e JOIN small s ON e.item = s.item GROUP BY e.item ORDER BY c DESC LIMIT 10",
        },
        SqlTestCase {
            name: "non_deterministic_filter_is_not_rewritten",
            description: "Two scans with a non-deterministic filter would not read the same rows.",
            setup_sqls: &[],
            sql: "SELECT item, count(*) AS c FROM events WHERE rand() < 0.5 GROUP BY item ORDER BY c DESC LIMIT 10",
        },
    ];

    for case in &cases {
        let settings: &[(&str, &str)] = match case.name {
            "top_count_sort_with_limit" => &[("enable_top_n", "0")],
            _ => &[],
        };
        write_optimized_case(&mut file, case, settings).await?;
    }
    Ok(())
}
