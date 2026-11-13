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

use std::collections::BTreeMap;
use std::collections::HashMap;
use std::sync::Arc;

use databend_common_catalog::BasicColumnStatistics;
use databend_common_catalog::TableStatistics;
use databend_common_catalog::table_context::TableContextSettings;
use databend_common_exception::Result;
use databend_common_expression::Scalar;
use databend_common_expression::TableDataType;
use databend_common_expression::TableField;
use databend_common_expression::types::NumberDataType;
use databend_common_expression::types::NumberScalar;
use databend_common_statistics::Datum;
use databend_storages_common_table_meta::meta::ColumnTopN;
use databend_storages_common_table_meta::meta::ColumnTopNEntry;

use crate::framework::FrequencyStatsMap;
use crate::framework::LiteTableContext;
use crate::framework::golden::SqlTestCase;
use crate::framework::golden::open_golden_file;
use crate::framework::golden::write_case_header;

const EVENTS_TABLE: &str = "CREATE TABLE events(item BIGINT NOT NULL, name VARCHAR NULL, value BIGINT NOT NULL, score DOUBLE NOT NULL)";
const FEW_ITEMS_TABLE: &str = "CREATE TABLE few_items(item BIGINT NOT NULL)";
const SMALL_TABLE: &str = "CREATE TABLE small(item BIGINT NOT NULL)";
const UNIQUE_TABLE: &str = "CREATE TABLE uniq(item BIGINT NOT NULL)";

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
    let rows = 10_000_000;
    ctx.register_table_sql_with_stats(
        EVENTS_TABLE,
        Some(table_statistics(rows)),
        HashMap::from([
            ("item".to_string(), int_statistics(5_000_000, rows)),
            ("value".to_string(), int_statistics(rows, rows)),
            ("name".to_string(), BasicColumnStatistics {
                min: Some(Datum::Bytes(b"a".to_vec())),
                max: Some(Datum::Bytes(b"z".to_vec())),
                ndv: Some(2_000_000),
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
    .await?;
    ctx.register_table_sql_with_stats(
        UNIQUE_TABLE,
        Some(table_statistics(rows)),
        HashMap::from([("item".to_string(), int_statistics(rows, rows))]),
        HashMap::new(),
    )
    .await?;

    // Almost unique keys, except three keys of 500k rows each recorded by the
    // top-N stats of an ANALYZE.
    let top_n = ColumnTopN {
        capacity: 3,
        values: (0..3)
            .map(|key| ColumnTopNEntry {
                scalar: Scalar::Number(NumberScalar::Int64(key)),
                count: 500_000,
                error: 0,
            })
            .collect(),
        min_index: None,
    };
    ctx.register_table_with_stats_and_frequency_stats(
        "default",
        "hot",
        vec![TableField::new(
            "item",
            TableDataType::Number(NumberDataType::Int64),
        )],
        Some(table_statistics(rows)),
        HashMap::from([("item".to_string(), int_statistics(9_500_000, rows))]),
        HashMap::new(),
        FrequencyStatsMap {
            top_n: HashMap::from([("item".to_string(), top_n)]),
            count_min_sketch: HashMap::new(),
        },
        BTreeMap::new(),
    )
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
    // The cost heuristics apply unless a case sets the value 2.
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
            description: "Local pure COUNT uses bounded exact counting, with the unchanged aggregation retained as a lazy fallback.",
            setup_sqls: &[],
            sql: "SELECT item, count(*) AS c FROM events GROUP BY item ORDER BY c DESC LIMIT 10",
        },
        SqlTestCase {
            name: "top_count_filter_offset_and_tie_breaker",
            description: "The bounded pass preserves the filter and proves limit + offset counts; the outer TopN resolves ties and offset.",
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
            description: "The bounded pass evaluates the computed group key; fallback retains the same EvalScalar.",
            setup_sqls: &[],
            sql: "SELECT item + 1 AS k, count(*) AS c FROM events GROUP BY k ORDER BY c DESC LIMIT 5",
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
            description: "A key with few distinct values keeps the aggregation cheap.",
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
            name: "unique_key_is_not_rewritten",
            description: "Without top-N stats, an almost unique key is expected to have too few frequent keys to prove the candidates.",
            setup_sqls: &[],
            sql: "SELECT item, count(*) AS c FROM uniq GROUP BY item ORDER BY c DESC LIMIT 10",
        },
        SqlTestCase {
            name: "forced_unique_key_is_rewritten",
            description: "enable_heavy_hitter_aggregate = 2 skips the cost heuristics.",
            setup_sqls: &[],
            sql: "SELECT item, count(*) AS c FROM uniq GROUP BY item ORDER BY c DESC LIMIT 10",
        },
        SqlTestCase {
            name: "top_n_stats_with_enough_frequent_keys",
            description: "Top-N stats show 3 keys frequent enough for the summary to prove the first 3, although the key is almost unique.",
            setup_sqls: &[],
            sql: "SELECT item, count(*) AS c FROM hot GROUP BY item ORDER BY c DESC LIMIT 3",
        },
        SqlTestCase {
            name: "top_n_stats_with_too_few_frequent_keys",
            description: "Top-N stats show only 3 frequent keys, so the summary cannot prove the first 10.",
            setup_sqls: &[],
            sql: "SELECT item, count(*) AS c FROM hot GROUP BY item ORDER BY c DESC LIMIT 10",
        },
        SqlTestCase {
            name: "expensive_filter_is_not_rewritten",
            description: "Both scans would evaluate a filter that is not a comparison of a column with a constant.",
            setup_sqls: &[],
            sql: "SELECT item, count(*) AS c FROM events WHERE value % 2 = 0 GROUP BY item ORDER BY c DESC LIMIT 10",
        },
        SqlTestCase {
            name: "selective_filter_is_not_rewritten",
            description: "A filter keeping few rows leaves little aggregation to save.",
            setup_sqls: &[],
            sql: "SELECT item, count(*) AS c FROM events WHERE value < 1000000 GROUP BY item ORDER BY c DESC LIMIT 10",
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
            "forced_unique_key_is_rewritten" => &[("enable_heavy_hitter_aggregate", "2")],
            _ => &[],
        };
        write_optimized_case(&mut file, case, settings).await?;
    }
    Ok(())
}
