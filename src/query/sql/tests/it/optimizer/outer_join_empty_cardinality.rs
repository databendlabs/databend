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

use databend_common_catalog::BasicColumnStatistics;
use databend_common_catalog::TableStatistics;
use databend_common_exception::Result;
use databend_common_sql::FormatOptions;
use databend_common_sql::optimizer::ir::StatContext;
use databend_common_statistics::Datum;

use crate::framework::LiteTableContext;
use crate::framework::golden::SqlTestCase;
use crate::framework::golden::open_golden_file;
use crate::framework::golden::write_case_header;

fn empty_table_statistics() -> TableStatistics {
    table_statistics(0)
}

fn table_statistics(rows: u64) -> TableStatistics {
    TableStatistics {
        num_rows: Some(rows),
        data_size: Some(rows.saturating_mul(8)),
        data_size_compressed: None,
        index_size: None,
        bloom_index_size: None,
        ngram_index_size: None,
        inverted_index_size: None,
        vector_index_size: None,
        virtual_column_size: None,
        number_of_blocks: Some(0),
        number_of_segments: Some(0),
    }
}

async fn write_optimized_case(file: &mut impl std::io::Write, case: &SqlTestCase) -> Result<()> {
    let ctx = LiteTableContext::create().await?;
    ctx.register_table_sql_with_stats(
        PROVEN_EMPTY_TABLE,
        Some(empty_table_statistics()),
        HashMap::new(),
        HashMap::new(),
    )
    .await?;
    ctx.register_table_sql(ESTIMATED_EMPTY_TABLE).await?;
    for (ddl, min, max) in [
        ("CREATE TABLE residual_left(k BIGINT NOT NULL)", 0, 5),
        ("CREATE TABLE residual_right(k BIGINT NOT NULL)", 10, 15),
    ] {
        ctx.register_table_sql_with_stats(
            ddl,
            Some(table_statistics(8)),
            HashMap::from([("k".to_string(), BasicColumnStatistics {
                min: Some(Datum::Int(min)),
                max: Some(Datum::Int(max)),
                ndv: Some(6),
                null_count: 0,
                in_memory_size: 64,
            })]),
            HashMap::new(),
        )
        .await?;
    }
    ctx.register_table_sql_with_stats(
        STALE_ALL_NULL_TABLE,
        Some(table_statistics(8)),
        HashMap::from([("k".to_string(), BasicColumnStatistics {
            min: Some(Datum::Int(0)),
            max: Some(Datum::Int(5)),
            ndv: Some(0),
            null_count: 8,
            in_memory_size: 64,
        })]),
        HashMap::new(),
    )
    .await?;

    let raw_plan = ctx.bind_sql(case.sql).await?;
    let optimized_plan = ctx.optimize_plan(raw_plan.clone()).await?;
    let format_options = FormatOptions { verbose: true };

    write_case_header(file, case)?;
    writeln!(file, "raw_plan:")?;
    writeln!(
        file,
        "{}",
        raw_plan.format_indent(format_options.clone(), &StatContext::default())?
    )?;
    writeln!(file, "optimized_plan:")?;
    writeln!(
        file,
        "{}",
        optimized_plan.format_indent(format_options, &StatContext::default())?
    )?;
    writeln!(file)?;

    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 1)]
async fn test_outer_join_empty_cardinality_optimizer_outcomes() -> Result<()> {
    let mut file = open_golden_file("optimizer", "outer_join_empty_cardinality.txt")?;
    let cases = [
        SqlTestCase {
            name: "commute_proven_empty_probe_to_build",
            description: "When both LEFT JOIN inputs estimate to zero, commute the proven-empty probe input to the hash-build side.",
            setup_sqls: &[],
            sql: "SELECT p.k, e.k
FROM proven_empty AS p
LEFT JOIN estimated_empty AS e ON p.k = e.k",
        },
        SqlTestCase {
            name: "keep_proven_empty_build_side",
            description: "When the proven-empty input is already the hash-build side, keep the LEFT JOIN orientation.",
            setup_sqls: &[],
            sql: "SELECT e.k, p.k
FROM estimated_empty AS e
LEFT JOIN proven_empty AS p ON e.k = p.k",
        },
        SqlTestCase {
            name: "catalog_all_null_does_not_fold_count",
            description: "Potentially stale catalog all-NULL statistics must not fold an IS NOT NULL count to a constant zero.",
            setup_sqls: &[],
            sql: "SELECT count(*) FROM stale_all_null WHERE k IS NOT NULL",
        },
        SqlTestCase {
            name: "stale_residual_range_is_not_proven_false",
            description: "Disjoint column ranges may be stale: a cross-side residual must not prove that an INNER join has no matches.",
            setup_sqls: &[],
            sql: "SELECT l.k, r.k FROM residual_left l INNER JOIN residual_right r ON l.k > r.k",
        },
        SqlTestCase {
            name: "stale_residual_range_is_not_proven_true",
            description: "An apparently always-true residual from stale ranges must not count as confirmed matches that eliminate every ANTI join row.",
            setup_sqls: &[],
            sql: "SELECT l.k FROM residual_left l LEFT ANTI JOIN residual_right r ON l.k < r.k",
        },
        SqlTestCase {
            name: "residual_modulo_signedness",
            description: "The non-equi Join estimation entry must preserve the modulo safeguards used by ordinary filters.",
            setup_sqls: &[],
            sql: "SELECT l.k FROM residual_left l LEFT ANTI JOIN residual_right r ON l.k % -2 = r.k",
        },
    ];

    for case in &cases {
        write_optimized_case(&mut file, case).await?;
    }

    Ok(())
}

const PROVEN_EMPTY_TABLE: &str = "CREATE TABLE proven_empty(k BIGINT)";
const ESTIMATED_EMPTY_TABLE: &str = "CREATE TABLE estimated_empty(k BIGINT)";
const STALE_ALL_NULL_TABLE: &str = "CREATE TABLE stale_all_null(k BIGINT NULL)";
