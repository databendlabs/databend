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
use databend_common_catalog::table_context::TableContextSettings;
use databend_common_exception::Result;
use databend_common_sql::optimizer::CollectStatisticsOptimizer;
use databend_common_sql::optimizer::OptimizerContext;
use databend_common_sql::optimizer::optimizers::CascadesOptimizer;
use databend_common_sql::optimizer::optimizers::DPhpyOptimizer;
use databend_common_sql::optimizer::optimizers::operator::PullUpFilterOptimizer;
use databend_common_sql::optimizer::optimizers::operator::RuleStatsAggregateOptimizer;
use databend_common_sql::optimizer::optimizers::operator::SubqueryDecorrelatorOptimizer;
use databend_common_sql::optimizer::optimizers::recursive::RecursiveRuleOptimizer;
use databend_common_sql::optimizer::optimizers::rule::DEFAULT_REWRITE_RULES;
use databend_common_sql::optimizer::optimizers::rule::RuleID;
use databend_common_sql::optimizer::pipeline::OptimizerPipeline;
use databend_common_sql::plans::Plan;
use databend_common_statistics::Datum;

use crate::framework::LiteTableContext;
use crate::framework::golden::open_golden_file;
use crate::framework::golden::write_case_title;

struct JoinMemoCase<'a> {
    name: &'a str,
    description: &'a str,
    tables: Vec<JoinMemoTable<'static>>,
    settings: &'a [(&'a str, &'a str)],
    sql: &'a str,
}

struct JoinMemoTable<'a> {
    create_sql: &'a str,
    column_statistics: fn(u64) -> HashMap<String, BasicColumnStatistics>,
}

const CLUSTER_KEY_DISCOUNT: &[(&str, &str)] = &[("cost_factor_cluster_key", "85")];

fn key_tables_with_a_clustered_k1_k2() -> Vec<JoinMemoTable<'static>> {
    vec![
        JoinMemoTable {
            create_sql: "CREATE TABLE a(k1 BIGINT, k2 BIGINT, v BIGINT) CLUSTER BY (k1, k2)",
            column_statistics,
        },
        JoinMemoTable {
            create_sql: "CREATE TABLE b(k1 BIGINT, k2 BIGINT, v BIGINT)",
            column_statistics,
        },
        JoinMemoTable {
            create_sql: "CREATE TABLE c(k1 BIGINT, k2 BIGINT, v BIGINT)",
            column_statistics,
        },
    ]
}

fn string_filter_tables() -> Vec<JoinMemoTable<'static>> {
    vec![
        JoinMemoTable {
            create_sql: "CREATE TABLE a(join_key BIGINT, column_a STRING, column_b STRING) CLUSTER BY (column_a)",
            column_statistics: string_column_statistics,
        },
        JoinMemoTable {
            create_sql: "CREATE TABLE b(join_key BIGINT, column_a STRING, column_b STRING)",
            column_statistics: string_column_statistics,
        },
    ]
}

fn composite_string_tables() -> Vec<JoinMemoTable<'static>> {
    vec![
        JoinMemoTable {
            create_sql: "CREATE TABLE table_a(join_key BIGINT, column_a STRING, column_b STRING) CLUSTER BY (column_a, column_b)",
            column_statistics: string_column_statistics,
        },
        JoinMemoTable {
            create_sql: "CREATE TABLE table_b(join_key BIGINT, column_a STRING, column_b STRING) CLUSTER BY (column_b, column_a)",
            column_statistics: string_column_statistics,
        },
    ]
}

fn table_statistics(rows: u64) -> TableStatistics {
    TableStatistics {
        num_rows: Some(rows),
        data_size: Some(rows.saturating_mul(24)),
        data_size_compressed: None,
        index_size: None,
        bloom_index_size: None,
        ngram_index_size: None,
        inverted_index_size: None,
        vector_index_size: None,
        virtual_column_size: None,
        number_of_blocks: Some(1),
        number_of_segments: Some(1),
    }
}

fn column_statistics(rows: u64) -> HashMap<String, BasicColumnStatistics> {
    ["k1", "k2", "v"]
        .into_iter()
        .map(|column| {
            (column.to_string(), BasicColumnStatistics {
                min: Some(Datum::Int(0)),
                max: Some(Datum::Int(rows as i64)),
                ndv: Some(rows),
                null_count: 0,
                in_memory_size: rows.saturating_mul(8),
            })
        })
        .collect()
}

fn trace_column_statistics(rows: u64) -> HashMap<String, BasicColumnStatistics> {
    let mut stats = column_statistics(rows);
    stats.insert("start_day".to_string(), BasicColumnStatistics {
        min: Some(Datum::UInt(20240101)),
        max: Some(Datum::UInt(20241231)),
        ndv: Some(365),
        null_count: 0,
        in_memory_size: rows.saturating_mul(4),
    });
    stats.insert("trace_id".to_string(), BasicColumnStatistics {
        min: Some(Datum::Bytes(
            b"0000000000000000000000000000000000000000".to_vec(),
        )),
        max: Some(Datum::Bytes(
            b"ffffffffffffffffffffffffffffffffffffffff".to_vec(),
        )),
        ndv: Some(rows),
        null_count: 0,
        in_memory_size: rows.saturating_mul(40),
    });
    stats
}

fn string_column_statistics(rows: u64) -> HashMap<String, BasicColumnStatistics> {
    let mut stats = HashMap::new();
    stats.insert("join_key".to_string(), BasicColumnStatistics {
        min: Some(Datum::Int(0)),
        max: Some(Datum::Int(rows as i64)),
        ndv: Some(rows),
        null_count: 0,
        in_memory_size: rows.saturating_mul(8),
    });
    for column in ["column_a", "column_b"] {
        stats.insert(column.to_string(), BasicColumnStatistics {
            min: Some(Datum::Bytes(b"aaa".to_vec())),
            max: Some(Datum::Bytes(b"zzz".to_vec())),
            ndv: Some(100),
            null_count: 0,
            in_memory_size: rows.saturating_mul(16),
        });
    }
    stats
}

#[tokio::test(flavor = "multi_thread", worker_threads = 1)]
async fn test_cluster_key_order_join_memo_golden() -> Result<()> {
    let mut file = open_golden_file("optimizer", "cluster_key_join_order.txt")?;

    for case in [
        JoinMemoCase {
            name: "k1_k2_prefix",
            description: "Full memo output when the clustered probe can first match a.k1.",
            tables: key_tables_with_a_clustered_k1_k2(),
            settings: CLUSTER_KEY_DISCOUNT,
            sql: "
                    SELECT *
                    FROM a
                    JOIN b ON a.k1 = b.k1
                    JOIN c ON a.k2 = c.k2
                ",
        },
        JoinMemoCase {
            name: "k2_k1_prefix",
            description: "Full memo output when the clustered probe can first match a.k2.",
            tables: vec![
                JoinMemoTable {
                    create_sql:
                        "CREATE TABLE a(k1 BIGINT, k2 BIGINT, v BIGINT) CLUSTER BY (k2, k1)",
                    column_statistics,
                },
                JoinMemoTable {
                    create_sql: "CREATE TABLE b(k1 BIGINT, k2 BIGINT, v BIGINT)",
                    column_statistics,
                },
                JoinMemoTable {
                    create_sql: "CREATE TABLE c(k1 BIGINT, k2 BIGINT, v BIGINT)",
                    column_statistics,
                },
            ],
            settings: CLUSTER_KEY_DISCOUNT,
            sql: "
                    SELECT *
                    FROM a
                    JOIN b ON a.k1 = b.k1
                    JOIN c ON a.k2 = c.k2
                ",
        },
        JoinMemoCase {
            name: "filter_preserves_cluster_keys",
            description: "Cluster keys still affect join order after a filter on the clustered table.",
            tables: key_tables_with_a_clustered_k1_k2(),
            settings: CLUSTER_KEY_DISCOUNT,
            sql: "
                    SELECT *
                    FROM (SELECT * FROM a WHERE v >= 0) a
                    JOIN b ON a.k1 = b.k1
                    JOIN c ON a.k2 = c.k2
                ",
        },
        JoinMemoCase {
            name: "limit_and_join_preserve_cluster_keys",
            description: "Cluster keys still affect join order after a limit subquery and a partial join.",
            tables: key_tables_with_a_clustered_k1_k2(),
            settings: CLUSTER_KEY_DISCOUNT,
            sql: "
                    SELECT *
                    FROM (SELECT * FROM a LIMIT 1000) a
                    JOIN b ON a.k1 = b.k1
                    JOIN c ON a.k2 = c.k2
                ",
        },
        JoinMemoCase {
            name: "build_side_cluster_keys_do_not_propagate",
            description: "Cluster keys from a build-side clustered table do not affect later join costs.",
            tables: key_tables_with_a_clustered_k1_k2(),
            settings: CLUSTER_KEY_DISCOUNT,
            sql: "
                    SELECT *
                    FROM b
                    JOIN (SELECT * FROM a LIMIT 100) a ON b.k1 = a.k1
                    JOIN (SELECT * FROM c LIMIT 10) c ON a.k2 = c.k2
                ",
        },
        JoinMemoCase {
            name: "clustered_filter_side_becomes_build",
            description: "A filter on the clustered column makes that side a better build input.",
            tables: string_filter_tables(),
            settings: CLUSTER_KEY_DISCOUNT,
            sql: "
                    SELECT *
                    FROM a
                    JOIN b ON a.join_key = b.join_key
                    WHERE a.column_a = 'xxx'
                      AND b.column_b = 'xxx'
                ",
        },
        JoinMemoCase {
            name: "self_join_filter_on_clustered_alias_becomes_build",
            description: "A self-join filter only matches the alias whose filter references the clustered column.",
            tables: vec![JoinMemoTable {
                create_sql:
                    "CREATE TABLE t(join_key BIGINT, column_a STRING, column_b STRING) CLUSTER BY (column_a)",
                column_statistics: string_column_statistics,
            }],
            settings: CLUSTER_KEY_DISCOUNT,
            sql: "
                    SELECT *
                    FROM t AS l
                    JOIN t AS r ON l.join_key = r.join_key
                    WHERE l.column_a = 'xxx'
                      AND r.column_b = 'xxx'
                ",
        },
        JoinMemoCase {
            name: "clustered_filter_discount_disabled",
            description: "The default cluster-key cost factor disables the join-order discount.",
            tables: string_filter_tables(),
            settings: &[],
            sql: "
                    SELECT *
                    FROM a
                    JOIN b ON a.join_key = b.join_key
                    WHERE a.column_a = 'xxx'
                      AND b.column_b = 'xxx'
                ",
        },
        JoinMemoCase {
            name: "composite_cluster_key_filter_on_second_column",
            description: "A filter on column_b favors the table whose cluster key starts with column_b.",
            tables: composite_string_tables(),
            settings: CLUSTER_KEY_DISCOUNT,
            sql: "
                    SELECT *
                    FROM table_a AS a
                    JOIN table_b AS b
                      ON a.column_a = b.column_a
                     AND a.column_b = b.column_b
                    WHERE b.column_b = 'xxx'
                ",
        },
        JoinMemoCase {
            name: "composite_cluster_key_filter_on_first_column",
            description: "A filter on column_a favors the table whose cluster key starts with column_a.",
            tables: composite_string_tables(),
            settings: CLUSTER_KEY_DISCOUNT,
            sql: "
                    SELECT *
                    FROM table_a AS a
                    JOIN table_b AS b
                      ON a.column_a = b.column_a
                     AND a.column_b = b.column_b
                    WHERE a.column_a = 'xxx'
                ",
        },
        JoinMemoCase {
            name: "composite_cluster_key_later_column_is_not_prefix",
            description: "A later composite cluster-key column is not treated as a leading prefix.",
            tables: vec![
                JoinMemoTable {
                    create_sql:
                        "CREATE TABLE table_a(join_key BIGINT, column_a STRING, column_b STRING) CLUSTER BY (column_a, column_b)",
                    column_statistics: string_column_statistics,
                },
                JoinMemoTable {
                    create_sql:
                        "CREATE TABLE table_b(join_key BIGINT, column_a STRING, column_b STRING)",
                    column_statistics: string_column_statistics,
                },
            ],
            settings: CLUSTER_KEY_DISCOUNT,
            sql: "
                    SELECT a.column_b, b.column_b
                    FROM table_a AS a
                    JOIN table_b AS b
                      ON a.column_b = b.column_b
                    WHERE a.column_b = 'xxx'
                ",
        },
        JoinMemoCase {
            name: "linear_expression_cluster_key",
            description: "A LINEAR cluster key with to_yyyymmdd and substring expressions affects join costs.",
            tables: vec![
                JoinMemoTable {
                    create_sql: "CREATE TABLE a(k1 BIGINT, k2 BIGINT, v BIGINT, start_time TIMESTAMP, start_day UInt32, trace_id STRING) CLUSTER BY linear (
                    to_yyyymmdd(start_time),
                    SUBSTRING(trace_id FROM 1 FOR 40)
                )",
                    column_statistics: trace_column_statistics,
                },
                JoinMemoTable {
                    create_sql:
                        "CREATE TABLE b(k1 BIGINT, k2 BIGINT, v BIGINT, start_time TIMESTAMP, start_day UInt32, trace_id STRING)",
                    column_statistics: trace_column_statistics,
                },
                JoinMemoTable {
                    create_sql:
                        "CREATE TABLE c(k1 BIGINT, k2 BIGINT, v BIGINT, start_time TIMESTAMP, start_day UInt32, trace_id STRING)",
                    column_statistics: trace_column_statistics,
                },
            ],
            settings: CLUSTER_KEY_DISCOUNT,
            sql: "
                    SELECT *
                    FROM a
                    JOIN b
                        ON to_yyyymmdd(a.start_time) = b.start_day
                        AND SUBSTRING(a.trace_id FROM 1 FOR 40) = b.trace_id
                    JOIN c ON a.k2 = c.k2
                ",
        },
    ] {
        write_cluster_key_join_order_memo(&mut file, case).await?;
    }

    Ok(())
}

async fn write_cluster_key_join_order_memo(
    file: &mut impl Write,
    case: JoinMemoCase<'_>,
) -> Result<()> {
    let ctx = LiteTableContext::create().await?;
    ctx.configure_for_optimizer_case(true)?;
    ctx.set_cluster_node_num(1);

    write_case_title(file, case.name, case.description)?;
    for (name, value) in case.settings {
        ctx.get_settings()
            .set_setting((*name).to_string(), (*value).to_string())?;
        writeln!(file, "setting: {name}={value}")?;
    }
    for table in &case.tables {
        writeln!(file, "setup: {}", table.create_sql)?;
        ctx.register_table_sql_with_stats(
            table.create_sql,
            Some(table_statistics(1000)),
            (table.column_statistics)(1000),
            HashMap::new(),
        )
        .await?;
    }

    let sql = case
        .sql
        .lines()
        .map(str::trim)
        .filter(|line| !line.is_empty())
        .collect::<Vec<_>>()
        .join("\n");
    writeln!(file, "sql: {sql}")?;
    writeln!(file, "memo:")?;
    writeln!(file, "{}", explain_memo(&ctx, &sql).await?)?;
    writeln!(file)?;
    Ok(())
}

async fn explain_memo(ctx: &std::sync::Arc<LiteTableContext>, sql: &str) -> Result<String> {
    let Plan::Query {
        s_expr, metadata, ..
    } = ctx.bind_sql(sql).await?
    else {
        unreachable!("SELECT should bind to a query plan");
    };

    let settings = ctx.get_settings();
    let opt_ctx = OptimizerContext::new(ctx.clone(), metadata)
        .with_settings(&settings)?
        .set_enable_distributed_optimization(true)
        .clone();
    opt_ctx.set_flag("explain_memo", true);

    let mut pipeline = OptimizerPipeline::new(opt_ctx.clone(), *s_expr)
        .await?
        .add(SubqueryDecorrelatorOptimizer::new(opt_ctx.clone(), None))
        .add(RuleStatsAggregateOptimizer::new(opt_ctx.clone()))
        .add(CollectStatisticsOptimizer::new(opt_ctx.clone()))
        .add(PullUpFilterOptimizer::new(opt_ctx.clone()))
        .add(RecursiveRuleOptimizer::new(
            opt_ctx.clone(),
            &DEFAULT_REWRITE_RULES,
        ))
        .add(RecursiveRuleOptimizer::new(opt_ctx.clone(), &[
            RuleID::SplitAggregate,
        ]))
        .add(DPhpyOptimizer::new(opt_ctx.clone()))
        .add(CascadesOptimizer::new(opt_ctx.clone())?);

    let _s_expr = pipeline.execute().await?;

    pipeline.memo().display()
}
