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
use std::sync::Arc;

use databend_common_catalog::BasicColumnStatistics;
use databend_common_exception::ErrorCode;
use databend_common_exception::Result;
use databend_common_sql::ColumnEntry;
use databend_common_sql::FormatOptions;
use databend_common_sql::Metadata;
use databend_common_sql::MetadataRef;
use databend_common_sql::Symbol;
use databend_common_sql::optimizer::ir::RelExpr;
use databend_common_sql::optimizer::ir::SExpr;
use databend_common_sql::optimizer::ir::StatInfo;
use databend_common_sql::plans::Operator;
use databend_common_sql::plans::Plan;
use databend_common_sql::plans::RelOp;
use databend_common_statistics::Histogram;

use super::column_stat;
use super::histogram_stat;
use super::table_statistics;
use crate::framework::LiteTableContext;
use crate::framework::golden::open_golden_file;
use crate::framework::golden::write_case_title;

struct TableInput {
    ddl: &'static str,
    rows: u64,
    column_stats: HashMap<String, BasicColumnStatistics>,
    histograms: HashMap<String, Histogram>,
}

struct UnionCase {
    name: &'static str,
    description: &'static str,
    sql: &'static str,
    tables: Vec<TableInput>,
    targets: Vec<StatTarget>,
}

struct StatTarget {
    // Child directions starting from the optimized Plan::Query SExpr root.
    path: Vec<Child>,
    operator: RelOp,
}

#[derive(Clone, Copy, Debug)]
enum Child {
    Left,
    Right,
}

impl Child {
    fn index(self) -> usize {
        match self {
            Self::Left => 0,
            Self::Right => 1,
        }
    }
}

fn collect_operator_paths(
    expr: &SExpr,
    operator: &RelOp,
    path: &mut Vec<Child>,
    paths: &mut Vec<Vec<Child>>,
) {
    if &expr.plan().rel_op() == operator {
        paths.push(path.clone());
    }

    for (index, child) in expr.children().enumerate() {
        path.push(match index {
            0 => Child::Left,
            1 => Child::Right,
            _ => unreachable!("SExpr nodes have at most two children"),
        });
        collect_operator_paths(child, operator, path, paths);
        path.pop();
    }
}

fn possible_operator_paths(root: &SExpr, operator: &RelOp) -> Vec<Vec<Child>> {
    let mut paths = Vec::new();
    collect_operator_paths(root, operator, &mut Vec::new(), &mut paths);
    paths
}

fn invalid_target_path(
    root: &SExpr,
    target: &StatTarget,
    reason: impl std::fmt::Display,
) -> ErrorCode {
    ErrorCode::Internal(format!(
        "cannot resolve {:?} target at path {:?}: {}; possible paths for this operator: {:?}",
        target.operator,
        target.path,
        reason,
        possible_operator_paths(root, &target.operator),
    ))
}

async fn register_table(ctx: &Arc<LiteTableContext>, input: TableInput) -> Result<()> {
    ctx.register_table_sql_with_stats(
        input.ddl,
        Some(table_statistics(input.rows)),
        input.column_stats,
        input.histograms,
    )
    .await
}

fn column_label(metadata: &Metadata, column: Symbol) -> String {
    let id = column.as_usize();
    match metadata.column(column) {
        ColumnEntry::BaseTableColumn(column) => {
            let table = metadata.table(column.table_index);
            format!("{}.{} (#{id})", table.name(), column.column_name)
        }
        entry => format!("{} (#{id})", entry.name()),
    }
}

fn write_stat_info(file: &mut impl Write, metadata: &Metadata, stat_info: &StatInfo) -> Result<()> {
    writeln!(file, "cardinality: {:.3}", stat_info.cardinality)?;
    writeln!(
        file,
        "precise_cardinality: {:?}",
        stat_info.statistics.precise_cardinality
    )?;

    let mut column_stats = stat_info.statistics.column_stats.iter().collect::<Vec<_>>();
    column_stats.sort_by_key(|(column, _)| **column);
    if column_stats.is_empty() {
        writeln!(file, "column_stats: none")?;
        return Ok(());
    }

    for (column, stat) in column_stats {
        writeln!(
            file,
            "column_stat: {} min={}, max={}, ndv={:?}, null_count={:?}, histogram={}",
            column_label(metadata, *column),
            stat.min,
            stat.max,
            stat.ndv,
            stat.null_count,
            if stat.histogram.is_some() {
                "some"
            } else {
                "none"
            }
        )?;
    }
    Ok(())
}

fn format_node(metadata: &MetadataRef, expr: &SExpr) -> Result<String> {
    Plan::Query {
        s_expr: Box::new(expr.clone()),
        metadata: metadata.clone(),
        bind_context: Default::default(),
        rewrite_kind: None,
        formatted_ast: None,
        ignore_result: false,
    }
    .format_indent(FormatOptions::default())
}

fn write_derived_stats(
    file: &mut impl Write,
    metadata: &MetadataRef,
    root: &SExpr,
    target: &StatTarget,
) -> Result<()> {
    let mut expr = root;
    for (depth, child) in target.path.iter().enumerate() {
        let index = child.index();
        expr = expr.child(index).map_err(|_| {
            invalid_target_path(
                root,
                target,
                format!(
                    "child index {} does not exist at node {:?}, which has {} children",
                    index,
                    &target.path[..depth],
                    expr.arity()
                ),
            )
        })?;
    }

    let actual = expr.plan().rel_op();
    if actual != target.operator {
        return Err(invalid_target_path(
            root,
            target,
            format!("found {actual:?} instead"),
        ));
    }

    writeln!(file, "path: {:?}", target.path)?;
    writeln!(file, "node:")?;
    writeln!(file, "{}", format_node(metadata, expr)?)?;
    let stat_info = RelExpr::with_s_expr(expr).derive_cardinality()?;
    write_stat_info(file, &metadata.read(), &stat_info)?;
    Ok(())
}

async fn write_case(file: &mut impl Write, case: UnionCase) -> Result<()> {
    let ctx = LiteTableContext::create().await?;
    for table in case.tables {
        register_table(&ctx, table).await?;
    }

    let raw_plan = ctx.bind_sql(case.sql).await?;
    let Plan::Query {
        s_expr, metadata, ..
    } = ctx.optimize_plan(raw_plan).await?
    else {
        unreachable!("UNION ALL query should bind to a query plan");
    };

    write_case_title(file, case.name, case.description)?;
    writeln!(file, "sql: {}", case.sql)?;
    for target in &case.targets {
        write_derived_stats(file, &metadata, &s_expr, target)?;
    }
    writeln!(file)?;
    Ok(())
}

fn table(ddl: &'static str, rows: u64, column: Option<BasicColumnStatistics>) -> TableInput {
    TableInput {
        ddl,
        rows,
        column_stats: column
            .map(|column| HashMap::from([("k".to_string(), column)]))
            .unwrap_or_default(),
        histograms: HashMap::new(),
    }
}

fn cases() -> Result<Vec<UnionCase>> {
    Ok(vec![
        UnionCase {
            name: "lossless_coercion",
            description: "SQL binding inserts a nullable INT to BIGINT coercion and UnionAll derives statistics after the cast.",
            sql: "SELECT k FROM l UNION ALL SELECT k FROM r",
            tables: vec![
                table(
                    "CREATE TABLE l(k INT NULL)",
                    10,
                    Some(column_stat(
                        r#"{"min": 1, "max": 3, "ndv": 3, "null_count": 1}"#,
                    )?),
                ),
                table(
                    "CREATE TABLE r(k BIGINT NULL)",
                    20,
                    Some(column_stat(
                        r#"{"min": 4, "max": 8, "ndv": 5, "null_count": 2}"#,
                    )?),
                ),
            ],
            targets: vec![StatTarget {
                path: vec![],
                operator: RelOp::UnionAll,
            }],
        },
        UnionCase {
            name: "finite_range_reduces_union_ndv_bound",
            description: "UnionAll reduces its conservative NDV upper bound to the merged finite range before an outer cast consumes the statistics.",
            sql: "SELECT CAST(k AS BIGINT) AS k FROM (SELECT k FROM l UNION ALL SELECT k FROM r) AS u",
            tables: vec![
                table(
                    "CREATE TABLE l(k INT NOT NULL)",
                    2,
                    Some(column_stat(
                        r#"{"min": 1, "max": 2, "ndv": 2, "null_count": 0}"#,
                    )?),
                ),
                table(
                    "CREATE TABLE r(k INT NOT NULL)",
                    1,
                    Some(column_stat(
                        r#"{"min": 1, "max": 1, "ndv": 1, "null_count": 0}"#,
                    )?),
                ),
            ],
            targets: vec![StatTarget {
                path: vec![],
                operator: RelOp::EvalScalar,
            }],
        },
        UnionCase {
            name: "nonempty_branch_without_column_stats",
            description: "A non-empty branch with unknown column statistics makes the UnionAll output column statistics unknown.",
            sql: "SELECT k FROM l UNION ALL SELECT k FROM r",
            tables: vec![
                table(
                    "CREATE TABLE l(k BIGINT NOT NULL)",
                    10,
                    Some(column_stat(
                        r#"{"min": 1, "max": 3, "ndv": 3, "null_count": 1}"#,
                    )?),
                ),
                table("CREATE TABLE r(k BIGINT NOT NULL)", 20, None),
            ],
            targets: vec![StatTarget {
                path: vec![],
                operator: RelOp::UnionAll,
            }],
        },
        UnionCase {
            name: "empty_branch_without_column_stats",
            description: "An exactly empty branch contributes no values, so UnionAll preserves the other branch statistics.",
            sql: "SELECT k FROM l UNION ALL SELECT k FROM r",
            tables: vec![
                table(
                    "CREATE TABLE l(k BIGINT NOT NULL)",
                    10,
                    Some(column_stat(
                        r#"{"min": 1, "max": 3, "ndv": 3, "null_count": 1}"#,
                    )?),
                ),
                table("CREATE TABLE r(k BIGINT NOT NULL)", 0, None),
            ],
            targets: vec![StatTarget {
                path: vec![],
                operator: RelOp::UnionAll,
            }],
        },
        UnionCase {
            name: "one_sided_expected_ndv",
            description: "UnionAll preserves the known expected NDV when the other branch only has an upper bound.",
            sql: "SELECT k FROM l UNION ALL SELECT k FROM r",
            tables: vec![
                table(
                    "CREATE TABLE l(k BIGINT NOT NULL)",
                    10,
                    Some(column_stat(
                        r#"{"min": 1, "max": 3, "ndv": null, "null_count": 0}"#,
                    )?),
                ),
                table(
                    "CREATE TABLE r(k BIGINT NOT NULL)",
                    20,
                    Some(column_stat(
                        r#"{"min": 2, "max": 8, "ndv": 5, "null_count": 0}"#,
                    )?),
                ),
            ],
            targets: vec![StatTarget {
                path: vec![],
                operator: RelOp::UnionAll,
            }],
        },
        UnionCase {
            name: "histogram_overlap",
            description: "Overlapping input histograms refine the merged NDV while the output histogram is dropped.",
            sql: "SELECT k FROM l UNION ALL SELECT k FROM r",
            tables: vec![
                TableInput {
                    ddl: "CREATE TABLE l(k BIGINT NOT NULL)",
                    rows: 100,
                    column_stats: HashMap::from([(
                        "k".to_string(),
                        column_stat(r#"{"min": 0, "max": 9, "ndv": 10, "null_count": 0}"#)?,
                    )]),
                    histograms: HashMap::from([(
                        "k".to_string(),
                        histogram_stat(
                            r#"{
                                "accuracy": false,
                                "buckets": [
                                    {"lower_bound": {"Int": 0}, "upper_bound": {"Int": 4}, "num_values": 50.0, "num_distinct": 5.0},
                                    {"lower_bound": {"Int": 5}, "upper_bound": {"Int": 9}, "num_values": 50.0, "num_distinct": 5.0}
                                ],
                                "avg_spacing": 4.5
                            }"#,
                        )?,
                    )]),
                },
                TableInput {
                    ddl: "CREATE TABLE r(k BIGINT NOT NULL)",
                    rows: 100,
                    column_stats: HashMap::from([(
                        "k".to_string(),
                        column_stat(r#"{"min": 5, "max": 14, "ndv": 10, "null_count": 0}"#)?,
                    )]),
                    histograms: HashMap::from([(
                        "k".to_string(),
                        histogram_stat(
                            r#"{
                                "accuracy": false,
                                "buckets": [
                                    {"lower_bound": {"Int": 5}, "upper_bound": {"Int": 9}, "num_values": 50.0, "num_distinct": 5.0},
                                    {"lower_bound": {"Int": 10}, "upper_bound": {"Int": 14}, "num_values": 50.0, "num_distinct": 5.0}
                                ],
                                "avg_spacing": 4.5
                            }"#,
                        )?,
                    )]),
                },
            ],
            targets: vec![StatTarget {
                path: vec![],
                operator: RelOp::UnionAll,
            }],
        },
        UnionCase {
            name: "join_consumes_union_stats",
            description: "A join above the SQL-derived UnionAll consumes its merged output NDV instead of treating the key as unknown.",
            sql: "SELECT u.k FROM (SELECT k FROM l UNION ALL SELECT k FROM r) AS u JOIN j ON u.k = j.k",
            tables: vec![
                table(
                    "CREATE TABLE l(k BIGINT NOT NULL)",
                    10,
                    Some(column_stat(
                        r#"{"min": 1, "max": 3, "ndv": 3, "null_count": 0}"#,
                    )?),
                ),
                table(
                    "CREATE TABLE r(k BIGINT NOT NULL)",
                    20,
                    Some(column_stat(
                        r#"{"min": 4, "max": 8, "ndv": 5, "null_count": 0}"#,
                    )?),
                ),
                table(
                    "CREATE TABLE j(k BIGINT NOT NULL)",
                    100,
                    Some(column_stat(
                        r#"{"min": 1, "max": 10, "ndv": 10, "null_count": 0}"#,
                    )?),
                ),
            ],
            targets: vec![
                StatTarget {
                    path: vec![Child::Left],
                    operator: RelOp::Join,
                },
                StatTarget {
                    path: vec![Child::Left, Child::Right],
                    operator: RelOp::UnionAll,
                },
            ],
        },
    ])
}

#[tokio::test(flavor = "multi_thread", worker_threads = 1)]
async fn test_union_all_statistics_golden() -> Result<()> {
    let mut output = Vec::new();
    for case in cases()? {
        write_case(&mut output, case).await?;
    }

    let mut file = open_golden_file("optimizer", "union_all.txt")?;
    file.write_all(&output)?;
    Ok(())
}
