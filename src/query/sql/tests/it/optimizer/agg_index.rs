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

use databend_common_catalog::table_context::TableContextPartitionStats;
use databend_common_catalog::table_context::TableContextSettings;
use databend_common_exception::Result;
use databend_common_sql::FormatOptions;
use databend_common_sql::MetadataIdHumanizer;
use databend_common_sql::optimizer::build_agg_index_plan_for_table;
use databend_common_sql::optimizer::ir::SExpr;
use databend_common_sql::optimizer::ir::SExprVisitor;
use databend_common_sql::optimizer::ir::VisitAction;
use databend_common_sql::plans::AggIndexInfo;
use databend_common_sql::plans::BoundColumnRef;
use databend_common_sql::plans::Operator;
use databend_common_sql::plans::Plan;
use databend_common_sql::plans::RelOperator;
use databend_common_sql::plans::Visitor;
use databend_common_sql_test_support::TestCase;
use databend_common_sql_test_support::TestCaseRunner;
use parking_lot::RwLock;

use crate::framework::LiteTableContext;
use crate::framework::golden::open_golden_file;
use crate::framework::golden::write_case_title;

struct AggIndexLiteRunner {
    ctx: Arc<LiteTableContext>,
    agg_index_table: &'static str,
    agg_index_sqls: &'static [&'static str],
}

struct AggIndexLiteCase {
    case: TestCase,
    agg_index_table: &'static str,
    agg_index_sqls: &'static [&'static str],
    is_matched: bool,
    index_selection: &'static [&'static str],
    rewritten_predicates: &'static [&'static str],
}

impl TestCaseRunner for AggIndexLiteRunner {
    async fn bind_sql(&self, sql: &str) -> Result<Plan> {
        self.ctx.bind_sql(sql).await
    }

    async fn optimize_plan(&self, plan: Plan) -> Result<Plan> {
        if let Plan::Query { metadata, .. } = &plan {
            let mut agg_index_plans = Vec::with_capacity(self.agg_index_sqls.len());
            let table_index = {
                let metadata_ref = metadata.read();
                metadata_ref
                    .tables()
                    .iter()
                    .find(|table| table.name() == self.agg_index_table)
                    .map(|table| table.index())
                    .expect("agg index table should exist in query metadata")
            };
            for (index_id, sql) in self.agg_index_sqls.iter().enumerate() {
                let index_metadata = Arc::new(RwLock::new(metadata.read().clone()));
                let index_plan = self.ctx.bind_sql(sql).await?;
                let Plan::Query { s_expr, .. } = index_plan else {
                    unreachable!("agg index sql must bind to a query plan");
                };
                agg_index_plans.push(build_agg_index_plan_for_table(
                    self.ctx.clone(),
                    None,
                    index_metadata,
                    table_index,
                    index_id as u64,
                    sql.to_string(),
                    *s_expr,
                )?);
            }
            metadata
                .write()
                .add_agg_indices(table_index, agg_index_plans);
        }

        self.ctx.optimize_plan(plan).await
    }
}

fn agg_index_test_case(name: &'static str, sql: &'static str) -> TestCase {
    TestCase {
        name: name.to_string(),
        sql: sql.to_string(),
        table_stats: HashMap::new(),
        column_stats: HashMap::new(),
        histogram_stats: HashMap::new(),
        auto_stats: false,
        stem: name.to_string(),
        subdir: None,
        node_num: None,
        tables: HashMap::from([(
            "t".to_string(),
            "create table t(a int, b int, c int)".to_string(),
        )]),
    }
}

fn agg_index_test_cases() -> Vec<AggIndexLiteCase> {
    vec![
        AggIndexLiteCase {
            case: agg_index_test_case(
                "agg_index_expression_exact_match",
                "select a + 1 from t where b > 1",
            ),
            agg_index_table: "t",
            agg_index_sqls: &["select a + 1, b from t where b > 0"],
            is_matched: true,
            index_selection: &["index_col_0 (#0)", "index_col_1 (#1)"],
            rewritten_predicates: &["gt(index_col_0 (#0), 1)"],
        },
        AggIndexLiteCase {
            case: agg_index_test_case(
                "agg_index_expression_structure_mismatch",
                "select a + 1 from t where b > 1",
            ),
            agg_index_table: "t",
            agg_index_sqls: &["select a + 2, b from t where b > 0"],
            is_matched: false,
            index_selection: &[],
            rewritten_predicates: &[],
        },
        AggIndexLiteCase {
            case: agg_index_test_case(
                "agg_index_expression_alias_output_match",
                "select x from (select a + 1 as x from t) s",
            ),
            agg_index_table: "t",
            agg_index_sqls: &["select a + 1 from t"],
            is_matched: true,
            index_selection: &["index_col_0 (#0)"],
            rewritten_predicates: &[],
        },
        AggIndexLiteCase {
            case: agg_index_test_case(
                "agg_index_aggregate_group_mismatch",
                "select sum(a) + 1 from t group by b",
            ),
            agg_index_table: "t",
            agg_index_sqls: &["select sum(a), c from t group by c"],
            is_matched: false,
            index_selection: &[],
            rewritten_predicates: &[],
        },
        AggIndexLiteCase {
            case: agg_index_test_case(
                "agg_index_aggregate_expression_output_match",
                "select sum(a) + 1 from t group by b",
            ),
            agg_index_table: "t",
            agg_index_sqls: &["select sum(a), b from t group by b"],
            is_matched: true,
            index_selection: &["index_col_0 (#0)", "index_col_1 (#1)"],
            rewritten_predicates: &[],
        },
        AggIndexLiteCase {
            case: agg_index_test_case(
                "agg_index_aggregate_distinct_mismatch",
                "select count(distinct a) from t",
            ),
            agg_index_table: "t",
            agg_index_sqls: &["select count(a) from t"],
            is_matched: false,
            index_selection: &[],
            rewritten_predicates: &[],
        },
        AggIndexLiteCase {
            case: agg_index_test_case(
                "agg_index_range_open_lower_cannot_cover_closed_query",
                "select sum(a), b from t where b >= 1 group by b",
            ),
            agg_index_table: "t",
            agg_index_sqls: &["select b, sum(a) from t where b > 1 group by b"],
            is_matched: false,
            index_selection: &[],
            rewritten_predicates: &[],
        },
        AggIndexLiteCase {
            case: agg_index_test_case(
                "agg_index_range_open_upper_cannot_cover_closed_query",
                "select sum(a), b from t where b <= 1 group by b",
            ),
            agg_index_table: "t",
            agg_index_sqls: &["select b, sum(a) from t where b < 1 group by b"],
            is_matched: false,
            index_selection: &[],
            rewritten_predicates: &[],
        },
        AggIndexLiteCase {
            case: agg_index_test_case(
                "agg_index_range_closed_index_can_cover_open_query",
                "select sum(a), b from t where b > 1 group by b",
            ),
            agg_index_table: "t",
            agg_index_sqls: &["select b, sum(a) from t where b >= 1 group by b"],
            is_matched: true,
            index_selection: &["index_col_0 (#0)", "index_col_1 (#1)"],
            rewritten_predicates: &["gt(index_col_0 (#0), 1)"],
        },
    ]
}

fn find_push_down_index_info_from_plan(plan: &Plan) -> Result<Option<&AggIndexInfo>> {
    let Plan::Query { s_expr, .. } = plan else {
        return Ok(None);
    };
    find_push_down_index_info(s_expr)
}

fn find_push_down_index_info(s_expr: &SExpr) -> Result<Option<&AggIndexInfo>> {
    match s_expr.plan() {
        RelOperator::Scan(scan) => Ok(scan.agg_index.as_ref()),
        _ => find_push_down_index_info(s_expr.child(0)?),
    }
}

fn format_selection(info: &AggIndexInfo) -> Vec<String> {
    let mut selection: Vec<_> = info
        .selection
        .iter()
        .map(|sel| databend_common_sql::format_scalar(&sel.scalar))
        .collect();
    selection.sort();
    selection
}

fn format_filter(info: &AggIndexInfo) -> Vec<String> {
    let mut predicates: Vec<_> = info
        .predicates
        .iter()
        .map(databend_common_sql::format_scalar)
        .collect();
    predicates.sort();
    predicates
}

fn find_scan(s_expr: &SExpr) -> Result<Option<&databend_common_sql::plans::Scan>> {
    match s_expr.plan() {
        RelOperator::Scan(scan) => Ok(Some(scan)),
        _ => find_scan(s_expr.child(0)?),
    }
}

fn collect_scan_table_indexes(s_expr: &SExpr, table_indexes: &mut Vec<usize>) {
    if let RelOperator::Scan(scan) = s_expr.plan() {
        table_indexes.push(scan.table_index);
    }

    for child in s_expr.children() {
        collect_scan_table_indexes(child, table_indexes);
    }
}

fn describe_table_columns(
    metadata: &databend_common_sql::Metadata,
    table_index: usize,
) -> Vec<String> {
    metadata
        .columns_by_table_index(table_index)
        .into_iter()
        .map(|column| match column {
            databend_common_sql::ColumnEntry::BaseTableColumn(col) => {
                format!(
                    "{} idx={} table_index={}",
                    col.column_name, col.column_index, col.table_index
                )
            }
            databend_common_sql::ColumnEntry::InternalColumn(col) => {
                format!(
                    "{} idx={} table_index={}",
                    col.internal_column.column_name(),
                    col.column_index,
                    col.table_index
                )
            }
            databend_common_sql::ColumnEntry::VirtualColumn(col) => {
                format!(
                    "{} idx={} table_index={}",
                    col.column_name, col.column_index, col.table_index
                )
            }
            databend_common_sql::ColumnEntry::DerivedColumn(_) => unreachable!(),
        })
        .collect()
}

fn describe_physical_table_columns(
    metadata: &databend_common_sql::Metadata,
    table_index: usize,
) -> Vec<String> {
    metadata
        .columns_by_table_index(table_index)
        .into_iter()
        .map(|column| match column {
            databend_common_sql::ColumnEntry::BaseTableColumn(col) => {
                format!(
                    "{} column_id={} position={:?} path={:?} virtual={:?}",
                    col.column_name,
                    col.column_id,
                    col.column_position,
                    col.path_indices,
                    col.virtual_expr
                )
            }
            databend_common_sql::ColumnEntry::InternalColumn(col) => {
                format!(
                    "{} internal column_id={}",
                    col.internal_column.column_name(),
                    col.internal_column.column_id()
                )
            }
            databend_common_sql::ColumnEntry::VirtualColumn(col) => {
                format!(
                    "{} virtual source_column_id={} column_id={}",
                    col.column_name, col.source_column_id, col.column_id
                )
            }
            databend_common_sql::ColumnEntry::DerivedColumn(_) => unreachable!(),
        })
        .collect()
}

fn describe_bound_columns(s_expr: &SExpr) -> Result<Vec<String>> {
    struct BoundColumnCollector {
        columns: Vec<String>,
    }

    impl SExprVisitor for BoundColumnCollector {
        fn visit(&mut self, expr: &SExpr) -> Result<VisitAction> {
            for scalar in expr.plan().scalar_expr_iter() {
                let mut scalar_collector = ScalarBoundColumnCollector {
                    columns: &mut self.columns,
                };
                scalar_collector.visit(scalar)?;
            }
            Ok(VisitAction::Continue)
        }
    }

    struct ScalarBoundColumnCollector<'a> {
        columns: &'a mut Vec<String>,
    }

    impl<'a, 'b> Visitor<'a> for ScalarBoundColumnCollector<'b> {
        fn visit_bound_column_ref(&mut self, col: &'a BoundColumnRef) -> Result<()> {
            if col.column.table_index.is_none() {
                return Ok(());
            }
            self.columns.push(format!(
                "db={:?} table={:?} col={} idx={} table_index={:?}",
                col.column.database_name,
                col.column.table_name,
                col.column.column_name,
                col.column.index,
                col.column.table_index
            ));
            Ok(())
        }
    }

    let mut collector = BoundColumnCollector {
        columns: Vec::new(),
    };
    let _ = s_expr.accept(&mut collector)?;
    collector.columns.sort();
    collector.columns.dedup();
    Ok(collector.columns)
}

async fn setup_tables(ctx: &Arc<LiteTableContext>, case: &TestCase) -> Result<()> {
    for sql in case.tables.values() {
        for statement in sql.split(';').filter(|s| !s.trim().is_empty()) {
            ctx.register_table_sql(statement).await?;
        }
    }
    Ok(())
}

async fn optimize_with_debug_agg_index(
    ctx: &Arc<LiteTableContext>,
    query_sql: &str,
    index_sql: &str,
) -> Result<Plan> {
    let plan = ctx.bind_sql(query_sql).await?;
    let metadata = match &plan {
        Plan::Query { metadata, .. } => metadata.clone(),
        _ => unreachable!("query sql must bind to query plan"),
    };

    let index_plan = ctx.bind_sql(index_sql).await?;
    let Plan::Query { s_expr, .. } = index_plan else {
        unreachable!("index sql must bind to query plan");
    };
    let table_index = {
        let metadata_guard = metadata.read();
        metadata_guard
            .tables()
            .iter()
            .find(|table| table.name() == "t")
            .map(|table| table.index())
            .expect("query metadata should contain table t")
    };
    let agg_index_plan = build_agg_index_plan_for_table(
        ctx.clone(),
        None,
        Arc::new(RwLock::new(metadata.read().clone())),
        table_index,
        0,
        index_sql.to_string(),
        *s_expr,
    )?;
    metadata
        .write()
        .add_agg_indices(table_index, vec![agg_index_plan]);

    ctx.optimize_plan(plan).await
}

async fn create_auto_bound_agg_index_ctx(index_sql: &str) -> Result<Arc<LiteTableContext>> {
    let ctx = LiteTableContext::create_isolated().await?;
    ctx.configure_for_optimizer_case(false)?;
    ctx.get_settings()
        .set_setting("enable_aggregating_index_scan".to_string(), "1".to_string())?;
    ctx.set_can_scan_from_agg_index(true);
    ctx.register_table_sql("create table t(a int, b int, c int)")
        .await?;
    ctx.register_agg_index("default", "t", "idx1", index_sql)?;
    Ok(ctx)
}

async fn write_auto_bound_golden_case(
    file: &mut impl Write,
    name: &str,
    description: &str,
    table_sqls: &[&str],
    agg_indexes: &[(&str, &str, &str, &str)],
    query_sql: &str,
) -> Result<()> {
    let ctx = LiteTableContext::create_isolated().await?;
    ctx.configure_for_optimizer_case(false)?;
    ctx.get_settings()
        .set_setting("enable_aggregating_index_scan".to_string(), "1".to_string())?;
    ctx.set_can_scan_from_agg_index(true);
    for table_sql in table_sqls {
        ctx.register_table_sql(table_sql).await?;
    }
    for (database, table, name, sql) in agg_indexes {
        ctx.register_agg_index(database, table, name, sql)?;
    }

    let raw_plan = ctx.bind_sql(query_sql).await?;
    let optimized_plan = ctx.optimize_plan(raw_plan.clone()).await?;
    let Plan::Query {
        s_expr, metadata, ..
    } = &raw_plan
    else {
        unreachable!("golden query must bind to query plan")
    };

    write_case_title(file, name, description)?;
    writeln!(file, "sql: {query_sql}")?;
    writeln!(file, "raw_plan:")?;
    writeln!(file, "{}", raw_plan.format_indent(Default::default())?)?;

    let mut scan_table_indexes = Vec::new();
    collect_scan_table_indexes(s_expr, &mut scan_table_indexes);
    scan_table_indexes.sort_unstable();
    scan_table_indexes.dedup();

    writeln!(file, "catalog_bound_agg_indexes:")?;
    for table_index in scan_table_indexes {
        let table_name = {
            let metadata_ref = metadata.read();
            let table = metadata_ref.table(table_index);
            format!("{}.{} (#{}):", table.database(), table.name(), table_index)
        };
        writeln!(file, "- table: {table_name}")?;

        let mut index_plans = metadata
            .read()
            .get_agg_indices(table_index)
            .map(|plans| plans.to_vec())
            .unwrap_or_default();
        index_plans.sort_by(|left, right| left.sql.cmp(&right.sql));
        if index_plans.is_empty() {
            writeln!(file, "  indexes: []")?;
        }
        for index_plan in index_plans {
            writeln!(file, "  - sql: {}", index_plan.sql)?;
            writeln!(file, "    s_expr:")?;
            let index_metadata = index_plan.metadata.read();
            let humanizer = MetadataIdHumanizer::new(&index_metadata, FormatOptions::default());
            for line in index_plan
                .s_expr
                .to_format_tree(&humanizer)?
                .format_pretty()?
                .lines()
            {
                writeln!(file, "      {line}")?;
            }
        }
    }

    writeln!(file, "optimized_plan:")?;
    writeln!(
        file,
        "{}",
        optimized_plan.format_indent(Default::default())?
    )?;
    writeln!(file)?;
    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn test_lite_agg_index_optimizer_cases() -> Result<()> {
    for test in agg_index_test_cases() {
        let ctx = LiteTableContext::create_isolated().await?;
        ctx.configure_for_optimizer_case(test.case.auto_stats)?;
        setup_tables(&ctx, &test.case).await?;

        let runner = AggIndexLiteRunner {
            ctx: ctx.clone(),
            agg_index_table: test.agg_index_table,
            agg_index_sqls: test.agg_index_sqls,
        };
        let plan = runner.bind_sql(&test.case.sql).await?;
        let optimized_plan = runner.optimize_plan(plan).await?;
        let agg_index = find_push_down_index_info_from_plan(&optimized_plan)?;

        assert_eq!(
            test.is_matched,
            agg_index.is_some(),
            "case: {}, sql: {}, indexes: {:?}",
            test.case.name,
            test.case.sql,
            test.agg_index_sqls
        );

        if let Some(agg_index) = agg_index {
            let mut expected_selection: Vec<_> = test
                .index_selection
                .iter()
                .map(|s| (*s).to_string())
                .collect();
            expected_selection.sort();
            assert_eq!(
                expected_selection,
                format_selection(agg_index),
                "case: {} selection mismatch",
                test.case.name
            );

            let mut expected_predicates: Vec<_> = test
                .rewritten_predicates
                .iter()
                .map(|s| (*s).to_string())
                .collect();
            expected_predicates.sort();
            assert_eq!(
                expected_predicates,
                format_filter(agg_index),
                "case: {} predicate mismatch",
                test.case.name
            );
        }
    }

    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 1)]
async fn test_lite_agg_index_auto_bound_golden() -> Result<()> {
    let mut file = open_golden_file("optimizer", "agg_index.txt")?;

    write_auto_bound_golden_case(
        &mut file,
        "catalog_bound_single_table",
        "bind_query_index should attach a catalog-bound aggregating index to the query table.",
        &["create table t(a int, b int, c int)"],
        &[(
            "default",
            "t",
            "idx1",
            "select b, sum(a) from t where b > 1 group by b",
        )],
        "select sum(a), b from t where b > 1 group by b",
    )
    .await?;

    write_auto_bound_golden_case(
        &mut file,
        "catalog_bound_tuple_inner_column",
        "Tuple inner fields should keep their base column path identity when matching an aggregating index.",
        &["create table t(x tuple(a int, b int), k int)"],
        &[(
            "default",
            "t",
            "idx1",
            "select k, sum(x:a) from t where k > 1 group by k",
        )],
        "select sum(x:a), k from t where k > 1 group by k",
    )
    .await?;

    write_auto_bound_golden_case(
        &mut file,
        "catalog_bound_multiple_indexes_single_table",
        "A table with multiple catalog-bound agg indexes should keep each bound index plan and rewrite with a matching one.",
        &["create table t(a int, b int, c int)"],
        &[
            (
                "default",
                "t",
                "idx1",
                "select sum(a), c from t group by c",
            ),
            (
                "default",
                "t",
                "idx2",
                "select b, sum(a) from t where b > 1 group by b",
            ),
        ],
        "select sum(a), b from t where b > 1 group by b",
    )
    .await?;

    write_auto_bound_golden_case(
        &mut file,
        "catalog_bound_cross_database_table",
        "The persisted index SQL should bind against the canonical table from the matching database.",
        &["create table other_db.t(a int, b int, c int)"],
        &[(
            "other_db",
            "t",
            "idx1",
            "select b, sum(a) from other_db.t where b > 1 group by b",
        )],
        "select sum(a), b from other_db.t where b > 1 group by b",
    )
    .await?;

    write_auto_bound_golden_case(
        &mut file,
        "catalog_bound_repeated_table_scan",
        "Each scan of the same table should receive an agg index bound to its own canonical table index.",
        &["create table t(a int, b int, c int)"],
        &[(
            "default",
            "t",
            "idx1",
            "select b, sum(a) from t where b > 1 group by b",
        )],
        "select l.a, r.c from t as l join t as r on l.b = r.b",
    )
    .await?;

    write_auto_bound_golden_case(
        &mut file,
        "rewrite_range_compensation",
        "A query with a narrower range can use a wider index and add a compensating predicate.",
        &["create table t(a int, b int, c int)"],
        &[(
            "default",
            "t",
            "idx1",
            "select b, sum(a) from t where b > 0 group by b",
        )],
        "select sum(a), b from t where b > 1 group by b",
    )
    .await?;

    write_auto_bound_golden_case(
        &mut file,
        "rewrite_range_open_lower_rejected",
        "An index with an open lower bound cannot cover a query with the same closed lower bound.",
        &["create table t(a int, b int, c int)"],
        &[(
            "default",
            "t",
            "idx1",
            "select b, sum(a) from t where b > 1 group by b",
        )],
        "select sum(a), b from t where b >= 1 group by b",
    )
    .await?;

    write_auto_bound_golden_case(
        &mut file,
        "rewrite_range_open_upper_rejected",
        "An index with an open upper bound cannot cover a query with the same closed upper bound.",
        &["create table t(a int, b int, c int)"],
        &[(
            "default",
            "t",
            "idx1",
            "select b, sum(a) from t where b < 1 group by b",
        )],
        "select sum(a), b from t where b <= 1 group by b",
    )
    .await?;

    write_auto_bound_golden_case(
        &mut file,
        "rewrite_range_closed_index_compensation",
        "A query with an open range can use an index with the same closed bound and add a compensating predicate.",
        &["create table t(a int, b int, c int)"],
        &[(
            "default",
            "t",
            "idx1",
            "select b, sum(a) from t where b >= 1 group by b",
        )],
        "select sum(a), b from t where b > 1 group by b",
    )
    .await?;

    write_auto_bound_golden_case(
        &mut file,
        "rewrite_residual_compensation",
        "Residual predicates can be compensated when the index outputs the required expression.",
        &["create table t(a int, b int, c int)"],
        &[("default", "t", "idx1", "select a + 1, c from t")],
        "select a + 1 from t where c + 1 > 3",
    )
    .await?;

    write_auto_bound_golden_case(
        &mut file,
        "rewrite_equality_class_compensation",
        "Column equivalence predicates must be covered by the query before range compensation is applied.",
        &["create table t(a int, b int, c int)"],
        &[(
            "default",
            "t",
            "idx1",
            "select a + 1, b, c from t where b = c",
        )],
        "select a + 1 from t where b = c and b > 1",
    )
    .await?;

    write_auto_bound_golden_case(
        &mut file,
        "rewrite_order_by_selection",
        "ORDER BY columns must be selected from the index even when they are not query outputs.",
        &["create table t(a int, b int, c int)"],
        &[("default", "t", "idx1", "select a + 1, b from t where b > 0")],
        "select a + 1 from t where b > 1 order by b",
    )
    .await?;

    write_auto_bound_golden_case(
        &mut file,
        "rewrite_aggregate_group_mismatch",
        "Aggregate indexes with different group items should remain bound but not rewrite the scan.",
        &["create table t(a int, b int, c int)"],
        &[(
            "default",
            "t",
            "idx1",
            "select sum(a), c from t group by c",
        )],
        "select sum(a) + 1 from t group by b",
    )
    .await?;

    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn test_lite_agg_index_auto_bound_matches_manual_injection() -> Result<()> {
    let index_sql = "select b, sum(a) from t where b > 1 group by b";
    for query_sql in [
        "select sum(a), b from t where b > 1 group by b",
        "select b from t where b > 1 group by b",
        "select sum(a) + 1 from t where b > 1 group by b",
    ] {
        let auto_ctx = create_auto_bound_agg_index_ctx(index_sql).await?;
        let auto_plan = auto_ctx
            .optimize_plan(auto_ctx.bind_sql(query_sql).await?)
            .await?;
        let auto_info = find_push_down_index_info_from_plan(&auto_plan)?
            .expect("auto-bound agg index should match");

        let manual_ctx = LiteTableContext::create_isolated().await?;
        manual_ctx.configure_for_optimizer_case(false)?;
        manual_ctx
            .register_table_sql("create table t(a int, b int, c int)")
            .await?;
        let manual_plan = optimize_with_debug_agg_index(&manual_ctx, query_sql, index_sql).await?;
        let manual_info = find_push_down_index_info_from_plan(&manual_plan)?
            .expect("manually injected agg index should match");

        assert_eq!(format_selection(auto_info), format_selection(manual_info));
        assert_eq!(format_filter(auto_info), format_filter(manual_info));
    }

    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn test_lite_agg_index_auto_bound_index_plan_is_normalized() -> Result<()> {
    let index_sql = "select b, sum(a) from t where b > 1 group by b";
    let query_sql = "select sum(a), b from t where b > 1 group by b";
    let ctx = create_auto_bound_agg_index_ctx(index_sql).await?;
    let plan = ctx.bind_sql(query_sql).await?;

    let (query_scan, metadata_ref) = match &plan {
        Plan::Query {
            s_expr, metadata, ..
        } => (
            find_scan(s_expr)?.expect("query scan should exist"),
            metadata.clone(),
        ),
        _ => unreachable!("debug test must bind to query plan"),
    };
    let metadata = metadata_ref.read();

    let agg_index = metadata
        .get_agg_indices(query_scan.table_index)
        .expect("agg index should be bound from catalog")
        .first()
        .expect("agg index should not be empty");
    let index_metadata = agg_index.metadata.read();
    let index_scan = find_scan(&agg_index.s_expr)?.expect("index scan should exist");
    let index_columns = describe_table_columns(&index_metadata, index_scan.table_index);
    let query_physical_columns = describe_physical_table_columns(&metadata, query_scan.table_index);
    let index_physical_columns =
        describe_physical_table_columns(&index_metadata, index_scan.table_index);
    let index_bound_columns = describe_bound_columns(&agg_index.s_expr)?;

    assert_eq!(
        index_scan.table_index, query_scan.table_index,
        "catalog-bound agg index should preserve the query scan table index"
    );
    assert_eq!(
        index_physical_columns, query_physical_columns,
        "catalog-bound agg index table metadata should preserve the target table's physical columns"
    );
    assert_eq!(
        index_columns,
        vec![
            format!("a idx=0 table_index={}", index_scan.table_index),
            format!("b idx=1 table_index={}", index_scan.table_index),
            format!("c idx=2 table_index={}", index_scan.table_index),
        ],
        "catalog-bound agg index should allocate table column symbols inside index metadata"
    );
    assert!(
        !Arc::ptr_eq(&agg_index.metadata, &metadata_ref),
        "catalog-bound agg index should use metadata independent from the main query",
    );
    assert!(
        index_scan.statistics.table_stats.is_none(),
        "normalized catalog-bound agg index scan should clear table statistics"
    );
    assert!(
        index_scan.statistics.column_stats.is_empty(),
        "normalized catalog-bound agg index scan should clear column statistics"
    );
    assert!(
        index_scan.statistics.histograms.is_empty(),
        "normalized catalog-bound agg index scan should clear histograms"
    );
    assert_eq!(
        index_bound_columns,
        vec![
            format!(
                "db=Some(\"default\") table=Some(\"t\") col=a idx=0 table_index=Some({})",
                index_scan.table_index
            ),
            format!(
                "db=Some(\"default\") table=Some(\"t\") col=b idx=1 table_index=Some({})",
                index_scan.table_index
            ),
        ],
        "catalog-bound agg index raw plan should keep canonical bound column names inside index metadata",
    );

    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn test_lite_agg_index_auto_bound_uses_per_index_metadata() -> Result<()> {
    let ctx = LiteTableContext::create_isolated().await?;
    ctx.configure_for_optimizer_case(false)?;
    ctx.get_settings()
        .set_setting("enable_aggregating_index_scan".to_string(), "1".to_string())?;
    ctx.set_can_scan_from_agg_index(true);
    ctx.register_table_sql("create table t(a int, b int, c int)")
        .await?;
    ctx.register_agg_index(
        "default",
        "t",
        "idx1",
        "select b, sum(a) from t where b > 1 group by b",
    )?;
    ctx.register_agg_index(
        "default",
        "t",
        "idx2",
        "select b, max(c) from t where b > 2 group by b",
    )?;

    let plan = ctx
        .bind_sql("select sum(a), b from t where b > 1 group by b")
        .await?;
    let (query_metadata_ref, s_expr) = match &plan {
        Plan::Query {
            metadata, s_expr, ..
        } => (metadata.clone(), s_expr),
        _ => unreachable!("debug test must bind to query plan"),
    };
    let metadata = query_metadata_ref.read();
    let agg_indices = metadata
        .get_agg_indices(
            find_scan(s_expr)?
                .expect("query scan should exist")
                .table_index,
        )
        .expect("agg index should be bound from catalog");

    assert_eq!(agg_indices.len(), 2);
    assert!(
        !Arc::ptr_eq(&agg_indices[0].metadata, &query_metadata_ref),
        "catalog-bound agg index should keep metadata independent from the main query",
    );
    assert!(
        !Arc::ptr_eq(&agg_indices[1].metadata, &query_metadata_ref),
        "catalog-bound agg index should keep metadata independent from the main query",
    );
    assert!(
        !Arc::ptr_eq(&agg_indices[0].metadata, &agg_indices[1].metadata),
        "catalog-bound agg indices should keep per-index metadata for display",
    );

    Ok(())
}
