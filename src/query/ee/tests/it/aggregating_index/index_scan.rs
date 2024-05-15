// Copyright 2023 Databend Cloud
//
// Licensed under the Elastic License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     https://www.elastic.co/licensing/elastic-license
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use std::collections::HashMap;
use std::fmt::Display;
use std::sync::Arc;

use databend_common_base::base::tokio;
use databend_common_base::runtime::Runtime;
use databend_common_exception::Result;
use databend_common_expression::block_debug::pretty_format_blocks;
use databend_common_expression::DataBlock;
use databend_common_expression::SendableDataBlockStream;
use databend_common_expression::SortColumnDescription;
use databend_common_sql::optimizer::SExpr;
use databend_common_sql::planner::plans::Plan;
use databend_common_sql::plans::RelOperator;
use databend_common_sql::Planner;
use databend_enterprise_query::test_kits::context::EESetup;
use databend_query::interpreters::InterpreterFactory;
use databend_query::sessions::QueryContext;
use databend_query::test_kits::*;
use futures_util::TryStreamExt;

#[tokio::test(flavor = "multi_thread")]
async fn test_index_scan() -> Result<()> {
    test_index_scan_impl("parquet").await?;
    test_index_scan_impl("native").await
}

#[tokio::test(flavor = "multi_thread")]
async fn test_index_scan_two_agg_funcs() -> Result<()> {
    test_index_scan_two_agg_funcs_impl("parquet").await?;
    test_index_scan_two_agg_funcs_impl("native").await
}

#[tokio::test(flavor = "multi_thread")]
async fn test_projected_index_scan() -> Result<()> {
    test_projected_index_scan_impl("parquet").await?;
    test_projected_index_scan_impl("native").await
}

#[tokio::test(flavor = "multi_thread")]
async fn test_index_scan_with_count() -> Result<()> {
    test_index_scan_with_count_impl("parquet").await?;
    test_index_scan_with_count_impl("native").await
}

#[tokio::test(flavor = "multi_thread")]
async fn test_index_scan_agg_args_are_expression() -> Result<()> {
    test_index_scan_agg_args_are_expression_impl("parquet").await?;
    test_index_scan_agg_args_are_expression_impl("native").await
}

#[test]
fn test_fuzz() -> Result<()> {
    let runtime = Runtime::with_worker_threads(2, None)?;
    runtime.block_on(async {
        test_fuzz_impl("parquet", false).await?;
        test_fuzz_impl("native", false).await
    })?;
    Ok(())
}

#[test]
fn test_fuzz_with_spill() -> Result<()> {
    let runtime = Runtime::with_worker_threads(2, None)?;
    runtime.block_on(async {
        test_fuzz_impl("parquet", true).await?;
        test_fuzz_impl("native", true).await
    })?;
    Ok(())
}

async fn plan_sql(ctx: Arc<QueryContext>, sql: &str) -> Result<Plan> {
    let mut planner = Planner::new(ctx.clone());
    let (plan, _) = planner.plan_sql(sql).await?;

    Ok(plan)
}

async fn execute_sql(ctx: Arc<QueryContext>, sql: &str) -> Result<SendableDataBlockStream> {
    let plan = plan_sql(ctx.clone(), sql).await?;
    execute_plan(ctx, &plan).await
}

async fn execute_plan(ctx: Arc<QueryContext>, plan: &Plan) -> Result<SendableDataBlockStream> {
    let interpreter = InterpreterFactory::get(ctx.clone(), plan).await?;
    interpreter.execute(ctx).await
}

async fn drop_index(ctx: Arc<QueryContext>, index_name: &str) -> Result<()> {
    let sql = format!("DROP AGGREGATING INDEX {index_name}");
    let _ = execute_sql(ctx, &sql).await?;

    Ok(())
}

async fn test_index_scan_impl(format: &str) -> Result<()> {
    let fixture = TestFixture::setup_with_custom(EESetup::new()).await?;

    // Create table
    fixture
        .execute_command(&format!(
            "CREATE TABLE t (a int, b int, c int) storage_format = '{format}'"
        ))
        .await?;

    fixture
        .execute_command("INSERT INTO t VALUES (1,1,4), (1,2,1), (1,2,4), (2,2,5)")
        .await?;

    // Create index
    let index_name = "index1";

    fixture.execute_command(
        &format!("CREATE ASYNC AGGREGATING INDEX {index_name} AS SELECT b, SUM(a) from t WHERE c > 1 GROUP BY b"),
    )
        .await?;

    // Refresh Index
    fixture
        .execute_command(&format!("REFRESH AGGREGATING INDEX {index_name}"))
        .await?;

    // Query with index
    let ctx = fixture.new_query_ctx().await?;
    let plan = plan_sql(
        ctx.clone(),
        "SELECT b, SUM(a) from t WHERE c > 1 GROUP BY b",
    )
    .await?;

    assert!(is_index_scan_plan(&plan));

    expects_ok("Index scan", execute_plan(ctx, &plan).await, vec![
        "+----------+----------+",
        "| Column 0 | Column 1 |",
        "+----------+----------+",
        "| 1        | 1        |",
        "| 2        | 3        |",
        "+----------+----------+",
    ])
    .await?;

    let ctx = fixture.new_query_ctx().await?;
    let plan = plan_sql(ctx.clone(), "SELECT b from t WHERE c > 1 GROUP BY b").await?;

    assert!(is_index_scan_plan(&plan));

    expects_ok("Index scan", execute_plan(ctx, &plan).await, vec![
        "+----------+",
        "| Column 0 |",
        "+----------+",
        "| 1        |",
        "| 2        |",
        "+----------+",
    ])
    .await?;

    let ctx = fixture.new_query_ctx().await?;
    let plan = plan_sql(ctx.clone(), "SELECT SUM(a) from t WHERE c > 1 GROUP BY b").await?;

    assert!(is_index_scan_plan(&plan));

    expects_ok("Index scan", execute_plan(ctx, &plan).await, vec![
        "+----------+",
        "| Column 0 |",
        "+----------+",
        "| 1        |",
        "| 3        |",
        "+----------+",
    ])
    .await?;

    // Insert new data but not refresh index
    fixture
        .execute_command("INSERT INTO t VALUES (1,1,4), (1,2,1), (1,2,4), (2,2,5)")
        .await?;

    // Query with one fuse block and one index block
    let ctx = fixture.new_query_ctx().await?;
    let plan = plan_sql(
        ctx.clone(),
        "SELECT b, SUM(a) from t WHERE c > 1 GROUP BY b",
    )
    .await?;

    assert!(is_index_scan_plan(&plan));

    expects_ok("Index scan", execute_plan(ctx, &plan).await, vec![
        "+----------+----------+",
        "| Column 0 | Column 1 |",
        "+----------+----------+",
        "| 1        | 2        |",
        "| 2        | 6        |",
        "+----------+----------+",
    ])
    .await?;

    let ctx = fixture.new_query_ctx().await?;
    let plan = plan_sql(ctx.clone(), "SELECT b + 1 from t WHERE c > 1 GROUP BY b").await?;

    assert!(is_index_scan_plan(&plan));

    expects_ok("Index scan", execute_plan(ctx, &plan).await, vec![
        "+----------+",
        "| Column 0 |",
        "+----------+",
        "| 2        |",
        "| 3        |",
        "+----------+",
    ])
    .await?;

    let ctx = fixture.new_query_ctx().await?;
    let plan = plan_sql(
        ctx.clone(),
        "SELECT SUM(a) + 1 from t WHERE c > 1 GROUP BY b",
    )
    .await?;

    assert!(is_index_scan_plan(&plan));

    expects_ok("Index scan", execute_plan(ctx.clone(), &plan).await, vec![
        "+----------+",
        "| Column 0 |",
        "+----------+",
        "| 3        |",
        "| 7        |",
        "+----------+",
    ])
    .await?;

    drop_index(ctx, index_name).await?;

    Ok(())
}

async fn test_index_scan_two_agg_funcs_impl(format: &str) -> Result<()> {
    let fixture = TestFixture::setup_with_custom(EESetup::new()).await?;

    // Create table
    fixture
        .execute_command(&format!(
            "CREATE TABLE t (a int, b int, c int) storage_format = '{format}'"
        ))
        .await?;

    // Insert data
    fixture
        .execute_command("INSERT INTO t VALUES (1,1,4), (1,2,1), (1,2,4), (2,2,5)")
        .await?;

    // Create index
    let index_name = "index1";

    fixture.execute_command(
        &format!("CREATE ASYNC AGGREGATING INDEX {index_name} AS SELECT b, MAX(a), SUM(a) from t WHERE c > 1 GROUP BY b"),
    )
        .await?;

    // Refresh Index
    fixture
        .execute_command(&format!("REFRESH AGGREGATING INDEX {index_name}"))
        .await?;

    // Query with index
    // sum
    let ctx = fixture.new_query_ctx().await?;
    let plan = plan_sql(
        ctx.clone(),
        "SELECT b, SUM(a) from t WHERE c > 1 GROUP BY b",
    )
    .await?;

    assert!(is_index_scan_plan(&plan));

    expects_ok("Index scan", execute_plan(ctx, &plan).await, vec![
        "+----------+----------+",
        "| Column 0 | Column 1 |",
        "+----------+----------+",
        "| 1        | 1        |",
        "| 2        | 3        |",
        "+----------+----------+",
    ])
    .await?;

    // sum and max
    let ctx = fixture.new_query_ctx().await?;
    let plan = plan_sql(
        ctx.clone(),
        "SELECT b, SUM(a), MAX(a) from t WHERE c > 1 GROUP BY b",
    )
    .await?;

    assert!(is_index_scan_plan(&plan));

    expects_ok("Index scan", execute_plan(ctx, &plan).await, vec![
        "+----------+----------+----------+",
        "| Column 0 | Column 1 | Column 2 |",
        "+----------+----------+----------+",
        "| 1        | 1        | 1        |",
        "| 2        | 3        | 2        |",
        "+----------+----------+----------+",
    ])
    .await?;

    // Insert new data but not refresh index
    fixture
        .execute_command("INSERT INTO t VALUES (1,1,4), (1,2,1), (1,2,4), (2,2,5)")
        .await?;

    // Query with one fuse block and one index block
    // sum
    let ctx = fixture.new_query_ctx().await?;
    let plan = plan_sql(
        ctx.clone(),
        "SELECT b, SUM(a) from t WHERE c > 1 GROUP BY b",
    )
    .await?;

    assert!(is_index_scan_plan(&plan));

    expects_ok("Index scan", execute_plan(ctx, &plan).await, vec![
        "+----------+----------+",
        "| Column 0 | Column 1 |",
        "+----------+----------+",
        "| 1        | 2        |",
        "| 2        | 6        |",
        "+----------+----------+",
    ])
    .await?;

    // sum and max
    let ctx = fixture.new_query_ctx().await?;
    let plan = plan_sql(
        ctx.clone(),
        "SELECT b, SUM(a), MAX(a) from t WHERE c > 1 GROUP BY b",
    )
    .await?;

    assert!(is_index_scan_plan(&plan));

    expects_ok("Index scan", execute_plan(ctx.clone(), &plan).await, vec![
        "+----------+----------+----------+",
        "| Column 0 | Column 1 | Column 2 |",
        "+----------+----------+----------+",
        "| 1        | 2        | 1        |",
        "| 2        | 6        | 2        |",
        "+----------+----------+----------+",
    ])
    .await?;

    drop_index(ctx, index_name).await?;

    Ok(())
}

async fn test_projected_index_scan_impl(format: &str) -> Result<()> {
    let fixture = TestFixture::setup_with_custom(EESetup::new()).await?;

    // Create table
    fixture
        .execute_command(&format!(
            "CREATE TABLE t (a int, b int, c int) storage_format = '{format}'"
        ))
        .await?;

    // Insert data
    fixture
        .execute_command("INSERT INTO t VALUES (1,1,4), (1,2,1), (1,2,4), (2,2,5)")
        .await?;

    // Create index
    let index_name = "index1";
    fixture.execute_command(
        &format!("CREATE ASYNC AGGREGATING INDEX {index_name} AS SELECT b, MAX(a), SUM(a) from t WHERE c > 1 GROUP BY b"),
    )
        .await?;

    // Refresh Index
    fixture
        .execute_command(&format!("REFRESH AGGREGATING INDEX {index_name}"))
        .await?;

    // Query with index
    // sum
    let ctx = fixture.new_query_ctx().await?;
    let plan = plan_sql(ctx.clone(), "SELECT b from t WHERE c > 1 GROUP BY b").await?;

    assert!(is_index_scan_plan(&plan));

    expects_ok("Index scan", execute_plan(ctx, &plan).await, vec![
        "+----------+",
        "| Column 0 |",
        "+----------+",
        "| 1        |",
        "| 2        |",
        "+----------+",
    ])
    .await?;

    // sum and max
    let ctx = fixture.new_query_ctx().await?;
    let plan = plan_sql(
        ctx.clone(),
        "SELECT b, SUM(a) from t WHERE c > 1 GROUP BY b",
    )
    .await?;

    assert!(is_index_scan_plan(&plan));

    expects_ok("Index scan", execute_plan(ctx, &plan).await, vec![
        "+----------+----------+",
        "| Column 0 | Column 1 |",
        "+----------+----------+",
        "| 1        | 1        |",
        "| 2        | 3        |",
        "+----------+----------+",
    ])
    .await?;

    // Insert new data but not refresh index
    fixture
        .execute_command("INSERT INTO t VALUES (1,1,4), (1,2,1), (1,2,4), (2,2,5)")
        .await?;

    // Query with one fuse block and one index block
    // sum
    let ctx = fixture.new_query_ctx().await?;
    let plan = plan_sql(
        ctx.clone(),
        "SELECT b, SUM(a) from t WHERE c > 1 GROUP BY b",
    )
    .await?;

    assert!(is_index_scan_plan(&plan));

    expects_ok("Index scan", execute_plan(ctx, &plan).await, vec![
        "+----------+----------+",
        "| Column 0 | Column 1 |",
        "+----------+----------+",
        "| 1        | 2        |",
        "| 2        | 6        |",
        "+----------+----------+",
    ])
    .await?;

    // sum and max
    let ctx = fixture.new_query_ctx().await?;
    let plan = plan_sql(
        ctx.clone(),
        "SELECT b, SUM(a) from t WHERE c > 1 GROUP BY b",
    )
    .await?;

    assert!(is_index_scan_plan(&plan));

    expects_ok("Index scan", execute_plan(ctx.clone(), &plan).await, vec![
        "+----------+----------+",
        "| Column 0 | Column 1 |",
        "+----------+----------+",
        "| 1        | 2        |",
        "| 2        | 6        |",
        "+----------+----------+",
    ])
    .await?;

    drop_index(ctx, index_name).await?;

    Ok(())
}

async fn test_index_scan_with_count_impl(format: &str) -> Result<()> {
    let fixture = TestFixture::setup_with_custom(EESetup::new()).await?;

    // Create table
    fixture
        .execute_command(&format!(
            "CREATE TABLE t (a string) storage_format = '{format}'"
        ))
        .await?;

    // Insert data
    fixture
        .execute_command("INSERT INTO t VALUES ('1'), ('2')")
        .await?;

    // Create index
    let index_name = "index1";

    fixture
        .execute_command(&format!(
            "CREATE ASYNC AGGREGATING INDEX {index_name} AS SELECT a, COUNT(*) from t GROUP BY a"
        ))
        .await?;

    // Refresh Index
    fixture
        .execute_command(&format!("REFRESH AGGREGATING INDEX {index_name}"))
        .await?;

    // Query with index
    let ctx = fixture.new_query_ctx().await?;
    let plan = plan_sql(ctx.clone(), "SELECT a, COUNT(*) from t GROUP BY a").await?;

    assert!(is_index_scan_plan(&plan));

    expects_ok("Index scan", execute_plan(ctx, &plan).await, vec![
        "+----------+----------+",
        "| Column 0 | Column 1 |",
        "+----------+----------+",
        "| '1'      | 1        |",
        "| '2'      | 1        |",
        "+----------+----------+",
    ])
    .await?;

    let ctx = fixture.new_query_ctx().await?;
    drop_index(ctx, index_name).await?;

    Ok(())
}

async fn test_index_scan_agg_args_are_expression_impl(format: &str) -> Result<()> {
    let fixture = TestFixture::setup_with_custom(EESetup::new()).await?;

    // Create table
    fixture
        .execute_command(&format!(
            "CREATE TABLE t (a string) storage_format = '{format}'"
        ))
        .await?;

    // Insert data
    fixture
        .execute_command("INSERT INTO t VALUES ('1'), ('21'), ('231')")
        .await?;

    // Create index
    let index_name = "index1";
    fixture.execute_command(
        &format!("CREATE ASYNC AGGREGATING INDEX {index_name} AS SELECT SUBSTRING(a, 1, 1) as s, sum(length(a)), min(a) from t GROUP BY s"),
    )
        .await?;

    // Refresh Index
    fixture
        .execute_command(&format!("REFRESH AGGREGATING INDEX {index_name}"))
        .await?;

    // Query with index
    let ctx = fixture.new_query_ctx().await?;
    let plan = plan_sql(
        ctx.clone(),
        "SELECT SUBSTRING(a, 1, 1) as s, sum(length(a)), min(a) from t GROUP BY s",
    )
    .await?;

    assert!(is_index_scan_plan(&plan));

    expects_ok("Index scan", execute_plan(ctx, &plan).await, vec![
        "+----------+----------+----------+",
        "| Column 0 | Column 1 | Column 2 |",
        "+----------+----------+----------+",
        "| '1'      | 1        | '1'      |",
        "| '2'      | 5        | '21'     |",
        "+----------+----------+----------+",
    ])
    .await?;

    let ctx = fixture.new_query_ctx().await?;
    drop_index(ctx, index_name).await?;

    Ok(())
}

fn is_index_scan_plan(plan: &Plan) -> bool {
    if let Plan::Query { s_expr, .. } = plan {
        is_index_scan_sexpr(s_expr.as_ref())
    } else {
        false
    }
}

fn is_index_scan_sexpr(s_expr: &SExpr) -> bool {
    if let RelOperator::Scan(scan) = s_expr.plan() {
        scan.agg_index.is_some()
    } else {
        s_expr.children().any(is_index_scan_sexpr)
    }
}

struct FuzzParams {
    query_sql: String,
    index_sql: String,
    is_index_scan: bool,
    num_blocks: usize,
    num_rows_per_block: usize,
    index_block_ratio: f64,
}

impl Display for FuzzParams {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        write!(
            f,
            "Query: {}\nIndex: {}\nIsIndexScan: {}\nNumBlocks: {}\nNumRowsPerBlock: {}\nIndexBlockRatio: {}\n",
            self.query_sql,
            self.index_sql,
            self.is_index_scan,
            self.num_blocks,
            self.num_rows_per_block,
            self.index_block_ratio,
        )
    }
}

async fn fuzz(ctx: Arc<QueryContext>, params: FuzzParams) -> Result<()> {
    let fuzz_info = params.to_string();
    let FuzzParams {
        query_sql,
        index_sql,
        is_index_scan,
        num_blocks,
        index_block_ratio,
        ..
    } = params;
    let num_index_blocks = (num_blocks as f64 * index_block_ratio) as usize;

    // Create agg index
    let _ = execute_sql(
        ctx.clone(),
        &format!("CREATE ASYNC AGGREGATING INDEX index AS {index_sql}"),
    )
    .await?;

    let plan = plan_sql(ctx.clone(), &query_sql).await?;
    if !is_index_scan_plan(&plan) {
        assert!(!is_index_scan, "{}", fuzz_info);
        // Clear
        drop_index(ctx.clone(), "index").await?;
        return Ok(());
    }
    assert!(is_index_scan, "{}", fuzz_info);

    // Get query result
    let expect: Vec<DataBlock> = execute_sql(ctx.clone(), &query_sql)
        .await?
        .try_collect()
        .await?;

    // Refresh index
    if num_index_blocks > 0 {
        let _ = execute_sql(
            ctx.clone(),
            &format!("REFRESH AGGREGATING INDEX index LIMIT {num_index_blocks}"),
        )
        .await?;
    }

    // Get index scan query result
    let actual: Vec<DataBlock> = execute_sql(ctx.clone(), &query_sql)
        .await?
        .try_collect()
        .await?;

    let expect = expect
        .iter()
        .filter(|b| !b.is_empty())
        .cloned()
        .collect::<Vec<_>>();
    let actual = actual
        .iter()
        .filter(|b| !b.is_empty())
        .cloned()
        .collect::<Vec<_>>();

    if expect.is_empty() {
        assert!(actual.is_empty());
        // Clear
        drop_index(ctx.clone(), "index").await?;
        return Ok(());
    }

    assert!(!actual.is_empty(), "{}", fuzz_info);

    let expect = DataBlock::concat(&expect)?;
    let expect = DataBlock::sort(&expect, &get_sort_col_descs(expect.num_columns()), None)?;

    let actual = DataBlock::concat(&actual)?;
    let actual = DataBlock::sort(&actual, &get_sort_col_descs(actual.num_columns()), None)?;

    let formated_expect = pretty_format_blocks(&[expect])?;
    let formated_actual = pretty_format_blocks(&[actual])?;

    assert_eq!(
        formated_expect, formated_actual,
        "Test params:{}\nExpected data block:\n{}\nActual data block:\n{}\n",
        fuzz_info, formated_expect, formated_actual
    );

    // Clear
    drop_index(ctx, "index").await?;

    Ok(())
}

fn get_sort_col_descs(num_cols: usize) -> Vec<SortColumnDescription> {
    let mut sorts = Vec::with_capacity(num_cols);
    for i in 0..num_cols {
        sorts.push(SortColumnDescription {
            offset: i,
            nulls_first: false,
            asc: true,
            is_nullable: false,
        });
    }
    sorts
}

#[derive(Default)]
struct TestSuite {
    query: &'static str,
    index: &'static str,

    is_index_scan: bool,
}

/// Generate test suites.
///
/// Table:
///
/// ```sql
/// create table t (a int, b int, c int);
/// ```
fn get_test_suites() -> Vec<TestSuite> {
    vec![
        // query: eval-scan, index: eval-scan
        TestSuite {
            query: "select to_string(c + 1) from t",
            index: "select c + 1 from t",
            is_index_scan: true,
        },
        TestSuite {
            query: "select c + 1 from t",
            index: "select c + 1 from t",
            is_index_scan: true,
        },
        TestSuite {
            query: "select a from t",
            index: "select a from t",
            is_index_scan: true,
        },
        TestSuite {
            query: "select a as z from t",
            index: "select a from t",
            is_index_scan: true,
        },
        TestSuite {
            query: "select a + 1, to_string(a) from t",
            index: "select a from t",
            is_index_scan: true,
        },
        TestSuite {
            query: "select a + 1 as z, to_string(a) from t",
            index: "select a from t",
            is_index_scan: true,
        },
        TestSuite {
            query: "select b from t",
            index: "select a, b from t",
            is_index_scan: true,
        },
        TestSuite {
            query: "select a from t",
            index: "select b, c from t",
            is_index_scan: false,
        },
        // query: eval-filter-scan, index: eval-scan
        TestSuite {
            query: "select a from t where b > 1",
            index: "select b, c from t",
            is_index_scan: false,
        },
        TestSuite {
            query: "select a from t where b > 1",
            index: "select a, b from t",
            is_index_scan: true,
        },
        // query: eval-agg-eval-scan, index: eval-scan
        TestSuite {
            query: "select sum(a) from t group by b",
            index: "select a from t",
            is_index_scan: false,
        },
        TestSuite {
            query: "select avg(a + 1) from t group by b",
            index: "select a + 1, b from t",
            is_index_scan: true,
        },
        TestSuite {
            query: "select avg(a + 1) from t",
            index: "select a + 1, b from t",
            is_index_scan: true,
        },
        // query: eval-agg-eval-filter-scan, index: eval-scan
        TestSuite {
            query: "select sum(a) from t where a > 1 group by b",
            index: "select a from t",
            is_index_scan: false,
        },
        // query: eval-scan, index: eval-filter-scan
        TestSuite {
            query: "select a from t",
            index: "select a from t where b > 1",
            is_index_scan: false,
        },
        // query: eval-filter-scan, index: eval-filter-scan
        TestSuite {
            query: "select a from t where b > 1",
            index: "select a, b from t where b > 2",
            is_index_scan: false,
        },
        TestSuite {
            query: "select a from t where b > 1",
            index: "select a, b from t where b > 0",
            is_index_scan: true,
        },
        TestSuite {
            query: "select a from t where b < 5",
            index: "select a, b from t where b > 0",
            is_index_scan: false,
        },
        TestSuite {
            query: "select a from t where b > 1 and b < 5",
            index: "select a, b from t where b > 0",
            is_index_scan: true,
        },
        TestSuite {
            query: "select a from t where b > 1 and b < 5",
            index: "select a, b from t where b > 0 and b < 6",
            is_index_scan: true,
        },
        // query: eval-agg-eval-scan, index: eval-filter-scan
        TestSuite {
            query: "select sum(a) from t group by b",
            index: "select a from t where b > 1",
            is_index_scan: false,
        },
        // query: eval-agg-eval-filter-scan, index: eval-filter-scan
        TestSuite {
            query: "select sum(a) from t where b > 1 group by b",
            index: "select a from t where b > 1",
            is_index_scan: false,
        },
        TestSuite {
            query: "select sum(a) from t where b > 1 group by b",
            index: "select a, b from t where b > 1",
            is_index_scan: true,
        },
        // query: eval-scan, index: eval-agg-eval-scan
        TestSuite {
            query: "select b from t",
            index: "select b, sum(a) from t group by b",
            is_index_scan: false,
        },
        // query: eval-filter-scan, index: eval-agg-eval-scan
        TestSuite {
            query: "select b from t where c > 1",
            index: "select b, sum(a) from t group by b",
            is_index_scan: false,
        },
        // query: eval-agg-eval-scan, index: eval-agg-eval-scan
        TestSuite {
            query: "select sum(a) from t group by b",
            index: "select b, sum(a) from t group by b",
            is_index_scan: true,
        },
        TestSuite {
            query: "select sum(a) from t group by b",
            index: "select sum(a) from t group by b",
            is_index_scan: true,
        },
        TestSuite {
            query: "select sum(a) + 1 from t group by b",
            index: "select sum(a) from t group by b",
            is_index_scan: true,
        },
        TestSuite {
            query: "select sum(a) + 1, b + 1 from t group by b",
            index: "select b, sum(a) from t group by b",
            is_index_scan: true,
        },
        TestSuite {
            query: "select sum(a) from t group by c",
            index: "select b, sum(a) from t group by b",
            is_index_scan: false,
        },
        TestSuite {
            query: "select sum(a) + 1 from t group by b",
            index: "select b, sum(a) from t group by b",
            is_index_scan: true,
        },
        // query: eval-agg-eval-filter-scan, index: eval-agg-eval-scan
        TestSuite {
            query: "select sum(a) + 1 from t where b > 1 group by b",
            index: "select b, sum(a) from t group by b",
            is_index_scan: true,
        },
        TestSuite {
            query: "select sum(a) + 1 from t where c > 1 group by b",
            index: "select b, sum(a) from t group by b",
            is_index_scan: false,
        },
        // query: eval-scan, index: eval-agg-eval-filter-scan
        TestSuite {
            query: "select b from t",
            index: "select b, sum(a) from t where a > 1 group by b",
            is_index_scan: false,
        },
        // query: eval-filter-scan, index: eval-agg-eval-filter-scan
        TestSuite {
            query: "select b from t where a > 1",
            index: "select b, sum(a) from t where a > 1 group by b",
            is_index_scan: false,
        },
        // query: eval-agg-eval-scan, index: eval-agg-eval-filter-scan
        TestSuite {
            query: "select sum(a) + 1 from t group by b",
            index: "select b, sum(a) from t where c > 1 group by b",
            is_index_scan: false,
        },
        // query: eval-agg-eval-filter-scan, index: eval-agg-eval-filter-scan
        TestSuite {
            query: "select sum(a) + 1 from t where c > 1 group by b",
            index: "select b, sum(a) from t where c > 1 group by b",
            is_index_scan: true,
        },
        TestSuite {
            query: "select sum(a) + 1, b + 2 from t where b > 1 group by b",
            index: "select b, sum(a) from t where b > 0 group by b",
            is_index_scan: true,
        },
        // query: eval-agg-scan, index: eval-agg-scan without group by
        TestSuite {
            query: "select sum(a) from t",
            index: "select sum(a) from t",
            is_index_scan: true,
        },
        // query: eval-agg-scan, index: eval-agg-scan with multiple agg funcs and without group by
        TestSuite {
            query: "select sum(a), approx_count_distinct(b) from t",
            index: "select sum(a), approx_count_distinct(b) from t",
            is_index_scan: true,
        },
        // query: eval-agg-scan, index: eval-agg-scan with both scalar and agg funcs
        TestSuite {
            query: "select sum(a), to_string(b) as bs from t group by bs",
            index: "select sum(a), to_string(b) as bs from t group by bs",
            is_index_scan: true,
        },
        // query: sort-eval-scan, index: eval-scan
        TestSuite {
            query: "select to_string(c + 1) as s from t order by s",
            index: "select c + 1 from t",
            is_index_scan: true,
        },
        // query: eval-sort-filter-scan, index: eval-scan
        TestSuite {
            query: "select a from t where b > 1 order by a",
            index: "select b, c from t",
            is_index_scan: false,
        },
        TestSuite {
            query: "select a from t where b > 1 order by a",
            index: "select a, b from t",
            is_index_scan: true,
        },
        // query: eval-sort-agg-eval-scan, index: eval-scan
        TestSuite {
            query: "select avg(a + 1) from t group by b order by b",
            index: "select a + 1, b from t",
            is_index_scan: true,
        },
        // query: eval-sort-filter-scan, index: eval-filter-scan
        TestSuite {
            query: "select a from t where b > 1 order by a",
            index: "select a, b from t where b > 0",
            is_index_scan: true,
        },
        TestSuite {
            query: "select a from t where b > 1 and b < 5 order by b",
            index: "select a, b from t where b > 0",
            is_index_scan: true,
        },
        // query: eval-sort-agg-eval-scan, index: eval-filter-scan
        TestSuite {
            query: "select sum(a) from t group by b order by b",
            index: "select a from t where b > 1",
            is_index_scan: false,
        },
        // query: eval-sort-agg-eval-filter-scan, index: eval-filter-scan
        TestSuite {
            query: "select sum(a) from t where b > 1 group by b order by b",
            index: "select a from t where b > 1",
            is_index_scan: false,
        },
        TestSuite {
            query: "select sum(a) from t where b > 1 group by b order by b",
            index: "select a, b from t where b > 1",
            is_index_scan: true,
        },
        // query: eval-sort-agg-eval-scan, index: eval-agg-eval-scan
        TestSuite {
            query: "select sum(a) from t group by b order by b",
            index: "select b, sum(a) from t group by b",
            is_index_scan: true,
        },
        TestSuite {
            query: "select sum(a) from t group by b order by b",
            index: "select sum(a) from t group by b",
            is_index_scan: true,
        },
        TestSuite {
            query: "select sum(a) + 1 from t group by b order by b",
            index: "select b, sum(a) from t group by b",
            is_index_scan: true,
        },
        // query: eval-sort-agg-eval-filter-scan, index: eval-agg-eval-scan
        TestSuite {
            query: "select sum(a) + 1 from t where b > 1 group by b order by b",
            index: "select b, sum(a) from t group by b",
            is_index_scan: true,
        },
        TestSuite {
            query: "select sum(a) + 1 from t where c > 1 group by b order by b",
            index: "select b, sum(a) from t group by b",
            is_index_scan: false,
        },
        // query: eval-sort-agg-eval-filter-scan, index: eval-agg-eval-filter-scan
        TestSuite {
            query: "select sum(a) + 1 from t where c > 1 group by b order by b",
            index: "select b, sum(a) from t where c > 1 group by b",
            is_index_scan: true,
        },
        TestSuite {
            query: "select sum(a) + 1, b + 2 from t where b > 1 group by b order by b",
            index: "select b, sum(a) from t where b > 0 group by b",
            is_index_scan: true,
        },
        // query: eval-sort-agg-scan, index: eval-agg-scan with both scalar and agg funcs
        TestSuite {
            query: "select sum(a), to_string(b) as bs from t group by bs order by bs",
            index: "select sum(a), to_string(b) as bs from t group by bs",
            is_index_scan: true,
        },
    ]
}

async fn test_fuzz_impl(format: &str, spill: bool) -> Result<()> {
    let test_suites = get_test_suites();
    let spill_settings = if spill {
        Some(HashMap::from([
            (
                "aggregate_spilling_memory_ratio".to_string(),
                "100".to_string(),
            ),
            (
                "aggregate_spilling_bytes_threshold_per_proc".to_string(),
                "1".to_string(),
            ),
        ]))
    } else {
        None
    };

    let fixture = TestFixture::setup_with_custom(EESetup::new()).await?;
    for num_blocks in [1, 10] {
        for num_rows_per_block in [1, 50] {
            let session = fixture.default_session();
            if let Some(s) = spill_settings.as_ref() {
                let settings = session.get_settings();
                // Make sure the operator will spill the aggregation.
                settings.set_batch_settings(s)?;
            }

            // Prepare table and data
            // Create random engine table to generate random data.
            fixture
                .execute_command("CREATE TABLE rt (a int, b int, c int) ENGINE = RANDOM")
                .await?;
            fixture
                .execute_command(&format!(
                    "CREATE TABLE t (a int, b int, c int) storage_format = '{format}'"
                ))
                .await?;
            // Insert random data to table t.
            for _ in 0..num_blocks {
                fixture
                    .execute_command(&format!(
                        "INSERT INTO t SELECT * FROM rt LIMIT {}",
                        num_rows_per_block
                    ))
                    .await?;
            }

            // Run fuzz tests with different index block ratios.
            for index_block_ratio in [0.2, 0.5, 0.8, 1.0] {
                for suite in test_suites.iter() {
                    fuzz(fixture.new_query_ctx().await?, FuzzParams {
                        query_sql: suite.query.to_string(),
                        index_sql: suite.index.to_string(),
                        is_index_scan: suite.is_index_scan,
                        num_blocks,
                        num_rows_per_block,
                        index_block_ratio,
                    })
                    .await?;
                }
            }

            // Clear data
            fixture.execute_command("DROP TABLE rt ALL").await?;
            fixture.execute_command("DROP TABLE t ALL").await?;
        }
    }

    Ok(())
}
