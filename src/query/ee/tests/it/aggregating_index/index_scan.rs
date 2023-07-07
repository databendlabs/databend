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

use std::fmt::Display;
use std::sync::Arc;

use common_base::base::tokio;
use common_exception::Result;
use common_expression::block_debug::pretty_format_blocks;
use common_expression::DataBlock;
use common_expression::SendableDataBlockStream;
use common_expression::SortColumnDescription;
use common_sql::optimizer::SExpr;
use common_sql::planner::plans::Plan;
use common_sql::plans::RelOperator;
use common_sql::Planner;
use databend_query::interpreters::InterpreterFactory;
use databend_query::sessions::QueryContext;
use databend_query::test_kits::table_test_fixture::expects_ok;
use databend_query::test_kits::TestFixture;
use enterprise_query::test_kits::context::create_ee_query_context;
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
async fn test_fuzz() -> Result<()> {
    test_fuzz_impl("parquet").await?;
    test_fuzz_impl("native").await
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
    execute_sql(ctx, &sql).await?;

    Ok(())
}

async fn test_index_scan_impl(format: &str) -> Result<()> {
    let (_guard, ctx, _) = create_ee_query_context(None).await.unwrap();
    let fixture = TestFixture::new_with_ctx(_guard, ctx).await;

    // Create table
    execute_sql(
        fixture.ctx(),
        &format!("CREATE TABLE t (a int, b int, c int) storage_format = '{format}'"),
    )
    .await?;

    // Insert data
    execute_sql(
        fixture.ctx(),
        "INSERT INTO t VALUES (1,1,4), (1,2,1), (1,2,4), (2,2,5)",
    )
    .await?;

    // Create index
    let index_name = "index1";

    execute_sql(
        fixture.ctx(),
        &format!("CREATE AGGREGATING INDEX {index_name} AS SELECT b, SUM(a) from t WHERE c > 1 GROUP BY b"),
    )
    .await?;

    // Refresh Index
    execute_sql(
        fixture.ctx(),
        &format!("REFRESH AGGREGATING INDEX {index_name}"),
    )
    .await?;

    // Query with index
    let plan = plan_sql(
        fixture.ctx(),
        "SELECT b, SUM(a) from t WHERE c > 1 GROUP BY b",
    )
    .await?;

    assert!(is_index_scan_plan(&plan));

    expects_ok(
        "Index scan",
        execute_plan(fixture.ctx(), &plan).await,
        vec![
            "+----------+----------+",
            "| Column 0 | Column 1 |",
            "+----------+----------+",
            "| 1        | 1        |",
            "| 2        | 3        |",
            "+----------+----------+",
        ],
    )
    .await?;

    let plan = plan_sql(fixture.ctx(), "SELECT b from t WHERE c > 1 GROUP BY b").await?;

    assert!(is_index_scan_plan(&plan));

    expects_ok(
        "Index scan",
        execute_plan(fixture.ctx(), &plan).await,
        vec![
            "+----------+",
            "| Column 0 |",
            "+----------+",
            "| 1        |",
            "| 2        |",
            "+----------+",
        ],
    )
    .await?;

    let plan = plan_sql(fixture.ctx(), "SELECT SUM(a) from t WHERE c > 1 GROUP BY b").await?;

    assert!(is_index_scan_plan(&plan));

    expects_ok(
        "Index scan",
        execute_plan(fixture.ctx(), &plan).await,
        vec![
            "+----------+",
            "| Column 0 |",
            "+----------+",
            "| 1        |",
            "| 3        |",
            "+----------+",
        ],
    )
    .await?;

    // Insert new data but not refresh index
    execute_sql(
        fixture.ctx(),
        "INSERT INTO t VALUES (1,1,4), (1,2,1), (1,2,4), (2,2,5)",
    )
    .await?;

    // Query with one fuse block and one index block
    let plan = plan_sql(
        fixture.ctx(),
        "SELECT b, SUM(a) from t WHERE c > 1 GROUP BY b",
    )
    .await?;

    assert!(is_index_scan_plan(&plan));

    expects_ok(
        "Index scan",
        execute_plan(fixture.ctx(), &plan).await,
        vec![
            "+----------+----------+",
            "| Column 0 | Column 1 |",
            "+----------+----------+",
            "| 1        | 2        |",
            "| 2        | 6        |",
            "+----------+----------+",
        ],
    )
    .await?;

    let plan = plan_sql(fixture.ctx(), "SELECT b + 1 from t WHERE c > 1 GROUP BY b").await?;

    assert!(is_index_scan_plan(&plan));

    expects_ok(
        "Index scan",
        execute_plan(fixture.ctx(), &plan).await,
        vec![
            "+----------+",
            "| Column 0 |",
            "+----------+",
            "| 2        |",
            "| 3        |",
            "+----------+",
        ],
    )
    .await?;

    let plan = plan_sql(
        fixture.ctx(),
        "SELECT SUM(a) + 1 from t WHERE c > 1 GROUP BY b",
    )
    .await?;

    assert!(is_index_scan_plan(&plan));

    expects_ok(
        "Index scan",
        execute_plan(fixture.ctx(), &plan).await,
        vec![
            "+----------+",
            "| Column 0 |",
            "+----------+",
            "| 3        |",
            "| 7        |",
            "+----------+",
        ],
    )
    .await?;

    drop_index(fixture.ctx(), index_name).await?;

    Ok(())
}

async fn test_index_scan_two_agg_funcs_impl(format: &str) -> Result<()> {
    let (_guard, ctx, _) = create_ee_query_context(None).await.unwrap();
    let fixture = TestFixture::new_with_ctx(_guard, ctx).await;

    // Create table
    execute_sql(
        fixture.ctx(),
        &format!("CREATE TABLE t (a int, b int, c int) storage_format = '{format}'"),
    )
    .await?;

    // Insert data
    execute_sql(
        fixture.ctx(),
        "INSERT INTO t VALUES (1,1,4), (1,2,1), (1,2,4), (2,2,5)",
    )
    .await?;

    // Create index
    let index_name = "index1";

    execute_sql(
        fixture.ctx(),
        &format!("CREATE AGGREGATING INDEX {index_name} AS SELECT b, MAX(a), SUM(a) from t WHERE c > 1 GROUP BY b"),
    )
    .await?;

    // Refresh Index
    execute_sql(
        fixture.ctx(),
        &format!("REFRESH AGGREGATING INDEX {index_name}"),
    )
    .await?;

    // Query with index
    // sum
    let plan = plan_sql(
        fixture.ctx(),
        "SELECT b, SUM(a) from t WHERE c > 1 GROUP BY b",
    )
    .await?;

    assert!(is_index_scan_plan(&plan));

    expects_ok(
        "Index scan",
        execute_plan(fixture.ctx(), &plan).await,
        vec![
            "+----------+----------+",
            "| Column 0 | Column 1 |",
            "+----------+----------+",
            "| 1        | 1        |",
            "| 2        | 3        |",
            "+----------+----------+",
        ],
    )
    .await?;

    // sum and max
    let plan = plan_sql(
        fixture.ctx(),
        "SELECT b, SUM(a), MAX(a) from t WHERE c > 1 GROUP BY b",
    )
    .await?;

    assert!(is_index_scan_plan(&plan));

    expects_ok(
        "Index scan",
        execute_plan(fixture.ctx(), &plan).await,
        vec![
            "+----------+----------+----------+",
            "| Column 0 | Column 1 | Column 2 |",
            "+----------+----------+----------+",
            "| 1        | 1        | 1        |",
            "| 2        | 3        | 2        |",
            "+----------+----------+----------+",
        ],
    )
    .await?;

    // Insert new data but not refresh index
    execute_sql(
        fixture.ctx(),
        "INSERT INTO t VALUES (1,1,4), (1,2,1), (1,2,4), (2,2,5)",
    )
    .await?;

    // Query with one fuse block and one index block
    // sum
    let plan = plan_sql(
        fixture.ctx(),
        "SELECT b, SUM(a) from t WHERE c > 1 GROUP BY b",
    )
    .await?;

    assert!(is_index_scan_plan(&plan));

    expects_ok(
        "Index scan",
        execute_plan(fixture.ctx(), &plan).await,
        vec![
            "+----------+----------+",
            "| Column 0 | Column 1 |",
            "+----------+----------+",
            "| 1        | 2        |",
            "| 2        | 6        |",
            "+----------+----------+",
        ],
    )
    .await?;

    // sum and max
    let plan = plan_sql(
        fixture.ctx(),
        "SELECT b, SUM(a), MAX(a) from t WHERE c > 1 GROUP BY b",
    )
    .await?;

    assert!(is_index_scan_plan(&plan));

    expects_ok(
        "Index scan",
        execute_plan(fixture.ctx(), &plan).await,
        vec![
            "+----------+----------+----------+",
            "| Column 0 | Column 1 | Column 2 |",
            "+----------+----------+----------+",
            "| 1        | 2        | 1        |",
            "| 2        | 6        | 2        |",
            "+----------+----------+----------+",
        ],
    )
    .await?;

    drop_index(fixture.ctx(), index_name).await?;

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
        s_expr
            .children()
            .iter()
            .any(|child| is_index_scan_sexpr(child.as_ref()))
    }
}

struct FuzzParams {
    num_rows_per_block: usize,
    num_blocks: usize,
    index_block_ratio: f64,
    query_sql: String,
    index_sql: String,
    is_index_scan: bool,
    table_format: String,
}

impl Display for FuzzParams {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "Query: {}\nIndex: {}\nIsIndexScan: {}\nNumBlocks: {}\nNumRowsPerBlock: {}\nIndexBlockRatio: {}\nTableFormat: {}\n",
            self.query_sql,
            self.index_sql,
            self.is_index_scan,
            self.num_blocks,
            self.num_rows_per_block,
            self.index_block_ratio,
            self.table_format,
        )
    }
}

async fn fuzz(params: FuzzParams) -> Result<()> {
    let fuzz_info = params.to_string();
    let FuzzParams {
        num_rows_per_block,
        num_blocks,
        index_block_ratio,
        query_sql,
        index_sql,
        is_index_scan,
        table_format: format,
    } = params;

    let (_guard, ctx, _) = create_ee_query_context(None).await.unwrap();
    let fixture = TestFixture::new_with_ctx(_guard, ctx).await;

    // Prepare table and data
    // Create random engine table
    execute_sql(
        fixture.ctx(),
        "CREATE TABLE rt (a int, b int, c int) ENGINE = RANDOM",
    )
    .await?;
    execute_sql(
        fixture.ctx(),
        &format!("CREATE TABLE t (a int, b int, c int) storage_format = '{format}'"),
    )
    .await?;
    execute_sql(
        fixture.ctx(),
        &format!("CREATE TABLE temp_t (a int, b int, c int) storage_format = '{format}'"),
    )
    .await?;
    execute_sql(
        fixture.ctx(),
        &format!("CREATE AGGREGATING INDEX index AS {index_sql}"),
    )
    .await?;

    let plan = plan_sql(fixture.ctx(), &query_sql).await?;
    if !is_index_scan_plan(&plan) {
        assert!(!is_index_scan, "{}", fuzz_info);
        // Clear
        drop_index(fixture.ctx(), "index").await?;
        execute_sql(fixture.ctx(), "DROP TABLE rt ALL").await?;
        execute_sql(fixture.ctx(), "DROP TABLE t ALL").await?;
        execute_sql(fixture.ctx(), "DROP TABLE temp_t ALL").await?;
        return Ok(());
    }
    assert!(is_index_scan, "{}", fuzz_info);

    // Generate data with index
    for _ in 0..num_blocks {
        execute_sql(
            fixture.ctx(),
            &format!(
                "INSERT INTO t SELECT * FROM rt LIMIT {}",
                num_rows_per_block
            ),
        )
        .await?;
    }

    let num_index_blocks = (num_blocks as f64 * index_block_ratio) as usize;
    if num_index_blocks > 0 {
        execute_sql(
            fixture.ctx(),
            &format!("REFRESH AGGREGATING INDEX index LIMIT {num_index_blocks}"),
        )
        .await?;
    }

    // Copy data into temp table
    execute_sql(fixture.ctx(), "INSERT INTO temp_t SELECT * FROM t").await?;

    // Get query result
    let expect: Vec<DataBlock> =
        execute_sql(fixture.ctx(), &query_sql.replace("from t", "from temp_t"))
            .await?
            .try_collect()
            .await?;

    // Get index scan query result
    let actual: Vec<DataBlock> = execute_sql(fixture.ctx(), &query_sql)
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
        drop_index(fixture.ctx(), "index").await?;
        execute_sql(fixture.ctx(), "DROP TABLE rt ALL").await?;
        execute_sql(fixture.ctx(), "DROP TABLE t ALL").await?;
        execute_sql(fixture.ctx(), "DROP TABLE temp_t ALL").await?;
        return Ok(());
    }

    assert!(!actual.is_empty(), "{}", fuzz_info);

    let expect = DataBlock::concat(&expect)?;
    let expect = DataBlock::sort(
        &expect,
        &[SortColumnDescription {
            offset: 0,
            nulls_first: false,
            asc: true,
            is_nullable: false,
        }],
        None,
    )?;

    let actual = DataBlock::concat(&actual)?;
    let actual = DataBlock::sort(
        &actual,
        &[SortColumnDescription {
            offset: 0,
            nulls_first: false,
            asc: true,
            is_nullable: false,
        }],
        None,
    )?;

    let formated_expect = pretty_format_blocks(&[expect])?;
    let formated_actual = pretty_format_blocks(&[actual])?;

    assert_eq!(
        formated_expect, formated_actual,
        "Test params:{}\nExpected data block:\n{}\nActual data block:\n{}\n",
        fuzz_info, formated_expect, formated_actual
    );

    // Clear
    drop_index(fixture.ctx(), "index").await?;
    execute_sql(fixture.ctx(), "DROP TABLE rt ALL").await?;
    execute_sql(fixture.ctx(), "DROP TABLE t ALL").await?;
    execute_sql(fixture.ctx(), "DROP TABLE temp_t ALL").await?;

    Ok(())
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
            is_index_scan: false,
        },
        TestSuite {
            query: "select sum(a) + 1 from t group by b",
            index: "select sum(a) from t group by b",
            is_index_scan: false,
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
    ]
}

async fn test_fuzz_impl(format: &str) -> Result<()> {
    let test_suites = get_test_suites();

    for suite in test_suites {
        for num_blocks in [1, 10] {
            for num_rows_per_block in [1, 10, 50, 100] {
                for index_block_ratio in [0.0, 0.2, 0.5, 0.8, 1.0] {
                    fuzz(FuzzParams {
                        num_rows_per_block,
                        num_blocks,
                        index_block_ratio,
                        query_sql: suite.query.to_string(),
                        index_sql: suite.index.to_string(),
                        is_index_scan: suite.is_index_scan,
                        table_format: format.to_string(),
                    })
                    .await?;
                }
            }
        }
    }
    Ok(())
}
