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
use std::sync::Arc;

use aggregating_index::get_agg_index_handler;
use chrono::Utc;
use common_base::base::tokio;
use common_exception::Result;
use common_expression::block_debug::pretty_format_blocks;
use common_expression::infer_table_schema;
use common_expression::DataBlock;
use common_expression::DataSchemaRefExt;
use common_expression::SendableDataBlockStream;
use common_expression::SortColumnDescription;
use common_meta_app::schema::CreateIndexReq;
use common_meta_app::schema::IndexMeta;
use common_meta_app::schema::IndexNameIdent;
use common_meta_app::schema::IndexType;
use common_sql::optimizer::SExpr;
use common_sql::planner::plans::Plan;
use common_sql::plans::RelOperator;
use common_sql::Planner;
use common_storages_fuse::io::serialize_block;
use common_storages_fuse::io::TableMetaLocationGenerator;
use common_storages_fuse::io::WriteSettings;
use common_storages_fuse::TableContext;
use databend_query::interpreters::InterpreterFactory;
use databend_query::sessions::QueryContext;
use databend_query::test_kits::table_test_fixture::expects_ok;
use databend_query::test_kits::TestFixture;
use enterprise_query::test_kits::context::create_ee_query_context;
use futures_util::StreamExt;
use futures_util::TryStreamExt;

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

async fn create_index(ctx: Arc<QueryContext>, index_name: &str, query: &str) -> Result<u64> {
    let sql = format!("CREATE AGGREGATING INDEX {index_name} AS {query}");

    let plan = plan_sql(ctx.clone(), &sql).await?;

    if let Plan::CreateIndex(plan) = plan {
        let catalog = ctx.get_catalog("default")?;
        let create_index_req = CreateIndexReq {
            if_not_exists: plan.if_not_exists,
            name_ident: IndexNameIdent {
                tenant: ctx.get_tenant(),
                index_name: index_name.to_string(),
            },
            meta: IndexMeta {
                table_id: plan.table_id,
                index_type: IndexType::AGGREGATING,
                created_on: Utc::now(),
                dropped_on: None,
                updated_on: None,
                query: query.to_string(),
            },
        };

        let handler = get_agg_index_handler();
        let res = handler.do_create_index(catalog, create_index_req).await?;

        return Ok(res.index_id);
    }

    unreachable!()
}

async fn drop_index(ctx: Arc<QueryContext>, index_name: &str) -> Result<()> {
    let sql = format!("DROP AGGREGATING INDEX {index_name}");
    execute_sql(ctx, &sql).await?;

    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn test_index_scan() -> Result<()> {
    let (_guard, ctx, _) = create_ee_query_context(None).await.unwrap();
    let fixture = TestFixture::new_with_ctx(_guard, ctx).await;

    // Create table
    execute_sql(
        fixture.ctx(),
        "CREATE TABLE t (a int, b int, c int) storage_format = 'parquet'",
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

    let index_id = create_index(
        fixture.ctx(),
        index_name,
        "SELECT b, SUM(a) from t WHERE c > 1 GROUP BY b",
    )
    .await?;

    // Refresh Index
    refresh_index(
        fixture.ctx(),
        "SELECT b, SUM_state(a), _block_name from t WHERE c > 1 GROUP BY b, _block_name",
        index_id,
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

#[tokio::test(flavor = "multi_thread")]
async fn test_index_scan_two_agg_funcs() -> Result<()> {
    let (_guard, ctx, _) = create_ee_query_context(None).await.unwrap();
    let fixture = TestFixture::new_with_ctx(_guard, ctx).await;

    // Create table
    execute_sql(
        fixture.ctx(),
        "CREATE TABLE t (a int, b int, c int) storage_format = 'parquet'",
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

    let index_id = create_index(
        fixture.ctx(),
        index_name,
        "SELECT b, MAX(a), SUM(a) from t WHERE c > 1 GROUP BY b",
    )
    .await?;

    // Refresh Index
    refresh_index(
        fixture.ctx(),
        "SELECT b, MAX_state(a), SUM_state(a), _block_name from t WHERE c > 1 GROUP BY b, _block_name",
        index_id,
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

async fn refresh_index(ctx: Arc<QueryContext>, sql: &str, index_id: u64) -> Result<()> {
    let plan = plan_sql(ctx.clone(), sql).await?;
    let mut output_fields = plan.schema().fields().to_vec();
    assert_eq!(output_fields.last().unwrap().name(), "_block_name");
    output_fields.pop(); // Pop _block_name field
    let output_schema = DataSchemaRefExt::create(output_fields);
    let index_schema = infer_table_schema(&output_schema)?;

    let mut buffer = vec![];
    let mut map: HashMap<String, Vec<(u32, u32, usize)>> = HashMap::new();

    let mut index = 0;
    let mut stream = execute_plan(ctx.clone(), &plan).await?;
    while let Some(block) = stream.next().await {
        let block = block?;
        let block = block.convert_to_full();
        let block_name_col = block
            .columns()
            .last()
            .unwrap()
            .value
            .as_column()
            .unwrap()
            .as_string()
            .unwrap()
            .clone();
        let result = block.pop_columns(1)?;

        for (row, block_name) in block_name_col.iter().enumerate() {
            let name = String::from_utf8(block_name.to_vec())?;

            map.entry(name)
                .and_modify(|v| v.push((index, row as u32, 1)))
                .or_insert(vec![(index, row as u32, 1)]);
        }

        buffer.push(result);
        index += 1;
    }

    let op = ctx.get_data_operator()?.operator();
    let data = buffer.iter().collect::<Vec<_>>();

    for (loc, indices) in map {
        let index_block = DataBlock::take_blocks(&data, &indices, indices.len());
        let mut buf = vec![];

        serialize_block(
            &WriteSettings::default(),
            &index_schema,
            index_block,
            &mut buf,
        )?;

        let index_loc =
            TableMetaLocationGenerator::gen_agg_index_location_from_block_location(&loc, index_id);

        op.write(&index_loc, buf).await?;
    }

    Ok(())
}

async fn fuzz(
    num_rows_per_block: usize,
    num_blocks: usize,
    index_block_ratio: f64,
    query_sql: &str,
    index_sql: &str,
    refresh_sql: &str,
    is_index_scan: bool,
) -> Result<()> {
    let fuzz_info = format_fuzz_test_params(
        query_sql,
        index_sql,
        refresh_sql,
        is_index_scan,
        num_blocks,
        num_rows_per_block,
        index_block_ratio,
    );

    let (_guard, ctx, _) = create_ee_query_context(None).await.unwrap();
    let fixture = TestFixture::new_with_ctx(_guard, ctx).await;
    let ctx = fixture.ctx();

    // Prepare table and data
    // Create random engine table
    execute_sql(
        ctx.clone(),
        "CREATE TABLE rt (a int, b int, c int) ENGINE = RANDOM",
    )
    .await?;
    execute_sql(
        ctx.clone(),
        "CREATE TABLE t (a int, b int, c int) storage_format = 'parquet'",
    )
    .await?;
    execute_sql(
        ctx.clone(),
        "CREATE TABLE temp_t (a int, b int, c int) storage_format = 'parquet'",
    )
    .await?;
    let index_id = create_index(ctx.clone(), "index", index_sql).await?;

    let plan = plan_sql(ctx.clone(), query_sql).await?;
    if !is_index_scan_plan(&plan) {
        assert!(!is_index_scan, "{}", fuzz_info);
        // Clear
        drop_index(ctx.clone(), "index").await?;
        execute_sql(ctx.clone(), "DROP TABLE rt ALL").await?;
        execute_sql(ctx.clone(), "DROP TABLE t ALL").await?;
        execute_sql(ctx, "DROP TABLE temp_t ALL").await?;
        return Ok(());
    }
    assert!(is_index_scan, "{}", fuzz_info);

    // Generate data with index
    let num_index_blocks = (num_blocks as f64 * index_block_ratio) as usize;
    for _ in 0..num_index_blocks {
        execute_sql(
            ctx.clone(),
            &format!(
                "INSERT INTO t SELECT * FROM rt LIMIT {}",
                num_rows_per_block
            ),
        )
        .await?;
    }
    refresh_index(ctx.clone(), refresh_sql, index_id).await?;

    // Generate data without index
    for _ in 0..num_blocks - num_index_blocks {
        execute_sql(
            ctx.clone(),
            &format!(
                "INSERT INTO t SELECT * FROM rt LIMIT {}",
                num_rows_per_block
            ),
        )
        .await?;
    }

    // Copy data into temp table
    execute_sql(ctx.clone(), "INSERT INTO temp_t SELECT * FROM t").await?;

    // Get query result
    let expect: Vec<DataBlock> =
        execute_sql(ctx.clone(), &query_sql.replace("from t", "from temp_t"))
            .await?
            .try_collect()
            .await?;

    // Get index scan query result
    let actual: Vec<DataBlock> = execute_sql(ctx.clone(), query_sql)
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
        execute_sql(ctx.clone(), "DROP TABLE rt ALL").await?;
        execute_sql(ctx.clone(), "DROP TABLE t ALL").await?;
        execute_sql(ctx, "DROP TABLE temp_t ALL").await?;
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
    drop_index(ctx.clone(), "index").await?;
    execute_sql(ctx.clone(), "DROP TABLE rt ALL").await?;
    execute_sql(ctx.clone(), "DROP TABLE t ALL").await?;
    execute_sql(ctx, "DROP TABLE temp_t ALL").await?;

    Ok(())
}

#[derive(Default)]
struct TestSuite {
    query: &'static str,
    index: &'static str,
    refresh: &'static str,
    is_matched: bool,
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
            refresh: "select c + 1, _block_name from t",
            is_matched: true,
        },
        TestSuite {
            query: "select c + 1 from t",
            index: "select c + 1 from t",
            refresh: "select c + 1, _block_name from t",
            is_matched: true,
        },
        TestSuite {
            query: "select a from t",
            index: "select a from t",
            refresh: "select a, _block_name from t",
            is_matched: true,
        },
        TestSuite {
            query: "select a as z from t",
            index: "select a from t",
            refresh: "select a, _block_name from t",
            is_matched: true,
        },
        TestSuite {
            query: "select a + 1, to_string(a) from t",
            index: "select a from t",
            refresh: "select a, _block_name from t",
            is_matched: true,
        },
        TestSuite {
            query: "select a + 1 as z, to_string(a) from t",
            index: "select a from t",
            refresh: "select a, _block_name from t",
            is_matched: true,
        },
        TestSuite {
            query: "select b from t",
            index: "select a, b from t",
            refresh: "select a, b, _block_name from t",
            is_matched: true,
        },
        TestSuite {
            query: "select a from t",
            index: "select b, c from t",
            refresh: "select b, c, _block_name from t",
            is_matched: false,
        },
        // query: eval-filter-scan, index: eval-scan
        TestSuite {
            query: "select a from t where b > 1",
            index: "select b, c from t",
            refresh: "select b, c, _block_name from t",
            is_matched: false,
        },
        TestSuite {
            query: "select a from t where b > 1",
            index: "select a, b from t",
            refresh: "select a, b, _block_name from t",
            is_matched: true,
        },
        // query: eval-agg-eval-scan, index: eval-scan
        TestSuite {
            query: "select sum(a) from t group by b",
            index: "select a from t",
            refresh: "select a, _block_name from t",
            is_matched: false,
        },
        // query: eval-agg-eval-filter-scan, index: eval-scan
        TestSuite {
            query: "select sum(a) from t where a > 1 group by b",
            index: "select a from t",
            refresh: "select a, _block_name from t",
            is_matched: false,
        },
        // query: eval-scan, index: eval-filter-scan
        TestSuite {
            query: "select a from t",
            index: "select a from t where b > 1",
            refresh: "select a, _block_name from t where b > 1",
            is_matched: false,
        },
        // query: eval-filter-scan, index: eval-filter-scan
        TestSuite {
            query: "select a from t where b > 1",
            index: "select a, b from t where b > 2",
            refresh: "select a, b, _block_name from t where b > 2",
            is_matched: false,
        },
        TestSuite {
            query: "select a from t where b > 1",
            index: "select a, b from t where b > 0",
            refresh: "select a, b, _block_name from t where b > 0",
            is_matched: true,
        },
        TestSuite {
            query: "select a from t where b < 5",
            index: "select a, b from t where b > 0",
            refresh: "select a, b, _block_name from t where b > 0",
            is_matched: false,
        },
        TestSuite {
            query: "select a from t where b > 1 and b < 5",
            index: "select a, b from t where b > 0",
            refresh: "select a, b, _block_name from t where b > 0",
            is_matched: true,
        },
        TestSuite {
            query: "select a from t where b > 1 and b < 5",
            index: "select a, b from t where b > 0 and b < 6",
            refresh: "select a, b, _block_name from t where b > 0 and b < 6",
            is_matched: true,
        },
        // query: eval-agg-eval-scan, index: eval-filter-scan
        TestSuite {
            query: "select sum(a) from t group by b",
            index: "select a from t where b > 1",
            refresh: "select a, _block_name from t where b > 1",
            is_matched: false,
        },
        // query: eval-agg-eval-filter-scan, index: eval-filter-scan
        TestSuite {
            query: "select sum(a) from t where b > 1 group by b",
            index: "select a from t where b > 1",
            refresh: "select a, _block_name from t where b > 1",
            is_matched: false,
        },
        // query: eval-scan, index: eval-agg-eval-scan
        TestSuite {
            query: "select b from t",
            index: "select b, sum(a) from t group by b",
            refresh: "select b, sum_state(a), _block_name from t group by b, _block_name",
            is_matched: false,
        },
        // query: eval-filter-scan, index: eval-agg-eval-scan
        TestSuite {
            query: "select b from t where c > 1",
            index: "select b, sum(a) from t group by b",
            refresh: "select b, sum_state(a), _block_name from t group by b, _block_name",
            is_matched: false,
        },
        // query: eval-agg-eval-scan, index: eval-agg-eval-scan
        TestSuite {
            query: "select sum(a) from t group by b",
            index: "select b, sum(a) from t group by b",
            refresh: "select b, sum_state(a), _block_name from t group by b, _block_name",
            is_matched: true,
        },
        TestSuite {
            query: "select sum(a) from t group by b",
            index: "select sum(a) from t group by b",
            refresh: "select sum_state(a), _block_name from t group by b, _block_name",
            is_matched: false,
        },
        TestSuite {
            query: "select sum(a) + 1 from t group by b",
            index: "select sum(a) from t group by b",
            refresh: "select sum_state(a), _block_name from t group by b, _block_name",
            is_matched: false,
        },
        TestSuite {
            query: "select sum(a) + 1, b + 1 from t group by b",
            index: "select b, sum(a) from t group by b",
            refresh: "select b, sum_state(a), _block_name from t group by b, _block_name",
            is_matched: true,
        },
        TestSuite {
            query: "select sum(a) from t group by c",
            index: "select b, sum(a) from t group by b",
            refresh: "select b, sum_state(a), _block_name from t group by b, _block_name",
            is_matched: false,
        },
        TestSuite {
            query: "select sum(a) + 1 from t group by b",
            index: "select b, sum(a) from t group by b",
            refresh: "select b, sum_state(a), _block_name from t group by b, _block_name",
            is_matched: true,
        },
        // query: eval-agg-eval-filter-scan, index: eval-agg-eval-scan
        TestSuite {
            query: "select sum(a) + 1 from t where b > 1 group by b",
            index: "select b, sum(a) from t group by b",
            refresh: "select b, sum_state(a), _block_name from t group by b, _block_name",
            is_matched: true,
        },
        TestSuite {
            query: "select sum(a) + 1 from t where c > 1 group by b",
            index: "select b, sum(a) from t group by b",
            refresh: "select b, sum_state(a), _block_name from t group by b, _block_name",
            is_matched: false,
        },
        // query: eval-scan, index: eval-agg-eval-filter-scan
        TestSuite {
            query: "select b from t",
            index: "select b, sum(a) from t where a > 1 group by b",
            refresh: "select b, sum_state(a), _block_name from t where a > 1 group by b, _block_name",
            is_matched: false,
        },
        // query: eval-filter-scan, index: eval-agg-eval-filter-scan
        TestSuite {
            query: "select b from t where a > 1",
            index: "select b, sum(a) from t where a > 1 group by b",
            refresh: "select b, sum_state(a), _block_name from t where a > 1 group by b, _block_name",
            is_matched: false,
        },
        // query: eval-agg-eval-scan, index: eval-agg-eval-filter-scan
        TestSuite {
            query: "select sum(a) + 1 from t group by b",
            index: "select b, sum(a) from t where c > 1 group by b",
            refresh: "select b, sum_state(a), _block_name from t where c > 1 group by b, _block_name",
            is_matched: false,
        },
        // query: eval-agg-eval-filter-scan, index: eval-agg-eval-filter-scan
        TestSuite {
            query: "select sum(a) + 1 from t where c > 1 group by b",
            index: "select b, sum(a) from t where c > 1 group by b",
            refresh: "select b, sum_state(a), _block_name from t where c > 1 group by b, _block_name",
            is_matched: true,
        },
        TestSuite {
            query: "select sum(a) + 1, b + 2 from t where b > 1 group by b",
            index: "select b, sum(a) from t where b > 0 group by b",
            refresh: "select b, sum_state(a), _block_name from t where b > 0 group by b, _block_name",
            is_matched: true,
        },
    ]
}

#[inline(always)]
fn format_fuzz_test_params(
    query: &str,
    index: &str,
    refresh: &str,
    is_index_scan: bool,
    num_blocks: usize,
    num_rows_per_block: usize,
    index_block_ratio: f64,
) -> String {
    format!(
        "Query: {}\nIndex: {}\nRefresh: {}\nIsIndexScan: {}\nNumBlocks: {}\nNumRowsPerBlock: {}\nIndexBlockRatio: {}\n",
        query, index, refresh, is_index_scan, num_blocks, num_rows_per_block, index_block_ratio,
    )
}

#[tokio::test(flavor = "multi_thread")]
async fn test_fuzz() -> Result<()> {
    let test_suites = get_test_suites();

    for suite in test_suites {
        for num_blocks in [1, 10] {
            for num_rows_per_block in [1, 10, 50, 100] {
                for index_block_ratio in [0.0, 0.2, 0.5, 0.8, 1.0] {
                    fuzz(
                        num_rows_per_block,
                        num_blocks,
                        index_block_ratio,
                        suite.query,
                        suite.index,
                        suite.refresh,
                        suite.is_matched,
                    )
                    .await?;
                }
            }
        }
    }
    Ok(())
}
