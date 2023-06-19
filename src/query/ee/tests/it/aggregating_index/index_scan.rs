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
use common_expression::infer_table_schema;
use common_expression::DataBlock;
use common_expression::DataSchemaRefExt;
use common_expression::SendableDataBlockStream;
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
                drop_on: None,
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
    let (_guard, ctx) = create_ee_query_context(None).await.unwrap();
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

    drop_index(fixture.ctx(), index_name).await?;

    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn test_index_scan_two_agg_funcs() -> Result<()> {
    let (_guard, ctx) = create_ee_query_context(None).await.unwrap();
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
    let mut map: HashMap<String, Vec<(usize, usize, usize)>> = HashMap::new();

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
                .and_modify(|v| v.push((index, row, 1)))
                .or_insert(vec![(index, row, 1)]);
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
