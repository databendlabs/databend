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

use common_base::base::tokio;
use common_exception::Result;
use common_expression::infer_table_schema;
use common_expression::DataBlock;
use common_expression::SendableDataBlockStream;
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

async fn create_index(ctx: Arc<QueryContext>, index_name: &str, query: &str) -> Result<()> {
    let sql = format!("CREATE AGGREGATING INDEX {index_name} AS {query}");
    execute_sql(ctx, &sql).await?;

    Ok(())
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

    create_index(
        fixture.ctx(),
        index_name,
        "SELECT b, SUM(a) from t WHERE c > 1 GROUP BY b",
    )
    .await?;

    // Refresh Index
    refresh_index(
        fixture.ctx(),
        "SELECT b, SUM_state(a), _block_name from t WHERE c > 1 GROUP BY b, _block_name",
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

async fn refresh_index(ctx: Arc<QueryContext>, sql: &str) -> Result<()> {
    let plan = plan_sql(ctx.clone(), sql).await?;
    let output_schema = plan.schema();
    let index_schema = infer_table_schema(&output_schema)?;
    let block_name_index = output_schema.index_of("_block_name")?;
    assert_eq!(block_name_index, output_schema.num_fields() - 1);

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
                .or_insert(vec![]);
        }

        buffer.push(result);
        index += 1;
    }

    let op = ctx.get_data_operator()?.operator();

    for (loc, indices) in map {
        let index_block =
            DataBlock::take_blocks(&buffer.iter().collect::<Vec<_>>(), &indices, indices.len());
        let mut buf = vec![];

        serialize_block(
            &WriteSettings::default(),
            &index_schema,
            index_block,
            &mut buf,
        )?;

        let index_loc =
            TableMetaLocationGenerator::gen_agg_index_location_from_block_location(&loc, 0);

        op.write(&index_loc, buf).await?;
    }

    Ok(())
}
