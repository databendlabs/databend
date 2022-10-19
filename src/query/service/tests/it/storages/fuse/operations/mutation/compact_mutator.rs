//  Copyright 2022 Datafuse Labs.
//
//  Licensed under the Apache License, Version 2.0 (the "License");
//  you may not use this file except in compliance with the License.
//  You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
//  Unless required by applicable law or agreed to in writing, software
//  distributed under the License is distributed on an "AS IS" BASIS,
//  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
//  See the License for the specific language governing permissions and
//  limitations under the License.

use std::sync::Arc;

use common_base::base::tokio;
use common_catalog::table::Table;
use common_catalog::table_mutator::TableMutator;
use common_exception::ErrorCode;
use common_exception::Result;
use common_storages_fuse::FuseTable;
use databend_query::interpreters::CreateTableInterpreterV2;
use databend_query::interpreters::Interpreter;
use databend_query::pipelines::executor::ExecutorSettings;
use databend_query::pipelines::executor::PipelineCompleteExecutor;
use databend_query::sessions::QueryContext;
use databend_query::sessions::TableContext;
use databend_query::sql::plans::Plan;
use databend_query::sql::Planner;

use crate::storages::fuse::table_test_fixture::execute_command;
use crate::storages::fuse::table_test_fixture::execute_query;
use crate::storages::fuse::table_test_fixture::expects_ok;
use crate::storages::fuse::table_test_fixture::TestFixture;

#[tokio::test]
async fn test_compact() -> Result<()> {
    let fixture = TestFixture::new().await;
    let ctx = fixture.ctx();
    let create_table_plan = fixture.create_normal_table_plan();

    // create test table
    let tbl_name = create_table_plan.table.clone();
    let db_name = create_table_plan.database.clone();
    let interpreter = CreateTableInterpreterV2::try_create(ctx.clone(), create_table_plan)?;
    interpreter.execute(ctx.clone()).await?;

    // insert
    for i in 0..9 {
        let qry = format!("insert into {}.{}(id) values({})", db_name, tbl_name, i);
        execute_command(ctx.clone(), qry.as_str()).await?;
    }

    // compact
    let catalog = ctx.get_catalog(fixture.default_catalog_name().as_str())?;
    let table = catalog
        .get_table(ctx.get_tenant().as_str(), &db_name, &tbl_name)
        .await?;
    let mutator = build_mutator(ctx.clone(), table.clone()).await?;

    // compact commit
    mutator.try_commit(table).await?;

    // check count
    let expected = vec![
        "+---------------+-------+",
        "| segment_count | count |",
        "+---------------+-------+",
        "| 1             | 1     |",
        "+---------------+-------+",
    ];
    let qry = format!(
        "select segment_count, block_count as count from fuse_snapshot('{}', '{}') limit 1",
        db_name, tbl_name
    );
    expects_ok(
        "check segment and block count",
        execute_query(fixture.ctx(), qry.as_str()).await,
        expected,
    )
    .await?;
    Ok(())
}

#[tokio::test]
async fn test_compact_resolved_conflict() -> Result<()> {
    let fixture = TestFixture::new().await;
    let ctx = fixture.ctx();
    let create_table_plan = fixture.create_normal_table_plan();

    // create test table
    let tbl_name = create_table_plan.table.clone();
    let db_name = create_table_plan.database.clone();
    let interpreter = CreateTableInterpreterV2::try_create(ctx.clone(), create_table_plan)?;
    interpreter.execute(ctx.clone()).await?;

    // insert
    for i in 0..9 {
        let qry = format!("insert into {}.{}(id) values({})", db_name, tbl_name, i);
        execute_command(ctx.clone(), qry.as_str()).await?;
    }

    // compact
    let catalog = ctx.get_catalog(fixture.default_catalog_name().as_str())?;
    let table = catalog
        .get_table(ctx.get_tenant().as_str(), &db_name, &tbl_name)
        .await?;
    let mutator = build_mutator(ctx.clone(), table.clone()).await?;

    // insert
    let qry = format!("insert into {}.{}(id) values(10)", db_name, tbl_name);
    execute_command(ctx.clone(), qry.as_str()).await?;

    // compact commit
    mutator.try_commit(table).await?;

    // check count
    let expected = vec![
        "+---------------+-------+",
        "| segment_count | count |",
        "+---------------+-------+",
        "| 2             | 2     |",
        "+---------------+-------+",
    ];
    let qry = format!(
        "select segment_count, block_count as count from fuse_snapshot('{}', '{}') limit 1",
        db_name, tbl_name
    );
    expects_ok(
        "check segment and block count",
        execute_query(fixture.ctx(), qry.as_str()).await,
        expected,
    )
    .await?;
    Ok(())
}

#[tokio::test]
async fn test_compact_unresolved_conflict() -> Result<()> {
    let fixture = TestFixture::new().await;
    let ctx = fixture.ctx();
    let create_table_plan = fixture.create_normal_table_plan();

    // create test table
    let tbl_name = create_table_plan.table.clone();
    let db_name = create_table_plan.database.clone();
    let interpreter = CreateTableInterpreterV2::try_create(ctx.clone(), create_table_plan)?;
    interpreter.execute(ctx.clone()).await?;

    // insert
    for i in 0..9 {
        let qry = format!("insert into {}.{}(id) values({})", db_name, tbl_name, i);
        execute_command(ctx.clone(), qry.as_str()).await?;
    }

    // compact
    let catalog = ctx.get_catalog(fixture.default_catalog_name().as_str())?;
    let table = catalog
        .get_table(ctx.get_tenant().as_str(), &db_name, &tbl_name)
        .await?;
    let mutator = build_mutator(ctx.clone(), table.clone()).await?;

    // delete
    let query = format!("delete from {}.{} where id=1", db_name, tbl_name);
    let mut planner = Planner::new(ctx.clone());
    let (plan, _, _) = planner.plan_sql(&query).await?;
    if let Plan::Delete(delete) = plan {
        table.delete(ctx.clone(), *delete.clone()).await?;
    }

    // compact commit
    let r = mutator.try_commit(table).await;
    assert!(r.is_err());
    assert_eq!(r.err().unwrap().code(), ErrorCode::storage_other_code());

    Ok(())
}

async fn build_mutator(
    ctx: Arc<QueryContext>,
    table: Arc<dyn Table>,
) -> Result<Arc<dyn TableMutator>> {
    let fuse_table = FuseTable::try_from_table(table.as_ref())?;
    let settings = ctx.get_settings();
    settings.set_max_threads(1)?;
    let mut pipeline = common_pipeline_core::Pipeline::create();
    let mutator = fuse_table.compact(ctx.clone(), &mut pipeline).await?;
    assert!(mutator.is_some());
    let mutator = mutator.unwrap();
    pipeline.set_max_threads(1);
    let executor_settings = ExecutorSettings::try_create(&settings)?;
    let executor = PipelineCompleteExecutor::try_create(pipeline, executor_settings)?;
    ctx.set_executor(Arc::downgrade(&executor.get_inner()));
    executor.execute()?;
    drop(executor);
    Ok(mutator)
}
