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
use common_catalog::table::CompactTarget;
use common_catalog::table::Table;
use common_exception::Result;
use common_storages_fuse::FuseTable;
use databend_query::pipelines::executor::ExecutorSettings;
use databend_query::pipelines::executor::PipelineCompleteExecutor;
use databend_query::sessions::QueryContext;
use databend_query::sessions::TableContext;

use crate::storages::fuse::table_test_fixture::execute_command;
use crate::storages::fuse::table_test_fixture::execute_query;
use crate::storages::fuse::table_test_fixture::expects_ok;
use crate::storages::fuse::table_test_fixture::TestFixture;

#[tokio::test]
async fn test_compact() -> Result<()> {
    let fixture = TestFixture::new().await;
    let ctx = fixture.ctx();
    let tbl_name = fixture.default_table_name();
    let db_name = fixture.default_db_name();

    fixture.create_normal_table().await?;

    // insert
    let qry = format!("insert into {}.{}(id) values(10)", db_name, tbl_name);
    execute_command(ctx.clone(), qry.as_str()).await?;

    // compact
    let catalog = ctx.get_catalog(fixture.default_catalog_name().as_str())?;
    let table = catalog
        .get_table(ctx.get_tenant().as_str(), &db_name, &tbl_name)
        .await?;
    let res = do_compact(ctx.clone(), table.clone()).await;
    assert!(res.is_ok());
    assert!(!res.unwrap());

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
    let res = do_compact(ctx.clone(), table.clone()).await;
    assert!(res.is_ok());
    assert!(res.unwrap());

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

async fn do_compact(ctx: Arc<QueryContext>, table: Arc<dyn Table>) -> Result<bool> {
    let fuse_table = FuseTable::try_from_table(table.as_ref())?;
    let settings = ctx.get_settings();
    let mut pipeline = common_pipeline_core::Pipeline::create();
    if fuse_table
        .compact(ctx.clone(), CompactTarget::Blocks, None, &mut pipeline)
        .await?
    {
        pipeline.set_max_threads(settings.get_max_threads()? as usize);
        let query_id = ctx.get_id();
        let executor_settings = ExecutorSettings::try_create(&settings, query_id)?;
        let executor = PipelineCompleteExecutor::try_create(pipeline, executor_settings)?;
        ctx.set_executor(Arc::downgrade(&executor.get_inner()));
        executor.execute()?;
        drop(executor);
        Ok(true)
    } else {
        Ok(false)
    }
}
