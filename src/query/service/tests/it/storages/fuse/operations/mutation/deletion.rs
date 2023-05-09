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
use common_exception::Result;
use common_sql::plans::DeletePlan;
use common_sql::plans::Plan;
use common_sql::Planner;
use common_storages_factory::Table;
use common_storages_fuse::FuseTable;
use databend_query::pipelines::executor::ExecutorSettings;
use databend_query::pipelines::executor::PipelineCompleteExecutor;
use databend_query::sessions::QueryContext;
use databend_query::sessions::TableContext;
use databend_query::test_kits::table_test_fixture::execute_command;
use databend_query::test_kits::table_test_fixture::execute_query;
use databend_query::test_kits::table_test_fixture::expects_ok;
use databend_query::test_kits::table_test_fixture::TestFixture;

#[tokio::test(flavor = "multi_thread")]
async fn test_deletion_mutator_multiple_empty_segments() -> Result<()> {
    let fixture = TestFixture::new().await;
    let ctx = fixture.ctx();
    let tbl_name = fixture.default_table_name();
    let db_name = fixture.default_db_name();

    fixture.create_normal_table().await?;

    // insert
    for i in 0..10 {
        let qry = format!("insert into {}.{}(id) values({})", db_name, tbl_name, i);
        execute_command(ctx.clone(), qry.as_str()).await?;
    }

    let catalog = ctx.get_catalog(fixture.default_catalog_name().as_str())?;
    let table = catalog
        .get_table(ctx.get_tenant().as_str(), &db_name, &tbl_name)
        .await?;
    // delete
    let query = format!("delete from {}.{} where id=1", db_name, tbl_name);
    let mut planner = Planner::new(ctx.clone());
    let (plan, _) = planner.plan_sql(&query).await?;
    if let Plan::Delete(delete) = plan {
        do_deletion(ctx.clone(), table.clone(), *delete).await?;
    }

    // check count
    let expected = vec![
        "+----------+----------+",
        "| Column 0 | Column 1 |",
        "+----------+----------+",
        "| 9        | 9        |",
        "+----------+----------+",
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

pub async fn do_deletion(
    ctx: Arc<QueryContext>,
    table: Arc<dyn Table>,
    plan: DeletePlan,
) -> Result<()> {
    let (filter, col_indices) = if let Some(scalar) = &plan.selection {
        (
            Some(
                scalar
                    .as_expr()?
                    .project_column_ref(|col| col.column_name.clone())
                    .as_remote_expr(),
            ),
            scalar.used_columns().into_iter().collect(),
        )
    } else {
        (None, vec![])
    };

    let fuse_table = FuseTable::try_from_table(table.as_ref())?;
    let settings = ctx.get_settings();
    let mut pipeline = common_pipeline_core::Pipeline::create();
    fuse_table
        .delete(ctx.clone(), filter, col_indices, &mut pipeline)
        .await?;

    if !pipeline.is_empty() {
        pipeline.set_max_threads(settings.get_max_threads()? as usize);
        let query_id = ctx.get_id();
        let executor_settings = ExecutorSettings::try_create(&settings, query_id)?;
        let executor = PipelineCompleteExecutor::try_create(pipeline, executor_settings)?;
        ctx.set_executor(executor.get_inner())?;
        executor.execute()?;
    }
    Ok(())
}
