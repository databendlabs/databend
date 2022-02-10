//  Copyright 2021 Datafuse Labs.
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
//

use common_base::tokio;
use common_exception::Result;
use common_planners::ReadDataSourcePlan;
use common_planners::TruncateTablePlan;
use databend_query::catalogs::Catalog;
use databend_query::interpreters::InterpreterFactory;
use databend_query::sql::PlanParser;
use databend_query::storages::fuse::TBL_OPT_KEY_CHUNK_BLOCK_NUM;
use databend_query::storages::ToReadDataSourcePlan;
use futures::TryStreamExt;

use crate::storages::fuse::table_test_fixture::TestFixture;

#[tokio::test]
async fn test_fuse_table_normal_case() -> Result<()> {
    let fixture = TestFixture::new().await;
    let ctx = fixture.ctx();

    let create_table_plan = fixture.default_crate_table_plan();
    let catalog = ctx.get_catalog();
    catalog.create_table(create_table_plan.into()).await?;

    let mut table = fixture.latest_default_table().await?;

    // basic append and read
    {
        let num_blocks = 2;
        let rows_per_block = 2;
        let value_start_from = 1;
        let stream =
            TestFixture::gen_sample_blocks_stream_ex(num_blocks, rows_per_block, value_start_from);

        let r = table.append_data(ctx.clone(), stream).await?;
        table
            .commit_insertion(ctx.clone(), r.try_collect().await?, false)
            .await?;

        // get the latest tbl
        let prev_version = table.get_table_info().ident.version;
        table = fixture.latest_default_table().await?;
        assert_ne!(prev_version, table.get_table_info().ident.version);

        let (stats, parts) = table.read_partitions(ctx.clone(), None).await?;
        assert_eq!(stats.read_rows, num_blocks * rows_per_block);

        ctx.try_set_partitions(parts)?;
        let stream = table
            .read(ctx.clone(), &ReadDataSourcePlan {
                table_info: Default::default(),
                scan_fields: None,
                parts: Default::default(),
                statistics: Default::default(),
                description: "".to_string(),
                tbl_args: None,
                push_downs: None,
            })
            .await?;
        let blocks = stream.try_collect::<Vec<_>>().await?;
        let rows: usize = blocks.iter().map(|block| block.num_rows()).sum();
        assert_eq!(rows, num_blocks * rows_per_block);

        // recall our test setting:
        //   - num_blocks = 2;
        //   - rows_per_block = 2;
        //   - value_start_from = 1
        // thus
        let expected = vec![
            "+----+", //
            "| id |", //
            "+----+", //
            "| 1  |", //
            "| 1  |", //
            "| 2  |", //
            "| 2  |", //
            "+----+", //
        ];
        common_datablocks::assert_blocks_sorted_eq(expected, blocks.as_slice());
    }

    // test commit with overwrite

    {
        // insert overwrite 5 blocks
        let num_blocks = 2;
        let rows_per_block = 2;
        let value_start_from = 2;
        let stream =
            TestFixture::gen_sample_blocks_stream_ex(num_blocks, rows_per_block, value_start_from);

        let r = table.append_data(ctx.clone(), stream).await?;
        table
            .commit_insertion(ctx.clone(), r.try_collect().await?, true)
            .await?;

        // get the latest tbl
        let prev_version = table.get_table_info().ident.version;
        let table = fixture.latest_default_table().await?;
        assert_ne!(prev_version, table.get_table_info().ident.version);

        let (stats, parts) = table.read_partitions(ctx.clone(), None).await?;
        assert_eq!(stats.read_rows, num_blocks * rows_per_block);

        // inject partitions to current ctx
        ctx.try_set_partitions(parts)?;

        let stream = table
            .read(ctx.clone(), &ReadDataSourcePlan {
                table_info: Default::default(),
                scan_fields: None,
                parts: Default::default(),
                statistics: Default::default(),
                description: "".to_string(),
                tbl_args: None,
                push_downs: None,
            })
            .await?;
        let blocks = stream.try_collect::<Vec<_>>().await?;
        let rows: usize = blocks.iter().map(|block| block.num_rows()).sum();
        assert_eq!(rows, num_blocks * rows_per_block);

        // two block, two rows for each block, value starts with 2
        let expected = vec![
            "+----+", //
            "| id |", //
            "+----+", //
            "| 2  |", //
            "| 2  |", //
            "| 3  |", //
            "| 3  |", //
            "+----+", //
        ];
        common_datablocks::assert_blocks_sorted_eq(expected, blocks.as_slice());
    }

    Ok(())
}

// TODO move this to test/it/storages/fuse/operations
#[tokio::test]
async fn test_fuse_table_truncate() -> Result<()> {
    let fixture = TestFixture::new().await;
    let ctx = fixture.ctx();

    let create_table_plan = fixture.default_crate_table_plan();
    let catalog = ctx.get_catalog();
    catalog.create_table(create_table_plan.into()).await?;

    let table = fixture.latest_default_table().await?;
    let truncate_plan = TruncateTablePlan {
        db: fixture.default_db_name(),
        table: fixture.default_table_name(),
        purge: false,
    };

    // 1. truncate empty table
    let prev_version = table.get_table_info().ident.version;
    let r = table.truncate(ctx.clone(), truncate_plan.clone()).await;
    let table = fixture.latest_default_table().await?;
    // no side effects
    assert_eq!(prev_version, table.get_table_info().ident.version);
    assert!(r.is_ok());

    // 2. truncate table which has data
    let num_blocks = 10;
    let rows_per_block = 3;
    let value_start_from = 1;
    let stream =
        TestFixture::gen_sample_blocks_stream_ex(num_blocks, rows_per_block, value_start_from);

    let r = table.append_data(ctx.clone(), stream).await?;
    table
        .commit_insertion(ctx.clone(), r.try_collect().await?, false)
        .await?;
    let source_plan = table.read_plan(ctx.clone(), None).await?;

    // get the latest tbl
    let prev_version = table.get_table_info().ident.version;
    let table = fixture.latest_default_table().await?;
    assert_ne!(prev_version, table.get_table_info().ident.version);

    // ensure data ingested
    let (stats, _) = table
        .read_partitions(ctx.clone(), source_plan.push_downs.clone())
        .await?;
    assert_eq!(stats.read_rows, (num_blocks * rows_per_block) as usize);

    // truncate
    let r = table.truncate(ctx.clone(), truncate_plan).await;
    assert!(r.is_ok());

    // get the latest tbl
    let prev_version = table.get_table_info().ident.version;
    let table = fixture.latest_default_table().await?;
    assert_ne!(prev_version, table.get_table_info().ident.version);
    let (stats, parts) = table
        .read_partitions(ctx.clone(), source_plan.push_downs.clone())
        .await?;
    // cleared?
    assert_eq!(parts.len(), 0);
    assert_eq!(stats.read_rows, 0);

    Ok(())
}

// TODO move this to test/it/storages/fuse/operations
#[tokio::test]
async fn test_fuse_table_optimize() -> Result<()> {
    let fixture = TestFixture::new().await;
    let ctx = fixture.ctx();

    let mut create_table_plan = fixture.default_crate_table_plan();
    // set chunk size to 100
    create_table_plan
        .table_meta
        .options
        .insert(TBL_OPT_KEY_CHUNK_BLOCK_NUM.to_owned(), 100.to_string());

    // create test table
    let tbl_name = create_table_plan.table.clone();
    let db_name = create_table_plan.db.clone();
    let catalog = ctx.get_catalog();
    catalog.create_table(create_table_plan.into()).await?;

    // insert 5 times
    let n = 5;
    for _ in 0..n {
        let table = fixture.latest_default_table().await?;
        let num_blocks = 1;
        let stream = TestFixture::gen_sample_blocks_stream(num_blocks, 1);
        let r = table.append_data(ctx.clone(), stream).await?;
        table
            .commit_insertion(ctx.clone(), r.try_collect().await?, false)
            .await?;
    }

    // there will be 5 blocks
    let table = fixture.latest_default_table().await?;
    let (_, parts) = table.read_partitions(ctx.clone(), None).await?;
    assert_eq!(parts.len(), n);

    // do compact
    let query = format!("optimize table {}.{} compact", db_name, tbl_name);

    let plan = PlanParser::parse(ctx.clone(), &query).await?;
    let interpreter = InterpreterFactory::get(ctx.clone(), plan)?;

    // `PipelineBuilder` will parallelize the table reading according to value of setting `max_threads`,
    // and `Table::read` will also try to de-queue read jobs preemptively. thus, the number of blocks
    // that `Table::append` takes are not deterministic (`append` is also executed parallelly in this case),
    // therefore, the final number of blocks varies.
    // To avoid flaky test, the value of setting `max_threads` is set to be 1, so that pipeline_builder will
    // only arrange one worker for the `ReadDataSourcePlan`.
    ctx.get_settings().set_max_threads(1)?;
    let data_stream = interpreter.execute(None).await?;
    let _ = data_stream.try_collect::<Vec<_>>();

    // verify compaction
    let table = fixture.latest_default_table().await?;
    let (_, parts) = table.read_partitions(ctx.clone(), None).await?;
    // blocks are so tiny, they should be compacted into one
    assert_eq!(parts.len(), 1);

    Ok(())
}
