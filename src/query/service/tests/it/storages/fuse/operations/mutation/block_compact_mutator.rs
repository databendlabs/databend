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
use common_expression::BlockThresholds;
use common_storages_fuse::io::SegmentWriter;
use common_storages_fuse::io::TableMetaLocationGenerator;
use common_storages_fuse::operations::BlockCompactMutator;
use common_storages_fuse::operations::CompactOptions;
use common_storages_fuse::operations::CompactPartInfo;
use common_storages_fuse::statistics::reducers::merge_statistics_mut;
use common_storages_fuse::FuseTable;
use databend_query::pipelines::executor::ExecutorSettings;
use databend_query::pipelines::executor::PipelineCompleteExecutor;
use databend_query::sessions::QueryContext;
use databend_query::sessions::TableContext;
use rand::thread_rng;
use rand::Rng;
use storages_common_table_meta::meta::Statistics;
use storages_common_table_meta::meta::TableSnapshot;
use uuid::Uuid;

use crate::storages::fuse::block_writer::BlockWriter;
use crate::storages::fuse::operations::mutation::segments_compact_mutator::CompactSegmentTestFixture;
use crate::storages::fuse::table_test_fixture::execute_command;
use crate::storages::fuse::table_test_fixture::execute_query;
use crate::storages::fuse::table_test_fixture::expects_ok;
use crate::storages::fuse::table_test_fixture::TestFixture;

#[tokio::test(flavor = "multi_thread")]
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
        "+----------+----------+",
        "| Column 0 | Column 1 |",
        "+----------+----------+",
        "| 1        | 1        |",
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

#[tokio::test(flavor = "multi_thread")]
async fn test_safety() -> Result<()> {
    let fixture = TestFixture::new().await;
    let ctx = fixture.ctx();
    let operator = ctx.get_data_operator()?.operator();

    let threshold = BlockThresholds {
        max_rows_per_block: 200,
        min_rows_per_block: 100,
        max_bytes_per_block: 1024,
    };

    let data_accessor = operator.clone();
    let location_gen = TableMetaLocationGenerator::with_prefix("test/".to_owned());
    let block_writer = BlockWriter::new(&data_accessor, &location_gen);
    let schema = TestFixture::default_table_schema();
    let segment_writer = SegmentWriter::new(&data_accessor, &location_gen);
    let mut rand = thread_rng();

    for r in 1..100 {
        eprintln!("round {}", r);
        let number_of_segments: usize = rand.gen_range(1..10);

        let mut block_number_of_segments = Vec::with_capacity(number_of_segments);

        for _ in 0..number_of_segments {
            block_number_of_segments.push(rand.gen_range(1..30));
        }

        let number_of_blocks: usize = block_number_of_segments.iter().sum();
        if number_of_blocks < 2 {
            eprintln!("number_of_blocks must large than 1");
            continue;
        }
        eprintln!(
            "generating segments number of segments {},  number of blocks {}",
            number_of_segments, number_of_blocks,
        );

        let (locations, _, segment_infos) = CompactSegmentTestFixture::gen_segments(
            &block_writer,
            &segment_writer,
            &block_number_of_segments,
        )
        .await?;

        eprintln!("data ready");

        let mut summary = Statistics::default();
        for seg in segment_infos {
            merge_statistics_mut(&mut summary, &seg.summary)?;
        }

        let id = Uuid::new_v4();
        let snapshot = TableSnapshot::new(
            id,
            &None,
            None,
            schema.as_ref().clone(),
            summary,
            locations,
            None,
            None,
        );

        let compact_params = CompactOptions {
            base_snapshot: Arc::new(snapshot),
            block_per_seg: 30,
            limit: None,
        };

        eprintln!("running target select");
        let mut block_compact_mutator =
            BlockCompactMutator::new(ctx.clone(), threshold, compact_params, operator.clone());
        block_compact_mutator.target_select().await?;
        let selections = block_compact_mutator.compact_tasks;
        let mut blocks_number = 0;
        for part in selections.partitions.into_iter() {
            let part = CompactPartInfo::from_part(&part)?;
            blocks_number += part.blocks.len();
        }

        assert_eq!(number_of_blocks, blocks_number);
    }

    Ok(())
}
