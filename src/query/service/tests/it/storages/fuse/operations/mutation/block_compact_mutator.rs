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

use std::collections::HashSet;
use std::sync::Arc;

use common_base::base::tokio;
use common_catalog::table::CompactTarget;
use common_catalog::table::Table;
use common_exception::Result;
use common_expression::BlockThresholds;
use common_storages_fuse::io::MetaReaders;
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
use databend_query::test_kits::block_writer::BlockWriter;
use databend_query::test_kits::table_test_fixture::execute_command;
use databend_query::test_kits::table_test_fixture::execute_query;
use databend_query::test_kits::table_test_fixture::expects_ok;
use databend_query::test_kits::table_test_fixture::TestFixture;
use rand::thread_rng;
use rand::Rng;
use storages_common_cache::LoadParams;
use storages_common_table_meta::meta::SegmentInfo;
use storages_common_table_meta::meta::Statistics;
use storages_common_table_meta::meta::TableSnapshot;
use uuid::Uuid;

use crate::storages::fuse::operations::mutation::segments_compact_mutator::CompactSegmentTestFixture;

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
    fuse_table
        .compact(ctx.clone(), CompactTarget::Blocks, None, &mut pipeline)
        .await?;

    if !pipeline.is_empty() {
        pipeline.set_max_threads(settings.get_max_threads()? as usize);
        let query_id = ctx.get_id();
        let executor_settings = ExecutorSettings::try_create(&settings, query_id)?;
        let executor = PipelineCompleteExecutor::try_create(pipeline, executor_settings)?;
        ctx.set_executor(executor.get_inner())?;
        executor.execute()?;
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
    let settings = ctx.get_settings();
    settings.set_max_threads(2)?;
    settings.set_max_storage_io_requests(4)?;

    let threshold = BlockThresholds {
        max_rows_per_block: 5,
        min_rows_per_block: 4,
        max_bytes_per_block: 1024,
    };

    let data_accessor = operator.clone();
    let location_gen = TableMetaLocationGenerator::with_prefix("test/".to_owned());
    let block_writer = BlockWriter::new(&data_accessor, &location_gen);
    let schema = TestFixture::default_table_schema();
    let segment_writer = SegmentWriter::new(&data_accessor, &location_gen);
    let mut rand = thread_rng();

    // for r in 1..100 { // <- use this at home
    for r in 1..10 {
        eprintln!("round {}", r);
        let number_of_segments: usize = rand.gen_range(1..10);

        let mut block_number_of_segments = Vec::with_capacity(number_of_segments);
        let mut rows_per_blocks = Vec::with_capacity(number_of_segments);

        for _ in 0..number_of_segments {
            block_number_of_segments.push(rand.gen_range(1..25));
            rows_per_blocks.push(rand.gen_range(1..8));
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
            &rows_per_blocks,
            threshold,
        )
        .await?;

        eprintln!("data ready");

        let mut summary = Statistics::default();
        for seg in &segment_infos {
            merge_statistics_mut(&mut summary, &seg.summary)?;
        }

        let mut block_ids = HashSet::new();
        for seg in &segment_infos {
            for b in &seg.blocks {
                block_ids.insert(b.location.clone());
            }
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

        let limit: usize = rand.gen_range(1..15);
        let compact_params = CompactOptions {
            base_snapshot: Arc::new(snapshot),
            block_per_seg: 10,
            limit: Some(limit),
        };

        eprintln!("running target select");
        let mut block_compact_mutator =
            BlockCompactMutator::new(ctx.clone(), threshold, compact_params, operator.clone());
        block_compact_mutator.target_select().await?;
        let selections = block_compact_mutator.compact_tasks;
        let mut blocks_number = 0;

        let mut block_ids_after_compaction = HashSet::new();
        for part in selections.partitions.into_iter() {
            let part = CompactPartInfo::from_part(&part)?;
            blocks_number += part.blocks.len();
            for b in &part.blocks {
                block_ids_after_compaction.insert(b.location.clone());
            }
        }

        for unchanged in block_compact_mutator.unchanged_blocks_map.values() {
            blocks_number += unchanged.len();
            for b in unchanged.values() {
                block_ids_after_compaction.insert(b.location.clone());
            }
        }

        let compact_segment_reader = MetaReaders::segment_info_reader(
            ctx.get_data_operator()?.operator(),
            TestFixture::default_table_schema(),
        );
        for unchanged_segment in block_compact_mutator.unchanged_segments_map.values() {
            let param = LoadParams {
                location: unchanged_segment.0.clone(),
                len_hint: None,
                ver: unchanged_segment.1,
                put_cache: false,
            };
            let compact_segment = compact_segment_reader.read(&param).await?;
            let segment = SegmentInfo::try_from(compact_segment.as_ref())?;
            blocks_number += segment.blocks.len();
            for b in &segment.blocks {
                block_ids_after_compaction.insert(b.location.clone());
            }
        }
        assert_eq!(number_of_blocks, blocks_number);
        assert_eq!(block_ids, block_ids_after_compaction);
    }

    Ok(())
}
