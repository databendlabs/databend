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

use std::collections::HashMap;
use std::collections::HashSet;
use std::sync::Arc;

use chrono::Utc;
use common_base::base::tokio;
use common_base::runtime::execute_futures_in_parallel;
use common_exception::ErrorCode;
use common_exception::Result;
use common_expression::BlockThresholds;
use common_expression::DataBlock;
use common_expression::Scalar;
use common_expression::TableSchema;
use common_expression::TableSchemaRef;
use common_storages_fuse::io::MetaWriter;
use common_storages_fuse::io::SegmentWriter;
use common_storages_fuse::io::TableMetaLocationGenerator;
use common_storages_fuse::operations::ReclusterMutator;
use common_storages_fuse::pruning::create_segment_location_vector;
use common_storages_fuse::statistics::reducers::merge_statistics_mut;
use common_storages_fuse::statistics::reducers::reduce_block_metas;
use common_storages_fuse::statistics::sort_by_cluster_stats;
use common_storages_fuse::FuseTable;
use databend_query::sessions::TableContext;
use databend_query::test_kits::table_test_fixture::TestFixture;
use itertools::Itertools;
use rand::thread_rng;
use rand::Rng;
use storages_common_table_meta::meta;
use storages_common_table_meta::meta::BlockMeta;
use storages_common_table_meta::meta::ClusterStatistics;
use storages_common_table_meta::meta::Location;
use storages_common_table_meta::meta::SegmentInfo;
use storages_common_table_meta::meta::Statistics;
use storages_common_table_meta::meta::Versioned;
use uuid::Uuid;

use crate::storages::fuse::operations::mutation::CompactSegmentTestFixture;

#[tokio::test(flavor = "multi_thread")]
async fn test_recluster_mutator_block_select() -> Result<()> {
    let fixture = TestFixture::new().await;
    let ctx = fixture.ctx();
    let location_generator = TableMetaLocationGenerator::with_prefix("_prefix".to_owned());

    let data_accessor = ctx.get_data_operator()?.operator();
    let seg_writer = SegmentWriter::new(&data_accessor, &location_generator);

    let cluster_key_id = 0;
    let gen_test_seg = |cluster_stats: Option<ClusterStatistics>| async {
        let block_id = Uuid::new_v4().simple().to_string();
        let location = (block_id, DataBlock::VERSION);
        let test_block_meta = Arc::new(BlockMeta::new(
            1,
            1,
            1,
            HashMap::default(),
            HashMap::default(),
            cluster_stats,
            location.clone(),
            None,
            0,
            meta::Compression::Lz4Raw,
            Some(Utc::now()),
        ));

        let statistics = reduce_block_metas(
            &[test_block_meta.as_ref()],
            BlockThresholds::default(),
            Some(0),
        );

        let segment = SegmentInfo::new(vec![test_block_meta], statistics);
        Ok::<_, ErrorCode>((seg_writer.write_segment(segment).await?, location))
    };

    let mut test_segment_locations = vec![];
    let mut test_block_locations = vec![];
    let (segment_location, block_location) = gen_test_seg(Some(ClusterStatistics::new(
        cluster_key_id,
        vec![Scalar::from(1i64)],
        vec![Scalar::from(3i64)],
        0,
        None,
    )))
    .await?;
    test_segment_locations.push(segment_location);
    test_block_locations.push(block_location);

    let (segment_location, block_location) = gen_test_seg(Some(ClusterStatistics::new(
        cluster_key_id,
        vec![Scalar::from(2i64)],
        vec![Scalar::from(4i64)],
        0,
        None,
    )))
    .await?;
    test_segment_locations.push(segment_location);
    test_block_locations.push(block_location);

    let (segment_location, block_location) = gen_test_seg(Some(ClusterStatistics::new(
        cluster_key_id,
        vec![Scalar::from(4i64)],
        vec![Scalar::from(5i64)],
        0,
        None,
    )))
    .await?;
    test_segment_locations.push(segment_location);
    test_block_locations.push(block_location);

    let schema = TableSchemaRef::new(TableSchema::empty());
    let ctx: Arc<dyn TableContext> = ctx.clone();
    let segment_locations = create_segment_location_vector(test_segment_locations, None);
    let compact_segments = FuseTable::segment_pruning(
        &ctx,
        schema,
        data_accessor.clone(),
        &None,
        segment_locations,
    )
    .await?;

    let mut mutator =
        ReclusterMutator::try_create(ctx, 1.0, BlockThresholds::default(), cluster_key_id)?;
    let need_recluster = mutator.target_select(compact_segments).await?;
    assert!(need_recluster);
    assert_eq!(mutator.take_blocks().len(), 3);

    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn test_safety_for_recluster() -> Result<()> {
    let fixture = TestFixture::new().await;
    let ctx = fixture.ctx();
    let operator = ctx.get_data_operator()?.operator();

    let cluster_key_id = 0;
    let block_per_seg = 5;
    let threshold = BlockThresholds {
        max_rows_per_block: 5,
        min_rows_per_block: 4,
        max_bytes_per_block: 1024,
    };

    let data_accessor = operator.clone();
    let schema = TestFixture::default_table_schema();
    let mut rand = thread_rng();

    // for r in 1..100 { // <- use this at home
    for r in 1..10 {
        eprintln!("round {}", r);
        let number_of_segments: usize = rand.gen_range(1..10);

        let mut block_number_of_segments = Vec::with_capacity(number_of_segments);
        let mut rows_per_blocks = Vec::with_capacity(number_of_segments);

        for _ in 0..number_of_segments {
            block_number_of_segments.push(rand.gen_range(1..8));
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
            ctx.clone(),
            block_number_of_segments,
            rows_per_blocks,
            threshold,
            Some(cluster_key_id),
            block_per_seg,
        )
        .await?;

        eprintln!("data ready");

        let mut summary = Statistics::default();
        for seg in &segment_infos {
            merge_statistics_mut(&mut summary, &seg.summary, Some(cluster_key_id));
        }

        let mut block_ids = HashSet::new();
        for seg in &segment_infos {
            for b in &seg.blocks {
                block_ids.insert(b.location.clone());
            }
        }

        let ctx: Arc<dyn TableContext> = ctx.clone();
        let segment_locations = create_segment_location_vector(locations, None);
        let compact_segments = FuseTable::segment_pruning(
            &ctx,
            schema.clone(),
            data_accessor.clone(),
            &None,
            segment_locations,
        )
        .await?;

        let mut need_recluster = false;
        let mut mutator =
            ReclusterMutator::try_create(ctx.clone(), 1.0, threshold, cluster_key_id)?;
        let selected_segs =
            ReclusterMutator::select_segments(&compact_segments, block_per_seg, 8, cluster_key_id)?;
        if selected_segs.is_empty() {
            for compact_segment in compact_segments.into_iter() {
                if !ReclusterMutator::segment_can_recluster(
                    &compact_segment.1.summary,
                    block_per_seg,
                    cluster_key_id,
                ) {
                    continue;
                }

                if mutator.target_select(vec![compact_segment]).await? {
                    need_recluster = true;
                    break;
                }
            }
        } else {
            let mut selected_segments = Vec::with_capacity(selected_segs.len());
            selected_segs.into_iter().for_each(|i| {
                selected_segments.push(compact_segments[i].clone());
            });
            need_recluster = mutator.target_select(selected_segments).await?;
        }

        eprintln!("need_recluster: {}", need_recluster);
        if need_recluster {
            let mut blocks = mutator.take_blocks();
            let remained_blocks = std::mem::take(&mut mutator.remained_blocks);
            eprintln!(
                "selected segments number {}, selected blocks number {}, remained blocks number {}",
                mutator.removed_segment_indexes.len(),
                blocks.len(),
                remained_blocks.len()
            );
            blocks.extend(remained_blocks);

            let mut block_ids_after_target = HashSet::new();
            for b in &blocks {
                block_ids_after_target.insert(b.location.clone());
            }

            let mut origin_blocks_ids = HashSet::new();
            for idx in &mutator.removed_segment_indexes {
                for b in &segment_infos[*idx].blocks {
                    origin_blocks_ids.insert(b.location.clone());
                }
            }
            assert_eq!(block_ids_after_target, origin_blocks_ids);

            // test recluster aggregator.
            let mut new_segments =
                generage_segments(ctx, blocks, threshold, cluster_key_id, block_per_seg).await?;
            let new_segments_len = new_segments.len();
            let removed_segments_len = mutator.removed_segment_indexes.len();
            let replaced_segments_len = new_segments_len.min(removed_segments_len);
            let mut merged_statistics = Statistics::default();
            let mut appended_segments = Vec::new();
            let mut replaced_segments = HashMap::with_capacity(replaced_segments_len);

            if new_segments_len > removed_segments_len {
                let appended = new_segments.split_off(removed_segments_len);
                for (location, stats) in appended.into_iter().rev() {
                    appended_segments.push(location);
                    merge_statistics_mut(&mut merged_statistics, &stats, Some(cluster_key_id));
                }
            }

            let mut indices = Vec::with_capacity(removed_segments_len);
            for (i, (location, stats)) in new_segments.into_iter().enumerate() {
                let idx = mutator.removed_segment_indexes[i];
                indices.push(idx);
                replaced_segments.insert(idx, location);
                merge_statistics_mut(&mut merged_statistics, &stats, Some(cluster_key_id));
            }

            let removed_segment_indexes =
                mutator.removed_segment_indexes[replaced_segments_len..].to_vec();
            eprintln!(
                "append segments number {}, replaced segments number {}, removed segments number {}",
                appended_segments.len(),
                replaced_segments.len(),
                removed_segment_indexes.len()
            );
            indices.extend(removed_segment_indexes);
            assert_eq!(indices, mutator.removed_segment_indexes);
            assert_eq!(merged_statistics, mutator.removed_segment_summary);
            assert_eq!(
                new_segments_len,
                appended_segments.len() + replaced_segments.len()
            );
        }
    }

    Ok(())
}

#[test]
fn test_check_point() {
    // [1,2] [2,3], check point 2
    let start = vec![1];
    let end = vec![0];
    assert!(ReclusterMutator::check_point(&start, &end));

    // [1,2] [2,2], check point 2
    let start = vec![1];
    let end = vec![0, 1];
    assert!(ReclusterMutator::check_point(&start, &end));

    // [1,1] [1,2], check point 1
    let start = vec![0, 1];
    let end = vec![0];
    assert!(ReclusterMutator::check_point(&start, &end));

    // [1,2] [1,3], check point 1
    let start = vec![0, 1];
    let end = vec![];
    assert!(!ReclusterMutator::check_point(&start, &end));

    // [1,3] [2,3], check point 3
    let start = vec![];
    let end = vec![0, 1];
    assert!(!ReclusterMutator::check_point(&start, &end));

    // [1,3] [3,3] [3,4], check point 3
    let start = vec![1, 2];
    let end = vec![0, 1];
    assert!(!ReclusterMutator::check_point(&start, &end));
}

async fn generage_segments(
    ctx: Arc<dyn TableContext>,
    blocks: Vec<Arc<BlockMeta>>,
    block_thresholds: BlockThresholds,
    default_cluster_key: u32,
    block_per_seg: usize,
) -> Result<Vec<(Location, Statistics)>> {
    let location_gen = TableMetaLocationGenerator::with_prefix("test/".to_owned());
    let data_accessor = ctx.get_data_operator()?.operator();

    let mut merged_blocks = blocks;
    // sort ascending.
    merged_blocks.sort_by(|a, b| {
        sort_by_cluster_stats(&a.cluster_stats, &b.cluster_stats, default_cluster_key)
    });

    let mut tasks = Vec::new();
    let segments_num = (merged_blocks.len() / block_per_seg).max(1);
    let chunk_size = merged_blocks.len().div_ceil(segments_num);
    for chunk in &merged_blocks.into_iter().chunks(chunk_size) {
        let new_blocks = chunk.collect::<Vec<_>>();
        let location_gen = location_gen.clone();
        let data_accessor = data_accessor.clone();
        tasks.push(async move {
            let new_summary =
                reduce_block_metas(&new_blocks, block_thresholds, Some(default_cluster_key));
            let segment_info = SegmentInfo::new(new_blocks, new_summary.clone());

            let path = location_gen.gen_segment_info_location();
            segment_info.write_meta(&data_accessor, &path).await?;
            Ok::<_, ErrorCode>(((path, SegmentInfo::VERSION), new_summary))
        });
    }

    let threads_nums = ctx.get_settings().get_max_threads()? as usize;
    execute_futures_in_parallel(
        tasks,
        threads_nums,
        threads_nums,
        "fuse-write-segments-worker".to_owned(),
    )
    .await?
    .into_iter()
    .collect::<Result<Vec<_>>>()
}
