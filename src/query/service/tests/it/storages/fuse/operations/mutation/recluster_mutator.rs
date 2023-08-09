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

use std::collections::BTreeMap;
use std::collections::HashMap;
use std::sync::Arc;

use chrono::Utc;
use common_base::base::tokio;
use common_exception::ErrorCode;
use common_exception::Result;
use common_expression::BlockThresholds;
use common_expression::DataBlock;
use common_expression::Scalar;
use common_expression::TableSchema;
use common_expression::TableSchemaRef;
use common_sql::BloomIndexColumns;
use common_storages_fuse::operations::BlockMetaIndex;
use common_storages_fuse::pruning::create_segment_location_vector;
use common_storages_fuse::pruning::FusePruner;
use common_storages_fuse::statistics::reducers::reduce_block_metas;
use databend_query::sessions::TableContext;
use databend_query::storages::fuse::io::SegmentWriter;
use databend_query::storages::fuse::io::TableMetaLocationGenerator;
use databend_query::storages::fuse::operations::ReclusterMutator;
use databend_query::test_kits::table_test_fixture::TestFixture;
use storages_common_table_meta::meta;
use storages_common_table_meta::meta::BlockMeta;
use storages_common_table_meta::meta::ClusterStatistics;
use storages_common_table_meta::meta::SegmentInfo;
use storages_common_table_meta::meta::Statistics;
use storages_common_table_meta::meta::TableSnapshot;
use storages_common_table_meta::meta::Versioned;
use uuid::Uuid;

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

    let base_snapshot = TableSnapshot::new(
        Uuid::new_v4(),
        &None,
        None,
        TableSchema::empty(),
        Statistics::default(),
        test_segment_locations.clone(),
        Some((0, "(id)".to_string())),
        None,
    );
    let base_snapshot = Arc::new(base_snapshot);

    let schema = TableSchemaRef::new(TableSchema::empty());
    let ctx: Arc<dyn TableContext> = ctx.clone();
    let segment_locations = base_snapshot.segments.clone();
    let segment_locations = create_segment_location_vector(segment_locations, None);
    let block_metas = FusePruner::create(
        &ctx,
        data_accessor.clone(),
        schema,
        &None,
        BloomIndexColumns::All,
    )?
    .read_pruning(segment_locations)
    .await?;
    let mut blocks_map: BTreeMap<i32, Vec<(BlockMetaIndex, Arc<BlockMeta>)>> = BTreeMap::new();
    block_metas.iter().for_each(|(idx, b)| {
        if let Some(stats) = &b.cluster_stats {
            blocks_map.entry(stats.level).or_default().push((
                BlockMetaIndex {
                    segment_idx: idx.segment_idx,
                    block_idx: idx.block_idx,
                },
                b.clone(),
            ));
        }
    });

    let mut mutator = ReclusterMutator::try_create(ctx, 1.0, BlockThresholds::default())?;

    let need_recluster = mutator.target_select(blocks_map).await?;
    assert!(need_recluster);
    assert_eq!(mutator.take_blocks().len(), 3);

    Ok(())
}
