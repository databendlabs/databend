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
use std::sync::Arc;

use common_base::base::tokio;
use common_catalog::table_mutator::TableMutator;
use common_datablocks::DataBlock;
use common_datavalues::DataSchema;
use common_datavalues::DataValue;
use common_exception::ErrorCode;
use common_exception::Result;
use common_fuse_meta::caches::CacheManager;
use common_fuse_meta::meta::BlockMeta;
use common_fuse_meta::meta::ClusterStatistics;
use common_fuse_meta::meta::SegmentInfo;
use common_fuse_meta::meta::Statistics;
use common_fuse_meta::meta::TableSnapshot;
use common_fuse_meta::meta::Versioned;
use databend_query::sessions::TableContext;
use databend_query::storages::fuse::io::BlockCompactor;
use databend_query::storages::fuse::io::SegmentWriter;
use databend_query::storages::fuse::io::TableMetaLocationGenerator;
use databend_query::storages::fuse::operations::ReclusterMutator;
use uuid::Uuid;

use crate::storages::fuse::table_test_fixture::TestFixture;

#[tokio::test]
async fn test_recluster_mutator_block_select() -> Result<()> {
    let fixture = TestFixture::new().await;
    let ctx = fixture.ctx();
    let location_generator = TableMetaLocationGenerator::with_prefix("_prefix".to_owned());

    let segment_info_cache = CacheManager::instance().get_table_segment_cache();
    let data_accessor = ctx.get_storage_operator()?;
    let seg_writer = SegmentWriter::new(&data_accessor, &location_generator, &segment_info_cache);

    let gen_test_seg = |cluster_stats: Option<ClusterStatistics>| async {
        let block_id = Uuid::new_v4().simple().to_string();
        let location = (block_id, DataBlock::VERSION);
        let test_block_meta = BlockMeta::new(
            1,
            1,
            1,
            HashMap::default(),
            HashMap::default(),
            cluster_stats,
            location.clone(),
            None,
            0,
        );
        let segment = SegmentInfo::new(vec![test_block_meta], Statistics::default());
        Ok::<_, ErrorCode>((seg_writer.write_segment(segment).await?, location))
    };

    let mut test_segment_locations = vec![];
    let mut test_block_locations = vec![];
    let (segment_location, block_location) = gen_test_seg(Some(ClusterStatistics {
        cluster_key_id: 0,
        min: vec![DataValue::Int64(1)],
        max: vec![DataValue::Int64(3)],
        level: 0,
    }))
    .await?;
    test_segment_locations.push(segment_location);
    test_block_locations.push(block_location);

    let (segment_location, block_location) = gen_test_seg(Some(ClusterStatistics {
        cluster_key_id: 0,
        min: vec![DataValue::Int64(2)],
        max: vec![DataValue::Int64(4)],
        level: 0,
    }))
    .await?;
    test_segment_locations.push(segment_location);
    test_block_locations.push(block_location);

    let (segment_location, block_location) = gen_test_seg(Some(ClusterStatistics {
        cluster_key_id: 0,
        min: vec![DataValue::Int64(4)],
        max: vec![DataValue::Int64(5)],
        level: 0,
    }))
    .await?;
    test_segment_locations.push(segment_location);
    test_block_locations.push(block_location);

    let base_snapshot = TableSnapshot::new(
        Uuid::new_v4(),
        &None,
        None,
        DataSchema::empty(),
        Statistics::default(),
        test_segment_locations.clone(),
        Some((0, "(id)".to_string())),
    );

    let mut mutator = ReclusterMutator::try_create(
        ctx,
        location_generator,
        Arc::new(base_snapshot),
        1.0,
        BlockCompactor::default(),
    )?;

    let need_recluster = mutator.blocks_select().await?;
    assert!(need_recluster);
    assert_eq!(mutator.selected_blocks().len(), 3);

    Ok(())
}
