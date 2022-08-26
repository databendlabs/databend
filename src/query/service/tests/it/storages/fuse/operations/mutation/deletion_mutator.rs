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

use std::collections::hash_map::RandomState;
use std::collections::HashMap;
use std::collections::HashSet;
use std::sync::Arc;

use common_base::base::tokio;
use common_datablocks::DataBlock;
use common_datavalues::DataSchema;
use common_exception::ErrorCode;
use common_exception::Result;
use common_fuse_meta::caches::CacheManager;
use common_fuse_meta::meta::BlockMeta;
use common_fuse_meta::meta::SegmentInfo;
use common_fuse_meta::meta::Statistics;
use common_fuse_meta::meta::TableSnapshot;
use common_fuse_meta::meta::Versioned;
use databend_query::sessions::TableContext;
use databend_query::storages::fuse::io::SegmentWriter;
use databend_query::storages::fuse::io::TableMetaLocationGenerator;
use databend_query::storages::fuse::operations::DeletionMutator;
use databend_query::storages::fuse::statistics::ClusterStatsGenerator;
use uuid::Uuid;

use crate::storages::fuse::table_test_fixture::TestFixture;

/// [issue#6570](https://github.com/datafuselabs/databend/issues/6570)
/// During deletion, there might be multiple segments become empty

#[tokio::test]
async fn test_deletion_mutator_multiple_empty_segments() -> Result<()> {
    // generates a batch of segments, and delete blocks from them
    // so that half of the segments will be empty

    let fixture = TestFixture::new().await;
    let ctx = fixture.ctx();
    let location_generator = TableMetaLocationGenerator::with_prefix("_prefix".to_owned());

    let segment_info_cache = CacheManager::instance().get_table_segment_cache();
    let data_accessor = ctx.get_storage_operator()?;
    let seg_writer = SegmentWriter::new(&data_accessor, &location_generator, &segment_info_cache);

    let gen_test_seg = || async {
        // generates test segment, each of them contains only one block
        // structures are filled with arbitrary values, no effects for this test case
        let block_id = Uuid::new_v4().simple().to_string();
        let location = (block_id, DataBlock::VERSION);
        let test_block_meta = BlockMeta::new(
            1,
            1,
            1,
            HashMap::default(),
            HashMap::default(),
            None,
            location.clone(),
            None,
            0,
        );
        let segment = SegmentInfo::new(vec![test_block_meta], Statistics::default());
        Ok::<_, ErrorCode>((seg_writer.write_segment(segment).await?, location))
    };

    // generates 100 segments, for each segment, contains one block
    let mut test_segment_locations = vec![];
    let mut test_block_locations = vec![];
    for _ in 0..100 {
        let (segment_location, block_location) = gen_test_seg().await?;
        test_segment_locations.push(segment_location);
        test_block_locations.push(block_location);
    }

    let base_snapshot = TableSnapshot::new(
        Uuid::new_v4(),
        &None,
        None,
        DataSchema::empty(),
        Statistics::default(),
        test_segment_locations.clone(),
        None,
    );

    let table_ctx: Arc<dyn TableContext> = ctx as Arc<dyn TableContext>;
    // we do not care about chunk size in this test case, choose it arbitrarily
    let chunk_size = 10;
    let mut mutator = DeletionMutator::try_create(
        table_ctx,
        location_generator,
        Arc::new(base_snapshot),
        ClusterStatsGenerator::default(),
        chunk_size,
    )?;

    // clear half of the segments
    for (i, _) in test_segment_locations.iter().enumerate().take(100) {
        if i % 2 == 0 {
            // empty the segment (segment only contains one block)
            mutator
                .replace_with(i, test_block_locations[i].clone(), None, DataBlock::empty())
                .await?;
        }
    }

    let (new_snapshot, _) = mutator.into_new_snapshot().await?;

    // half segments left after deletion
    assert_eq!(new_snapshot.segments.len(), 50);

    // new_segments should be a subset of test_segments in our case (no partial deletion of segment)
    let new_segments = HashSet::<_, RandomState>::from_iter(new_snapshot.segments.into_iter());
    let test_segments = HashSet::from_iter(test_segment_locations.into_iter());
    assert!(new_segments.is_subset(&test_segments));

    Ok(())
}
