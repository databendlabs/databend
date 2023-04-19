// Copyright 2022 Datafuse Labs.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
//

use std::sync::Arc;

use chrono::Duration;
use chrono::Utc;
use common_base::base::tokio;
use common_catalog::table_context::TableContext;
use common_exception::Result;
use common_expression::DataBlock;
use common_storages_factory::Table;
use common_storages_fuse::io::MetaWriter;
use common_storages_fuse::io::SegmentWriter;
use common_storages_fuse::statistics::gen_columns_statistics;
use common_storages_fuse::statistics::merge_statistics;
use common_storages_fuse::FuseTable;
use futures_util::TryStreamExt;
use storages_common_table_meta::meta::Location;
use storages_common_table_meta::meta::SegmentInfo;
use storages_common_table_meta::meta::Statistics;
use storages_common_table_meta::meta::TableSnapshot;
use storages_common_table_meta::meta::TableSnapshotV2;
use storages_common_table_meta::meta::Versioned;
use uuid::Uuid;

use crate::storages::fuse::block_writer::BlockWriter;
use crate::storages::fuse::table_test_fixture::append_sample_data;
use crate::storages::fuse::table_test_fixture::check_data_dir;
use crate::storages::fuse::table_test_fixture::TestFixture;

#[tokio::test(flavor = "multi_thread")]
async fn test_fuse_purge_normal_case() -> Result<()> {
    let fixture = TestFixture::new().await;
    let ctx = fixture.ctx();
    fixture.create_default_table().await?;

    // ingests some test data
    append_sample_data(1, &fixture).await?;

    // do_gc
    let table = fixture.latest_default_table().await?;
    let fuse_table = FuseTable::try_from_table(table.as_ref())?;
    let keep_last_snapshot = true;
    let table_ctx: Arc<dyn TableContext> = ctx.clone();
    fuse_table.do_purge(&table_ctx, keep_last_snapshot).await?;

    let expected_num_of_snapshot = 1;
    check_data_dir(
        &fixture,
        "do_gc: there should be 1 snapshot, 0 segment/block",
        expected_num_of_snapshot,
        0, // 0 snapshot statistic
        1, // 1 segments
        1, // 1 blocks
        1, // 1 index
        Some(()),
        None,
    )
    .await?;
    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn test_fuse_purge_normal_orphan_snapshot() -> Result<()> {
    let fixture = TestFixture::new().await;
    let ctx = fixture.ctx();
    fixture.create_default_table().await?;

    // ingests some test data
    append_sample_data(1, &fixture).await?;

    let table = fixture.latest_default_table().await?;
    let fuse_table = FuseTable::try_from_table(table.as_ref())?;

    // create orphan snapshot, its timestamp is larger than the current one
    {
        let current_snapshot = fuse_table.read_table_snapshot().await?.unwrap();
        let operator = fuse_table.get_operator();
        let location_gen = fuse_table.meta_location_generator();
        let orphan_snapshot_id = Uuid::new_v4();
        let orphan_snapshot_location = location_gen
            .snapshot_location_from_uuid(&orphan_snapshot_id, TableSnapshot::VERSION)?;
        // orphan_snapshot is created by using `from_previous`, which guarantees
        // that the timestamp of snapshot returned is larger than `current_snapshot`'s.
        let orphan_snapshot = TableSnapshot::from_previous(current_snapshot.as_ref());
        orphan_snapshot
            .write_meta(&operator, &orphan_snapshot_location)
            .await?;
    }

    // do_gc
    let keep_last_snapshot = true;
    let table_ctx: Arc<dyn TableContext> = ctx.clone();
    fuse_table.do_purge(&table_ctx, keep_last_snapshot).await?;

    // expects two snapshot there
    // - one snapshot of the latest version
    // - one orphan snapshot which timestamp is larger then the latest snapshot's
    let expected_num_of_snapshot = 2;
    check_data_dir(
        &fixture,
        "do_gc: there should be 1 snapshot, 0 segment/block",
        expected_num_of_snapshot,
        0, // 0 snapshot statistic
        1, // 0 segments
        1, // 0 blocks
        1, // 0 index
        Some(()),
        None,
    )
    .await?;
    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn test_fuse_purge_orphan_retention() -> Result<()> {
    // verifies that:
    //
    // - snapshots that beyond retention period shall be collected, but
    // - if segments are referenced by snapshot within retention period,
    //   they shall not be collected during purge.
    //   the blocks referenced by those segments, shall not be collected as well.
    //
    // for example :
    //
    //   ──┬──           S_current───────────► seg_c ──────────────► block_c
    //     │
    //  within retention
    //     │
    //     │             S_2 ───────────┐
    //   ──┴──                          └───────┐
    //  beyond retention                        ▼
    //     │             S_1 ────────────────► seg_1 ──────────────► block_1
    //     │
    //     │             S_0 ────────────────► seg_0 ──────────────► block_0
    //
    // - S_current is the gc root
    // - S_2, S_1, S_0 are all orphan snapshots in S_current's point of view
    //   each of them is not a number of  S_current's precedents
    //
    // - s_current, seg_c, and block_c shall NOT be purged
    //   since they are referenced by the current table snapshot
    // - S_2 should NOT be purged
    //   since it is within the retention period
    // - S_1 should be purged, since it is beyond the retention period
    //    BUT
    //    - seg_1 shall NOT be purged, since it is still referenced by s_1
    //      although it is not referenced by the current snapshot.
    //    - block_1 shall NOT be purged , since it is referenced by seg_1
    // - S_0 should be purged, since it is beyond the retention period
    //    - seg_0 and block_0 shall be purged
    //
    //  put them together, after GC, there will be
    //  - 2 snapshots left: s_current, s_2
    //  - 2 segments left: seg_c, seg_1
    //  - 2 blocks left: block_c, block_1

    let fixture = TestFixture::new().await;
    let ctx = fixture.ctx();
    fixture.create_default_table().await?;

    // 1. prepare `S_current`
    let number_of_block = 1;
    append_sample_data(number_of_block, &fixture).await?;
    // no we have 1 snapshot, 1 segment, 1 blocks

    let table = fixture.latest_default_table().await?;
    let fuse_table = FuseTable::try_from_table(table.as_ref())?;
    let base_snapshot = fuse_table.read_table_snapshot().await?.unwrap();
    let base_timestamp = base_snapshot.timestamp.unwrap();

    // 2. prepare `seg_1`
    let num_of_segments = 1;
    let blocks_per_segment = 1;
    let segments =
        utils::generate_segments(fuse_table, num_of_segments, blocks_per_segment).await?;
    let (segment_locations, _segment_info): (Vec<_>, Vec<_>) = segments.into_iter().unzip();

    // 2. prepare S_2
    let new_timestamp = base_timestamp - Duration::minutes(1);
    let _snapshot_location = utils::generate_snapshot_with_segments(
        fuse_table,
        segment_locations.clone(),
        Some(new_timestamp),
        TableSnapshot::VERSION,
    )
    .await?;

    // 2. prepare S_1
    let new_timestamp = base_timestamp - Duration::days(2);
    let _snapshot_location = utils::generate_snapshot_with_segments(
        fuse_table,
        segment_locations.clone(),
        Some(new_timestamp),
        TableSnapshot::VERSION,
    )
    .await?;

    // 2. prepare S_0
    {
        let num_of_segments = 1;
        let blocks_per_segment = 1;
        let segments =
            utils::generate_segments(fuse_table, num_of_segments, blocks_per_segment).await?;
        let segment_locations: Vec<Location> = segments.into_iter().map(|(l, _)| l).collect();
        let new_timestamp = base_timestamp - Duration::days(2);
        let _snapshot_location = utils::generate_snapshot_with_segments(
            fuse_table,
            segment_locations.clone(),
            Some(new_timestamp),
            TableSnapshot::VERSION,
        )
        .await?;
    }

    // do_gc
    let keep_last_snapshot = true;
    let table_ctx: Arc<dyn TableContext> = ctx.clone();
    fuse_table.do_purge(&table_ctx, keep_last_snapshot).await?;

    let expected_num_of_snapshot = 2;
    let expected_num_of_segment = 2;
    let expected_num_of_blocks = 2;
    let expected_num_of_index = expected_num_of_blocks;
    check_data_dir(
        &fixture,
        "do_gc: verify retention period",
        expected_num_of_snapshot,
        0,
        expected_num_of_segment,
        expected_num_of_blocks,
        expected_num_of_index,
        Some(()),
        None,
    )
    .await?;
    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn test_fuse_purge_older_version() -> Result<()> {
    let fixture = TestFixture::new().await;
    let ctx = fixture.ctx();
    fixture.create_default_table().await?;

    let now = Utc::now();

    let schema = TestFixture::default_table_schema();
    let mut table = fixture.latest_default_table().await?;
    let mut fuse_table = FuseTable::try_from_table(table.as_ref())?;
    let location_gen = fuse_table.meta_location_generator();
    let operator = fuse_table.get_operator();

    {
        let num_of_segments = 3;
        let blocks_per_segment = 2;
        let segments =
            utils::generate_segments(fuse_table, num_of_segments, blocks_per_segment).await?;

        // create snapshot 0.
        let locations = vec![segments[0].0.clone()];
        let id = Uuid::new_v4();
        let mut snapshot_0 = TableSnapshotV2::new(
            id,
            &None,
            None,
            schema.as_ref().clone(),
            segments[0].1.summary.clone(),
            locations,
            None,
            None,
        );
        snapshot_0.timestamp = Some(now - Duration::hours(13));

        let new_snapshot_location = location_gen
            .snapshot_location_from_uuid(&snapshot_0.snapshot_id, TableSnapshotV2::VERSION)?;
        utils::write_snapshot_v2(&operator, &new_snapshot_location, &snapshot_0).await?;

        // create snapshot 1.
        let mut locations = Vec::with_capacity(2);
        for i in [1, 0] {
            locations.push(segments[i].0.clone());
        }
        let mut snapshot_1 = TableSnapshot::new(
            Uuid::new_v4(),
            &snapshot_0.timestamp,
            Some((snapshot_0.snapshot_id, TableSnapshotV2::VERSION)),
            schema.as_ref().clone(),
            Statistics::default(),
            locations,
            None,
            None,
        );
        snapshot_1.timestamp = Some(now - Duration::hours(12));
        snapshot_1.summary = merge_statistics(&snapshot_0.summary, &segments[1].1.summary)?;
        let new_snapshot_location = location_gen
            .snapshot_location_from_uuid(&snapshot_1.snapshot_id, TableSnapshot::VERSION)?;
        snapshot_1
            .write_meta(&operator, &new_snapshot_location)
            .await?;

        // create snapshot 2.
        let mut locations = Vec::with_capacity(3);
        for i in [2, 1, 0] {
            locations.push(segments[i].0.clone());
        }
        let mut snapshot_2 = TableSnapshot::from_previous(&snapshot_1);
        snapshot_2.segments = locations;
        snapshot_2.timestamp = Some(now);
        snapshot_2.summary = merge_statistics(&snapshot_1.summary, &segments[2].1.summary)?;
        let new_snapshot_location = location_gen
            .snapshot_location_from_uuid(&snapshot_2.snapshot_id, TableSnapshot::VERSION)?;
        snapshot_2
            .write_meta(&operator, &new_snapshot_location)
            .await?;
        FuseTable::commit_to_meta_server(
            ctx.as_ref(),
            fuse_table.get_table_info(),
            location_gen,
            snapshot_2,
            None,
            &None,
            &operator,
        )
        .await?;
    }

    let table_ctx: Arc<dyn TableContext> = ctx.clone();
    table = fixture.latest_default_table().await?;
    fuse_table = FuseTable::try_from_table(table.as_ref())?;
    // retention period is 0, the base snapshot is root snapshot.
    fuse_table.do_purge(&table_ctx, true).await?;

    let expected_num_of_snapshot = 1;
    let expected_num_of_segment = 3;
    let expected_num_of_blocks = 6;
    let expected_num_of_index = expected_num_of_blocks;
    check_data_dir(
        &fixture,
        "do_gc: retention period is 0",
        expected_num_of_snapshot,
        0,
        expected_num_of_segment,
        expected_num_of_blocks,
        expected_num_of_index,
        Some(()),
        None,
    )
    .await?;
    Ok(())
}

mod utils {
    use std::io::Error;
    use std::sync::Arc;

    use chrono::DateTime;
    use chrono::Utc;
    use common_storages_factory::Table;
    use common_storages_fuse::io::MetaWriter;
    use common_storages_fuse::FuseStorageFormat;
    use opendal::Operator;

    use super::*;

    pub async fn generate_snapshot_with_segments(
        fuse_table: &FuseTable,
        segment_locations: Vec<Location>,
        time_stamp: Option<DateTime<Utc>>,
        version: u64,
    ) -> Result<String> {
        let current_snapshot = fuse_table.read_table_snapshot().await?.unwrap();
        let operator = fuse_table.get_operator();
        let location_gen = fuse_table.meta_location_generator();
        let mut new_snapshot = TableSnapshot::from_previous(current_snapshot.as_ref());
        new_snapshot.segments = segment_locations;
        let new_snapshot_location =
            location_gen.snapshot_location_from_uuid(&new_snapshot.snapshot_id, version)?;
        if let Some(ts) = time_stamp {
            new_snapshot.timestamp = Some(ts)
        }

        new_snapshot
            .write_meta(&operator, &new_snapshot_location)
            .await?;

        Ok(new_snapshot_location)
    }

    pub async fn generate_segments(
        fuse_table: &FuseTable,
        number_of_segments: usize,
        blocks_per_segment: usize,
    ) -> Result<Vec<(Location, SegmentInfo)>> {
        let mut segs = vec![];
        for _ in 0..number_of_segments {
            let (location, segment) = generate_segment(fuse_table, blocks_per_segment).await?;
            segs.push((location, segment))
        }
        Ok(segs)
    }

    async fn generate_segment(
        fuse_table: &FuseTable,
        num_blocks: usize,
    ) -> Result<(Location, SegmentInfo)> {
        let dal = fuse_table.get_operator_ref();
        let schema = fuse_table.schema();
        let block_writer = BlockWriter::new(dal, fuse_table.meta_location_generator());
        let mut block_metas = vec![];

        // does not matter in this suite
        let rows_per_block = 1;
        let value_start_from = 1;

        let stream =
            TestFixture::gen_sample_blocks_stream_ex(num_blocks, rows_per_block, value_start_from);

        let blocks: std::vec::Vec<DataBlock> = stream.try_collect().await?;
        for block in blocks {
            let stats = gen_columns_statistics(&block, None, &schema)?;
            let (block_meta, _index_meta) = block_writer
                .write(FuseStorageFormat::Parquet, &schema, block, stats, None)
                .await?;
            block_metas.push(Arc::new(block_meta));
        }

        let segment_info = SegmentInfo::new(block_metas, Statistics::default());
        let segment_writer = SegmentWriter::new(dal, fuse_table.meta_location_generator());
        let segment_location = segment_writer.write_segment_no_cache(&segment_info).await?;
        Ok((segment_location, segment_info))
    }

    pub async fn write_snapshot_v2(data_accessor: &Operator, location: &str, meta: &TableSnapshotV2) -> Result<()> {
        let bs = serde_json::to_vec(&meta).map_err(Error::other)?;
        data_accessor.write(location, bs).await?;
        Ok(())
    }
}
