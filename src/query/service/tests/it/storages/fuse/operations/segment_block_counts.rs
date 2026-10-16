// Copyright 2021 Datafuse Labs.
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

use std::collections::HashSet;

use databend_common_catalog::table::Table;
use databend_common_storages_fuse::FuseSegmentFormat;
use databend_common_storages_fuse::FuseTable;
use databend_common_storages_fuse::io::SegmentsIO;
use databend_common_storages_fuse::io::read::read_column_oriented_segment;
use databend_common_storages_fuse::segment_format_from_location;
use databend_query::sessions::TableContextTableAccess;
use databend_query::test_kits::TestFixture;
use databend_storages_common_cache::CacheAccessor;
use databend_storages_common_cache::CacheManager;
use databend_storages_common_table_meta::meta::column_oriented_segment::ROW_COUNT;

pub(super) async fn assert_segment_block_counts(table: &FuseTable) -> anyhow::Result<()> {
    let snapshot = table.read_table_snapshot().await?.unwrap();
    assert_eq!(snapshot.segment_block_counts.len(), snapshot.segments.len());
    let mut total = 0;
    for (location, count) in snapshot.segments.iter().zip(&snapshot.segment_block_counts) {
        // Check actual block metadata, not the summary used by the writer.
        let actual = match segment_format_from_location(&location.0) {
            FuseSegmentFormat::Row => SegmentsIO::read_compact_segment(
                table.get_operator(),
                location.clone(),
                table.schema(),
                false,
            )
            .await?
            .block_metas()?
            .len(),
            FuseSegmentFormat::Column => read_column_oriented_segment(
                table.get_operator(),
                &location.0,
                &HashSet::from([ROW_COUNT.to_string()]),
                false,
            )
            .await?
            .block_metas
            .num_rows(),
        } as u64;
        assert_eq!(*count, Some(actual), "segment {}", location.0);
        total += actual;
    }
    assert_eq!(snapshot.summary.block_count, total);
    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn test_segment_block_counts_across_writes() -> anyhow::Result<()> {
    let fixture = TestFixture::setup().await?;
    fixture.create_default_database().await?;
    let db = fixture.default_db_name();
    let name = "segment_counts";
    let target = format!("{db}.{name}");
    fixture
        .execute_command("SET auto_compaction_imperfect_blocks_threshold=0")
        .await?;
    fixture.execute_command("SET enable_auto_analyze=0").await?;
    fixture
        .execute_command(&format!(
            "CREATE TABLE {target}(id INT, v INT) block_per_segment=10"
        ))
        .await?;
    for statement in [
        format!("INSERT INTO {target} VALUES (1,10),(2,20),(3,30)"),
        format!("INSERT INTO {target} VALUES (4,40),(5,50)"),
        format!("OPTIMIZE TABLE {target} COMPACT SEGMENT"),
        format!("INSERT INTO {target} VALUES (6,60)"),
        format!("OPTIMIZE TABLE {target} COMPACT"),
        format!("UPDATE {target} SET v=v+1 WHERE id=1"),
        format!("DELETE FROM {target} WHERE id=2"),
        format!(
            "MERGE INTO {target} AS t USING (SELECT 3 AS id, 300 AS v) AS s \
             ON t.id=s.id WHEN MATCHED THEN UPDATE SET v=s.v \
             WHEN NOT MATCHED THEN INSERT (id,v) VALUES (s.id,s.v)"
        ),
        format!("REPLACE INTO {target} ON (id) VALUES (3,301),(7,70)"),
        format!("ANALYZE TABLE {target}"),
        format!("INSERT OVERWRITE {target} VALUES (8,80),(9,90)"),
        format!("TRUNCATE TABLE {target}"),
        format!("INSERT INTO {target} VALUES (10,100)"),
    ] {
        fixture.execute_command(&statement).await?;
        let ctx = fixture.new_query_ctx().await?;
        let table = ctx.get_table("default", &db, name).await?;
        assert_segment_block_counts(FuseTable::try_from_table(table.as_ref())?).await?;
    }
    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn test_segment_block_counts_upgrade_without_backfill() -> anyhow::Result<()> {
    let fixture = TestFixture::setup().await?;
    fixture.create_default_database().await?;
    fixture.create_default_table().await?;
    let target = format!(
        "{}.{}",
        fixture.default_db_name(),
        fixture.default_table_name()
    );
    fixture
        .execute_command("SET auto_compaction_imperfect_blocks_threshold=0")
        .await?;
    fixture.execute_command("SET enable_auto_analyze=0").await?;
    fixture
        .execute_command(&format!("INSERT INTO {target}(id) VALUES (1)"))
        .await?;
    let table = fixture.latest_default_table().await?;
    let table = FuseTable::try_from_table(table.as_ref())?;
    let location = table.snapshot_loc().unwrap();
    let snapshot = table.read_table_snapshot().await?.unwrap();
    let mut legacy = snapshot.as_ref().clone();
    legacy.segment_block_counts.clear();
    table
        .get_operator()
        .write(&location, legacy.to_bytes()?)
        .await?;
    if let Some(cache) = CacheManager::instance().get_table_snapshot_cache() {
        cache.evict(&location);
    }

    fixture
        .execute_command(&format!("INSERT INTO {target}(id) VALUES (2)"))
        .await?;
    let table = fixture.latest_default_table().await?;
    let table = FuseTable::try_from_table(table.as_ref())?;
    let snapshot = table.read_table_snapshot().await?.unwrap();
    assert_eq!(snapshot.segment_block_counts, vec![Some(1), None]);
    assert_eq!(snapshot.segments[1], legacy.segments[0]);

    fixture
        .execute_command(&format!("ANALYZE TABLE {target}"))
        .await?;
    let table = fixture.latest_default_table().await?;
    let table = FuseTable::try_from_table(table.as_ref())?;
    let analyzed = table.read_table_snapshot().await?.unwrap();
    assert_eq!(analyzed.segment_block_counts, vec![Some(1), None]);
    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn test_segment_block_counts_metadata_rewrites() -> anyhow::Result<()> {
    let fixture = TestFixture::setup().await?;
    fixture.create_default_database().await?;
    let db = fixture.default_db_name();
    let name = "segment_counts_rewrite";
    let target = format!("{db}.{name}");
    fixture
        .execute_command(&format!("CREATE TABLE {target}(d DECIMAL(10,2))"))
        .await?;
    for statement in [
        format!("INSERT INTO {target} VALUES (1.23),(4.56)"),
        format!("INSERT INTO {target} VALUES (7.89)"),
        format!("ALTER TABLE {target} MODIFY COLUMN d DECIMAL(15,2)"),
        format!("ALTER TABLE {target} SET OPTIONS (segment_format='column_oriented')"),
    ] {
        fixture.execute_command(&statement).await?;
        let ctx = fixture.new_query_ctx().await?;
        let table = ctx.get_table("default", &db, name).await?;
        assert_segment_block_counts(FuseTable::try_from_table(table.as_ref())?).await?;
    }
    Ok(())
}
