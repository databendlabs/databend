// Copyright 2023 Databend Cloud
//
// Licensed under the Elastic License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     https://www.elastic.co/licensing/elastic-license
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

//! The disk cache under the table operations that are not queries. They may read from the disk
//! cache but do not fill it, as it serves queries; and an object they rewrite in place or
//! delete is never read back from it as it was.
//!
//! Each test runs a node with its own disk cache and the memory tiers of the meta and index
//! caches off, so that every read of those goes through the disk tier. The node has no other
//! users, so the disk cache counts are exact.

use std::collections::BTreeMap;
use std::collections::BTreeSet;
use std::sync::Arc;
use std::time::Duration;
use std::time::Instant;

use databend_common_catalog::table::Table;
use databend_common_config::CacheStorageTypeInnerConfig;
use databend_common_exception::Result;
use databend_common_expression::DataBlock;
use databend_common_expression::block_debug::pretty_format_blocks;
use databend_common_storages_fuse::FuseTable;
use databend_common_storages_fuse::io::SegmentsIO;
use databend_common_storages_fuse::io::TableMetaLocationGenerator;
use databend_enterprise_query::test_kits::context::EESetup;
use databend_query::sessions::TableContextTableAccess;
use databend_query::test_kits::TestFixture;
use databend_storages_common_cache::CacheFill;
use databend_storages_common_cache::CacheKind;
use databend_storages_common_cache::CacheManager;
use databend_storages_common_cache::CachePolicy;
use databend_storages_common_cache::DiskCache;
use databend_storages_common_cache::DiskCachedStorage;
use databend_storages_common_cache::DiskKey;
use databend_storages_common_cache::SegmentInfo;
use databend_storages_common_cache::kinds::bloom;
use databend_storages_common_cache::kinds::column;
use databend_storages_common_cache::kinds::segment;
use databend_storages_common_cache::kinds::table;
use futures::TryStreamExt;
use opendal::Operator;
use tempfile::TempDir;

const DB: &str = "disk_cache_db";

/// A node with the disk cache on under `dir`, and the memory tiers of the meta and index
/// caches off.
async fn setup(dir: &TempDir) -> Result<TestFixture> {
    let mut setup = EESetup::new();
    let cache = &mut setup.config_mut().cache;
    cache.data_cache_storage = CacheStorageTypeInnerConfig::Disk;
    cache.disk_cache_config.path = dir.path().to_str().unwrap().to_string();
    cache.disk_cache_config.max_bytes = 1 << 30;
    cache.table_meta_snapshot_count = 0;
    cache.table_meta_statistic_count = 0;
    cache.table_meta_segment_bytes = 0;
    cache.segment_statistics_bytes = 0;
    cache.table_bloom_index_meta_count = 0;
    cache.table_bloom_index_filter_size = 0;
    cache.table_prune_partitions_count = 0;
    cache.inverted_index_meta_count = 0;
    cache.inverted_index_lookup_size = 0;
    cache.inverted_index_payload_size = 0;
    cache.virtual_column_meta_count = 0;
    let fixture = TestFixture::setup_with_custom(setup).await?;
    fixture
        .default_session()
        .get_settings()
        .set_data_retention_time_in_days(0)?;
    fixture
        .execute_command(&format!("create database {DB}"))
        .await?;
    Ok(fixture)
}

fn disk() -> Arc<DiskCache> {
    CacheManager::instance()
        .disk()
        .expect("the disk cache is on in EE mode")
}

/// Waits until the entries queued so far are written or dropped.
async fn settle() {
    let disk = disk();
    let deadline = Instant::now() + Duration::from_secs(30);
    while disk.stats().queued_bytes > 0
        || disk.pool_stats().iter().any(|(_, stats)| stats.writing > 0)
    {
        assert!(Instant::now() < deadline, "the disk cache writer is stuck");
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
}

/// Entries on disk, by the name of their kind's row in `system.caches`.
fn disk_items() -> BTreeMap<String, u64> {
    CacheManager::instance()
        .rows()
        .into_iter()
        .filter(|row| row.disk && row.kind.is_some())
        .map(|row| (row.name, row.num_items))
        .collect()
}

/// Lookups of this test's disk cache so far, hits and misses, by kind. From the cache's own
/// counters: the `system.caches` rows read metric families shared by every disk cache of the
/// process, those of the tests running alongside included.
fn disk_accesses() -> BTreeMap<String, u64> {
    let disk = CacheManager::instance().disk().expect("a disk cache");
    CacheKind::all()
        .map(|kind| {
            let stats = disk.kind_stats(kind);
            (kind.name().to_string(), stats.hits + stats.misses)
        })
        .filter(|(_, n)| *n > 0)
        .collect()
}

async fn query(fixture: &TestFixture, sql: &str) -> Result<String> {
    let blocks: Vec<DataBlock> = fixture.execute_query(sql).await?.try_collect().await?;
    pretty_format_blocks(&blocks)
}

async fn fuse_table(fixture: &TestFixture, name: &str) -> Result<Arc<dyn Table>> {
    let ctx = fixture.new_query_ctx().await?;
    ctx.get_table("default", DB, name).await
}

/// The rows every table of these tests holds, three inserts of a thousand: `a` overlaps across
/// the inserts, so the blocks can be reclustered.
async fn create_table(fixture: &TestFixture, name: &str, options: &str) -> Result<()> {
    fixture
        .execute_command(&format!(
            "create table {DB}.{name}(a int, s string, v variant) {options}"
        ))
        .await?;
    for i in 0..3 {
        fixture
            .execute_command(&format!(
                "insert into {DB}.{name} select number, concat('k', (number + {i} * 1000)::string), \
                 parse_json(concat('{{\"a\":', number::string, '}}')) from numbers(1000)"
            ))
            .await?;
    }
    Ok(())
}

async fn check_rows(fixture: &TestFixture, name: &str) -> Result<()> {
    let rows = query(
        fixture,
        &format!(
            "select concat_ws(',', count(*), sum(a), max(s), sum(v['a']::int)) from {DB}.{name}"
        ),
    )
    .await?;
    assert!(
        rows.contains("3000,1498500,k999,1498500"),
        "{name}:\n{rows}"
    );
    Ok(())
}

/// Each maintenance operation, on a table no query read but for its current snapshot, which
/// every statement on the table reads: it bypasses the disk cache, neither looking it up, the
/// cached snapshot included, nor adding an entry. Its results are checked by a query
/// afterwards.
#[tokio::test(flavor = "multi_thread")]
async fn test_maintenance_does_not_fill_the_disk_cache() -> anyhow::Result<()> {
    let dir = TempDir::new()?;
    let fixture = setup(&dir).await?;
    let options = "cluster by (a) enable_virtual_column = true";
    let operations: &[(&str, &[&str], &[&str])] = &[
        ("analyze", &[], &["analyze table {t}"]),
        ("compact segment", &[], &[
            "optimize table {t} compact segment",
        ]),
        ("compact", &[], &["optimize table {t} compact"]),
        ("recluster", &[], &["alter table {t} recluster final"]),
        ("refresh virtual column", &[], &[
            "refresh virtual column for {t}",
        ]),
        (
            "refresh ngram index",
            &["create ngram index idx_{n} on {t}(s)"],
            &["refresh ngram index idx_{n} on {t}"],
        ),
        (
            "refresh inverted index",
            &["create inverted index idx_{n} on {t}(s)"],
            &["refresh inverted index idx_{n} on {t}"],
        ),
        ("vacuum", &["optimize table {t} compact"], &[
            "vacuum table {t}",
        ]),
        ("purge", &["optimize table {t} compact"], &[
            "optimize table {t} purge",
        ]),
        // A query statement: the table function marks its own reads.
        ("fuse_vacuum2", &["optimize table {t} compact"], &[
            "select * from fuse_vacuum2('{db}', '{n}')",
        ]),
    ];

    let mut fills = Vec::new();
    for (i, (operation, prepare, run)) in operations.iter().enumerate() {
        let name = format!("t{i}");
        let sql = |s: &str| {
            s.replace("{t}", &format!("{DB}.{name}"))
                .replace("{db}", DB)
                .replace("{n}", &name)
        };
        create_table(&fixture, &name, options).await?;
        for statement in prepare.iter() {
            fixture.execute_command(&sql(statement)).await?;
        }
        let table = fuse_table(&fixture, &name).await?;
        let table = FuseTable::try_from_table(table.as_ref())?;
        let snapshot = table.snapshot_loc().expect("a snapshot");
        fill(
            &table.get_operator(),
            CacheKind::of::<table::Snapshot>(),
            &snapshot,
            None,
        )
        .await?;
        let before = disk_items();
        let accesses = disk_accesses();
        for statement in run.iter() {
            fixture.execute_command(&sql(statement)).await?;
        }
        settle().await;
        for (kind, n) in disk_accesses() {
            let was = accesses.get(&kind).copied().unwrap_or(0);
            if n > was {
                fills.push(format!(
                    "{operation}: {} {kind} disk cache lookups",
                    n - was
                ));
            }
        }
        for (kind, items) in disk_items() {
            let was = before.get(&kind).copied().unwrap_or(0);
            if items > was {
                fills.push(format!("{operation}: {kind} {was} -> {items}"));
            }
        }
        check_rows(&fixture, &name).await?;
    }
    assert!(
        fills.is_empty(),
        "disk cache used by maintenance:\n{}",
        fills.join("\n")
    );
    Ok(())
}

/// The bytes of `path` through the disk cache, without filling it, against storage.
async fn assert_reads_as_stored(
    op: &Operator,
    kind: CacheKind,
    path: &str,
    range: Option<std::ops::Range<u64>>,
) -> Result<()> {
    let cached = DiskCachedStorage::new(op.clone())
        .read(kind, path, range.clone(), CacheFill::ReadOnly)
        .await?;
    let stored = match range {
        Some(range) => op.read_with(path).range(range).await?,
        None => op.read(path).await?,
    }
    .to_bytes();
    assert!(
        cached == stored,
        "{path} is read from the disk cache as it was"
    );
    Ok(())
}

/// Reads `path` as a query would, filling the disk cache, and checks it is there.
async fn fill(
    op: &Operator,
    kind: CacheKind,
    path: &str,
    range: Option<std::ops::Range<u64>>,
) -> Result<()> {
    let key = match &range {
        Some(range) => DiskKey::range(path, range.clone()),
        None => DiskKey::whole(path),
    };
    DiskCachedStorage::new(op.clone())
        .read(kind, path, range, CacheFill::ReadWrite)
        .await?;
    settle().await;
    assert!(disk().contains(kind, &key), "{path} is not on disk");
    Ok(())
}

/// The segments of the current snapshot, and their statistics files.
async fn segments_and_stats(
    fixture: &TestFixture,
    name: &str,
) -> Result<(Vec<String>, Vec<String>)> {
    let table = fuse_table(fixture, name).await?;
    let table = FuseTable::try_from_table(table.as_ref())?;
    let snapshot = table.read_table_snapshot().await?.expect("a snapshot");
    let segments: Vec<String> = snapshot.segments.iter().map(|(p, _)| p.clone()).collect();
    let op = table.get_operator();
    let mut stats = Vec::new();
    for segment in &segments {
        let path =
            TableMetaLocationGenerator::gen_segment_stats_location_from_segment_location(segment);
        if op.exists(&path).await? {
            stats.push(path);
        }
    }
    Ok((segments, stats))
}

/// ANALYZE rewrites segments in place, with their statistics, when it scans blocks for
/// statistics they lack. Read before by a query, the segments are read as rewritten after.
/// Segment statistics have no disk tier: only writes and maintenance read them.
#[tokio::test(flavor = "multi_thread")]
async fn test_analyze_rewrites_are_not_read_back_as_they_were() -> anyhow::Result<()> {
    let dir = TempDir::new()?;
    let fixture = setup(&dir).await?;
    create_table(&fixture, "t", "").await?;
    // The statistics files exist before the second ANALYZE, which rebuilds them for Top-N.
    fixture
        .execute_command(&format!("analyze table {DB}.t"))
        .await?;
    fixture
        .execute_command(&format!(
            "alter table {DB}.t set options(analyze_frequency_columns = 'a', analyze_top_n_size = 2)"
        ))
        .await?;
    let table = fuse_table(&fixture, "t").await?;
    let op = FuseTable::try_from_table(table.as_ref())?.get_operator();
    let (segments, stats) = segments_and_stats(&fixture, "t").await?;
    assert!(!stats.is_empty(), "ANALYZE wrote no segment statistics");
    assert!(!CacheKind::of::<segment::Stats>().on_disk());
    let objects: Vec<_> = segments
        .iter()
        .map(|p| (CacheKind::of::<segment::Info>(), p.clone()))
        .collect();
    let mut old = Vec::new();
    for (kind, path) in &objects {
        fill(&op, *kind, path, None).await?;
        old.push(op.read(path).await?.to_bytes());
    }

    fixture
        .execute_command(&format!("analyze table {DB}.t"))
        .await?;
    settle().await;

    assert_eq!(segments_and_stats(&fixture, "t").await?, (segments, stats));
    for ((kind, path), old) in objects.iter().zip(old) {
        assert!(
            op.read(path).await?.to_bytes() != old,
            "{path} was not rewritten in place"
        );
        assert_reads_as_stored(&op, *kind, path, None).await?;
    }
    Ok(())
}

/// The bloom index files of the current snapshot's blocks.
async fn bloom_indexes(fixture: &TestFixture, name: &str) -> Result<Vec<String>> {
    let ctx = fixture.new_query_ctx().await?;
    let table = fuse_table(fixture, name).await?;
    let table = FuseTable::try_from_table(table.as_ref())?;
    let snapshot = table.read_table_snapshot().await?.expect("a snapshot");
    let segments = SegmentsIO::create(ctx, table.get_operator(), table.schema())
        .read_segments::<SegmentInfo>(&snapshot.segments, CachePolicy::READ_ONLY)
        .await?;
    let mut paths = Vec::new();
    for segment in segments {
        for block in segment?.blocks {
            paths.extend(
                block
                    .bloom_filter_index_location
                    .as_ref()
                    .map(|(p, _)| p.clone()),
            );
        }
    }
    Ok(paths)
}

/// An ngram index refresh rewrites the bloom index files in place. Read before by a query,
/// whole or by range, they are read as rewritten after, and the new filters prune.
#[tokio::test(flavor = "multi_thread")]
async fn test_ngram_refresh_rewrites_are_not_read_back_as_they_were() -> anyhow::Result<()> {
    let dir = TempDir::new()?;
    let fixture = setup(&dir).await?;
    create_table(&fixture, "t", "").await?;
    let table = fuse_table(&fixture, "t").await?;
    let op = FuseTable::try_from_table(table.as_ref())?.get_operator();
    let indexes = bloom_indexes(&fixture, "t").await?;
    assert_eq!(indexes.len(), 3);
    let (meta, filter) = (
        CacheKind::of::<bloom::IndexMeta>(),
        CacheKind::of::<bloom::Filter>(),
    );
    let mut old = Vec::new();
    for path in &indexes {
        fill(&op, meta, path, None).await?;
        fill(&op, filter, path, Some(0..64)).await?;
        old.push(op.read(path).await?.to_bytes());
    }

    fixture
        .execute_command(&format!("create ngram index idx on {DB}.t(s)"))
        .await?;
    fixture
        .execute_command(&format!("refresh ngram index idx on {DB}.t"))
        .await?;
    settle().await;

    assert_eq!(bloom_indexes(&fixture, "t").await?, indexes);
    for (path, old) in indexes.iter().zip(old) {
        assert!(
            op.read(path).await?.to_bytes() != old,
            "{path} was not rewritten in place"
        );
        assert_reads_as_stored(&op, meta, path, None).await?;
        assert_reads_as_stored(&op, filter, path, Some(0..64)).await?;
    }
    let rows = query(
        &fixture,
        &format!("explain select * from {DB}.t where s like '%zzzzz%'"),
    )
    .await?;
    assert!(rows.contains("bloom pruning: 3 to 0"), "{rows}");
    Ok(())
}

/// The kind a fuse table file is cached as, by its directory.
fn kind_of(path: &str) -> CacheKind {
    if path.contains("/_ss/") {
        CacheKind::of::<table::Snapshot>()
    } else if path.contains("/_sg/") {
        CacheKind::of::<segment::Info>()
    } else if path.contains("/_i_b") {
        CacheKind::of::<bloom::Filter>()
    } else {
        CacheKind::of::<column::Data>()
    }
}

async fn table_files(op: &Operator, prefix: &str) -> Result<BTreeSet<String>> {
    let entries = op.list_with(&format!("{prefix}/")).recursive(true).await?;
    Ok(entries
        .into_iter()
        .filter(|e| !e.metadata().is_dir())
        .map(|e| e.path().to_string())
        .filter(|p| !p.contains("last_snapshot_location_hint"))
        .collect())
}

/// Vacuum deletes the snapshots, segments, blocks and indexes no snapshot it keeps refers to.
/// Read before by a query, none of them is read from the disk cache after; the table reads as
/// it was.
#[tokio::test(flavor = "multi_thread")]
async fn test_vacuumed_files_are_not_read_back() -> anyhow::Result<()> {
    let dir = TempDir::new()?;
    let fixture = setup(&dir).await?;
    create_table(&fixture, "t", "").await?;
    fixture
        .execute_command(&format!("optimize table {DB}.t compact"))
        .await?;
    let table = fuse_table(&fixture, "t").await?;
    let table = FuseTable::try_from_table(table.as_ref())?;
    let op = table.get_operator();
    let prefix = table.meta_location_generator().prefix().to_string();
    let before = table_files(&op, &prefix).await?;
    for path in &before {
        fill(&op, kind_of(path), path, None).await?;
    }

    fixture
        .execute_command(&format!("vacuum table {DB}.t"))
        .await?;
    settle().await;

    let after = table_files(&op, &prefix).await?;
    let deleted: Vec<_> = before.difference(&after).collect();
    for dir in ["/_ss/", "/_sg/", "/_b/", "/_i_b"] {
        assert!(
            deleted.iter().any(|p| p.contains(dir)),
            "vacuum deleted nothing under {dir}: {deleted:?}"
        );
    }
    for path in deleted {
        let read = DiskCachedStorage::new(op.clone())
            .read(kind_of(path), path, None, CacheFill::ReadOnly)
            .await;
        assert!(
            read.is_err(),
            "{path} is read from the disk cache after vacuum"
        );
    }
    check_rows(&fixture, "t").await?;
    Ok(())
}

/// Writes leave the disk cache as it was: the blocks, segments and indexes an INSERT writes
/// are not put on disk, nor is any of them looked up there. Only the snapshot the INSERT reads
/// to commit, as a query would, may be. The compaction and recluster the INSERT triggers after
/// its commit run as maintenance, and `test_maintenance_does_not_fill_the_disk_cache` covers
/// them on their own.
#[tokio::test(flavor = "multi_thread")]
async fn test_writes_do_not_fill_the_disk_cache() -> anyhow::Result<()> {
    let dir = TempDir::new()?;
    let fixture = setup(&dir).await?;
    create_table(&fixture, "w", "cluster by (a) enable_virtual_column = true").await?;
    fixture
        .execute_command(&format!("create ngram index idx_w on {DB}.w(s)"))
        .await?;
    settle().await;
    let before = disk_items();
    let accesses = disk_accesses();
    fixture
        .execute_command(&format!(
            "insert into {DB}.w select number, concat('k', number::string), \
             parse_json(concat('{{\"a\":', number::string, '}}')) from numbers(1000)"
        ))
        .await?;
    settle().await;
    let snapshot = CacheKind::of::<table::Snapshot>().name();
    let filled: Vec<String> = disk_items()
        .into_iter()
        .filter(|(kind, items)| {
            !kind.contains(snapshot.trim_start_matches("memory_cache_"))
                && *items > before.get(kind).copied().unwrap_or(0)
        })
        .map(|(kind, items)| format!("{kind}: {items}"))
        .collect();
    assert!(
        filled.is_empty(),
        "filled by writes:\n{}",
        filled.join("\n")
    );
    let looked_up: Vec<String> = disk_accesses()
        .into_iter()
        .filter(|(kind, n)| kind != snapshot && *n > accesses.get(kind).copied().unwrap_or(0))
        .map(|(kind, n)| format!("{kind}: {n}"))
        .collect();
    assert!(
        looked_up.is_empty(),
        "looked up by writes:\n{}",
        looked_up.join("\n")
    );
    Ok(())
}
