// Copyright 2021 Datafuse Labs
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

//! Reading blocks whose columns live in several column group files ("split blocks").
//!
//! Nothing writes split blocks yet, so `split_blocks` forges them: it moves columns of every
//! block of a table into new group files, as a partial-column UPDATE will. Each test runs the
//! same statements on the split table and on a reference table that holds the same rows in
//! legacy blocks, and compares the results.
//!
//! Each group file gets a Bloom index of its columns. The legacy file keeps its Bloom index,
//! which still holds the filter of `a` for the values `a` had before the split.

use std::collections::HashMap;
use std::sync::Arc;

use databend_common_catalog::plan::Projection;
use databend_common_catalog::table::Table;
use databend_common_exception::Result;
use databend_common_expression::Column;
use databend_common_expression::DataBlock;
use databend_common_expression::FunctionContext;
use databend_common_expression::block_debug::pretty_format_blocks;
use databend_common_storages_fuse::FuseTable;
use databend_common_storages_fuse::io::MetaWriter;
use databend_common_storages_fuse::io::SegmentsIO;
use databend_common_storages_fuse::io::TableMetaLocationGenerator;
use databend_common_storages_fuse::io::WriteSettings;
use databend_common_storages_fuse::io::serialize_block;
use databend_common_storages_fuse::statistics::gen_columns_statistics;
use databend_common_storages_fuse::statistics::reducers::reduce_block_metas;
use databend_query::sessions::QueryContext;
use databend_query::sessions::TableContext;
use databend_query::sessions::TableContextTableAccess;
use databend_query::sessions::TableContextTableManagement;
use databend_query::test_kits::*;
use databend_storages_common_blocks::blocks_to_parquet;
use databend_storages_common_cache::CachePolicy;
use databend_storages_common_index::BloomIndex;
use databend_storages_common_index::BloomIndexBuilder;
use databend_storages_common_index::BloomIndexType;
use databend_storages_common_index::filters::BlockFilter;
use databend_storages_common_io::ReadSettings;
use databend_storages_common_table_meta::meta::ColumnGroupBloomMeta;
use databend_storages_common_table_meta::meta::ColumnGroupFileMeta;
use databend_storages_common_table_meta::meta::SegmentInfo;
use databend_storages_common_table_meta::meta::TableSnapshot;
use databend_storages_common_table_meta::meta::Versioned;
use databend_storages_common_table_meta::table::TableCompression;
use futures::TryStreamExt;

/// One group file to write for every block: its columns and its codec.
struct Group {
    columns: &'static [&'static str],
    compression: TableCompression,
}

/// `a` takes the values of `id`, so that reading `a` from the legacy file would show.
/// `id`, `arr` and `c` stay in the legacy file.
const GROUPS: &[Group] = &[
    Group {
        columns: &["a"],
        compression: TableCompression::LZ4,
    },
    Group {
        columns: &["s", "n"],
        compression: TableCompression::None,
    },
];

const SCHEMA: &str = "(id int not null, a int not null, s string, n tuple(x int, y string), \
     arr array(int), c string)";

/// Creates the table `t`, made of split blocks, and the table `t_ref`, holding the same rows in
/// legacy blocks.
async fn setup(fixture: &TestFixture, drop_stats_of: &[&str]) -> Result<()> {
    setup_with(fixture, drop_stats_of, 4, 6).await
}

/// Like [`setup`], with blocks of up to `row_per_block` rows filled by three inserts of `rows`
/// rows each.
async fn setup_with(
    fixture: &TestFixture,
    drop_stats_of: &[&str],
    row_per_block: usize,
    rows: usize,
) -> Result<()> {
    fixture
        .execute_command(&format!(
            "create table t {SCHEMA} row_per_block = {row_per_block}"
        ))
        .await?;
    fixture
        .execute_command(&format!(
            "create table t_ref {SCHEMA} row_per_block = {row_per_block}"
        ))
        .await?;
    for batch in 0..3 {
        let values = "select number + {start} as id, (number + {start}) * 2 as a, \
             concat('v', (number + {start})::string) as s, \
             (number + {start}, concat('y', (number + {start})::string)) as n, \
             [number + {start}, number + {start} + 1] as arr, \
             concat('c', (number + {start})::string) as c from numbers({rows})"
            .replace("{start}", &(batch * rows).to_string())
            .replace("{rows}", &rows.to_string());
        fixture
            .execute_command(&format!("insert into t {values}"))
            .await?;
        fixture
            .execute_command(&format!(
                "insert into t_ref select id, id, s, n, arr, c from ({values})"
            ))
            .await?;
    }
    split_blocks(fixture, "t", drop_stats_of).await
}

async fn latest_table(ctx: &Arc<QueryContext>, name: &str) -> Result<FuseTable> {
    ctx.evict_table_from_cache("default", "default", name)?;
    let table = ctx
        .get_catalog("default")
        .await?
        .get_table(&ctx.get_tenant(), "default", name)
        .await?;
    Ok(FuseTable::try_from_table(table.as_ref())?.clone())
}

/// Moves the columns of `GROUPS` of every block of `name` into new group files, and drops the
/// column statistics of `drop_stats_of`, which a block may legitimately lack.
async fn split_blocks(fixture: &TestFixture, name: &str, drop_stats_of: &[&str]) -> Result<()> {
    let ctx = fixture.new_query_ctx().await?;
    let table = latest_table(&ctx, name).await?;
    let snapshot = table.read_table_snapshot().await?.unwrap();
    let schema = table.schema();
    let operator = table.get_operator();
    let location_gen = table.meta_location_generator();
    let timestamps = TestFixture::default_table_meta_timestamps();
    let reader = table.create_block_reader(
        ctx.clone(),
        Projection::Columns((0..schema.num_fields()).collect()),
        CachePolicy::READ_ONLY,
    )?;
    let table_ctx: Arc<dyn TableContext> = ctx.clone();
    let read_settings = ReadSettings::from_ctx(&table_ctx)?;
    let segments = SegmentsIO::create(ctx.clone(), operator.clone(), schema.clone())
        .read_segments::<SegmentInfo>(&snapshot.segments, CachePolicy::READ_ONLY)
        .await?;

    let mut segment_locations = Vec::with_capacity(segments.len());
    let mut all_blocks = Vec::new();
    for segment in segments {
        let mut segment = segment?;
        let mut blocks = Vec::with_capacity(segment.blocks.len());
        for block in &segment.blocks {
            let data = reader
                .read_by_meta(&read_settings, block, &table.get_storage_format())
                .await?;
            let mut meta = block.as_ref().clone();
            for group in GROUPS {
                let indices = group
                    .columns
                    .iter()
                    .map(|name| schema.index_of(name))
                    .collect::<Result<Vec<_>>>()?;
                let group_schema = Arc::new(schema.project(&indices));
                let columns = group
                    .columns
                    .iter()
                    .zip(&indices)
                    .map(|(name, index)| {
                        let source = if *name == "a" {
                            schema.index_of("id").unwrap()
                        } else {
                            *index
                        };
                        data.get_by_offset(source).to_column()
                    })
                    .collect::<Vec<Column>>();
                let group_block = DataBlock::new_from_columns(columns);
                let stats = gen_columns_statistics(
                    &group_block,
                    None,
                    &group_schema,
                    &Default::default(),
                    HashMap::new(),
                )?;
                let bloom_columns = table
                    .bloom_index_cols()
                    .bloom_index_fields(group_schema.clone(), BloomIndex::supported_type)?;
                let mut bloom_builder = BloomIndexBuilder::create(
                    FunctionContext::default(),
                    BloomIndexType::default(),
                    bloom_columns,
                    &[],
                )?;
                bloom_builder.add_block(&group_block)?;
                let bloom_index = bloom_builder
                    .finalize()?
                    .expect("a group with a Bloom column");
                let write_settings = WriteSettings {
                    table_compression: group.compression,
                    ..table.get_write_settings()
                };
                let (col_metas, bytes) =
                    serialize_block(&write_settings, &group_schema, group_block)?;
                let (location, _) = location_gen.gen_block_location(timestamps);
                let file_size = bytes.len() as u64;
                operator.write(&location.0, bytes).await?;
                let bloom_location = (
                    TableMetaLocationGenerator::gen_bloom_index_location_from_block_location(
                        &location.0,
                    ),
                    BlockFilter::VERSION,
                );
                let bloom_file = blocks_to_parquet(
                    &bloom_index.filter_schema,
                    vec![bloom_index.serialize_to_data_block()?],
                    TableCompression::None,
                    false,
                    None,
                )?
                .payload;
                let bloom_size = bloom_file.len() as u64;
                operator.write(&bloom_location.0, bloom_file).await?;
                meta.col_stats.extend(stats);
                meta.append_column_group(ColumnGroupFileMeta {
                    location,
                    file_size,
                    leaf_column_metas: col_metas,
                    bloom: Some(ColumnGroupBloomMeta {
                        location: bloom_location,
                        file_size: bloom_size,
                    }),
                    compression: group.compression.into(),
                })?;
            }
            for name in drop_stats_of {
                let field = schema.field_with_name(name)?;
                for column_id in field.leaf_column_ids() {
                    meta.col_stats.remove(&column_id);
                }
            }
            assert!(meta.is_split());
            blocks.push(Arc::new(meta));
        }
        // Pruning trusts the summaries: they must cover the new values.
        segment.summary.col_stats =
            reduce_block_metas(&blocks, table.get_block_thresholds(), None)?.col_stats;
        all_blocks.extend(blocks.iter().cloned());
        segment.blocks = blocks;
        let location = location_gen.gen_segment_info_location(timestamps, false);
        segment.write_meta(&operator, &location).await?;
        segment_locations.push((location, SegmentInfo::VERSION));
    }

    let mut new_snapshot =
        TableSnapshot::try_from_previous(snapshot, table.cluster_key_info(), None, timestamps)?;
    new_snapshot.segments = segment_locations;
    new_snapshot.summary.col_stats =
        reduce_block_metas(&all_blocks, table.get_block_thresholds(), None)?.col_stats;
    table
        .commit_to_meta_server(
            ctx.as_ref(),
            table.get_table_info(),
            location_gen,
            new_snapshot,
            None,
            &None,
            &operator,
        )
        .await
}

async fn query(fixture: &TestFixture, sql: &str) -> Result<String> {
    let blocks: Vec<DataBlock> = fixture.execute_query(sql).await?.try_collect().await?;
    if blocks.is_empty() {
        return Ok("<no rows>".to_string());
    }
    pretty_format_blocks(&blocks)
}

/// Runs `sql` on `t` and on `t_ref`, `{t}` standing for the table, and compares the results.
async fn assert_same(fixture: &TestFixture, sql: &str) -> Result<()> {
    let split = query(fixture, &sql.replace("{t}", "t")).await?;
    let reference = query(fixture, &sql.replace("{t}", "t_ref")).await?;
    assert_eq!(split, reference, "{sql}");
    Ok(())
}

async fn execute_on_both(fixture: &TestFixture, sql: &str) -> Result<()> {
    fixture.execute_command(&sql.replace("{t}", "t")).await?;
    fixture.execute_command(&sql.replace("{t}", "t_ref")).await
}

/// The number of split blocks of `name`, and the number of all its blocks.
async fn block_counts(fixture: &TestFixture, name: &str) -> Result<(usize, usize)> {
    let ctx = fixture.new_query_ctx().await?;
    let table = latest_table(&ctx, name).await?;
    let Some(snapshot) = table.read_table_snapshot().await? else {
        return Ok((0, 0));
    };
    let segments = SegmentsIO::create(ctx.clone(), table.get_operator(), table.schema())
        .read_segments::<SegmentInfo>(&snapshot.segments, CachePolicy::READ_ONLY)
        .await?;
    let (mut split, mut total) = (0, 0);
    for segment in segments {
        let segment = segment?;
        split += segment.blocks.iter().filter(|b| b.is_split()).count();
        total += segment.blocks.len();
    }
    Ok((split, total))
}

#[tokio::test(flavor = "multi_thread")]
async fn test_scan_split_blocks() -> anyhow::Result<()> {
    let fixture = TestFixture::setup().await?;
    setup(&fixture, &[]).await?;
    let (split, total) = block_counts(&fixture, "t").await?;
    assert!(total > 1);
    assert_eq!(split, total);

    for sql in [
        "select * from {t} order by id",
        "select a, arr from {t} where id > 5 order by id",
        "select id, s from {t} where a = 3",
        "select n.1, n.2, a from {t} where a between 4 and 9 order by a",
        "select count(*), sum(a) from {t}",
        "select id from {t} where s = 'v7'",
        // Lazy materialization fetches the rows of the top-k from the blocks.
        "select * from {t} order by a desc limit 3",
    ] {
        assert_same(&fixture, sql).await?;
    }

    // A column added after the split is in no group and reads as its default.
    execute_on_both(&fixture, "alter table {t} add column z int default 7").await?;
    assert_same(&fixture, "select * from {t} order by id").await?;
    Ok(())
}

/// A filter-only scan decodes the filter columns of a block of more than one batch in batches.
/// The columns come from files of different codecs.
#[tokio::test(flavor = "multi_thread")]
async fn test_filter_only_scan_split_blocks() -> anyhow::Result<()> {
    let fixture = TestFixture::setup().await?;
    setup_with(&fixture, &[], 10_000, 10_000).await?;
    let (split, total) = block_counts(&fixture, "t").await?;
    assert_eq!(split, total);
    let blocks = query(
        &fixture,
        "select min(row_count) > 8192 from fuse_block('default', 't')",
    )
    .await?;
    assert!(blocks.contains("true"), "{blocks}");

    for sql in [
        "select count(*) from {t} where s like '%99%'",
        "select count(*) from {t} where s like '%99%' and id % 2 = 0",
        "select count(*) from {t} where a % 7 = 0 and s <> 'v7'",
    ] {
        assert_same(&fixture, sql).await?;
    }
    Ok(())
}

/// A split block keeps a column without statistics. Range pruning must not take the column for
/// one the block lacks, which it would read as its default value.
#[tokio::test(flavor = "multi_thread")]
async fn test_range_pruning_split_block_without_column_stats() -> anyhow::Result<()> {
    let fixture = TestFixture::setup().await?;
    setup(&fixture, &["s", "a"]).await?;
    assert_same(&fixture, "select id from {t} where s = 'v7'").await?;
    assert_same(&fixture, "select id from {t} where a = 7").await?;
    Ok(())
}

/// Every mutation reads the split blocks it rewrites and writes legacy blocks.
#[tokio::test(flavor = "multi_thread")]
async fn test_mutate_split_blocks() -> anyhow::Result<()> {
    let fixture = TestFixture::setup().await?;
    setup(&fixture, &[]).await?;

    execute_on_both(&fixture, "update {t} set s = 'u' where id % 3 = 0").await?;
    assert_same(&fixture, "select * from {t} order by id").await?;

    execute_on_both(&fixture, "delete from {t} where id % 4 = 1").await?;
    assert_same(&fixture, "select * from {t} order by id").await?;

    execute_on_both(
        &fixture,
        "merge into {t} using (select number as id from numbers(18) where number % 5 = 0) as src \
         on {t}.id = src.id when matched then update set a = {t}.a + 100",
    )
    .await?;
    assert_same(&fixture, "select * from {t} order by id").await?;

    execute_on_both(
        &fixture,
        "replace into {t} on(id) values (2, 200, 'r', (2, 'r'), [2], 'r'), \
         (100, 100, 'n', (1, 'n'), [], 'n')",
    )
    .await?;
    assert_same(&fixture, "select * from {t} order by id").await?;
    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn test_compact_split_blocks() -> anyhow::Result<()> {
    let fixture = TestFixture::setup().await?;
    setup(&fixture, &[]).await?;
    execute_on_both(
        &fixture,
        "alter table {t} set options(row_per_block = 1000)",
    )
    .await?;
    execute_on_both(&fixture, "optimize table {t} compact").await?;
    assert_eq!(block_counts(&fixture, "t").await?.0, 0);
    assert_same(&fixture, "select * from {t} order by id").await?;
    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn test_recluster_split_blocks() -> anyhow::Result<()> {
    let fixture = TestFixture::setup().await?;
    setup(&fixture, &[]).await?;
    for multiway_merge in [0, 1] {
        fixture
            .execute_command(&format!(
                "set enable_recluster_multiway_merge = {multiway_merge}"
            ))
            .await?;
        execute_on_both(&fixture, "alter table {t} cluster by (a)").await?;
        execute_on_both(&fixture, "alter table {t} recluster final").await?;
        assert_same(&fixture, "select * from {t} order by id").await?;
    }
    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn test_table_functions_on_split_blocks() -> anyhow::Result<()> {
    let fixture = TestFixture::setup().await?;
    setup(&fixture, &[]).await?;

    // Each column is reported in the file that stores it: every block has 3 files.
    let (_, blocks) = block_counts(&fixture, "t").await?;
    let files = query(
        &fixture,
        &format!(
            "select count(distinct block_location) = {} from fuse_column('default', 't')",
            blocks * 3
        ),
    )
    .await?;
    assert!(files.contains("true"), "{files}");
    assert_same(
        &fixture,
        "select column_name, count(*), sum(row_count) from fuse_column('default', '{t}') \
         group by column_name order by column_name",
    )
    .await?;
    assert_same(
        &fixture,
        "select column_name, count(*) from fuse_encoding('default', '{t}') \
         group by column_name order by column_name",
    )
    .await?;
    assert_same(
        &fixture,
        // The group files are written with dictionary pages, so only data pages compare.
        "select column_name, sum(num_values) from fuse_page('default', '{t}') \
         where page_type <> 'dictionary' group by column_name order by column_name",
    )
    .await?;

    execute_on_both(&fixture, "analyze table {t}").await?;
    Ok(())
}

/// The blocks bloom pruning gets and keeps for `sql`, as `explain` reports them.
async fn bloom_pruning(fixture: &TestFixture, sql: &str) -> Result<(u64, u64)> {
    let plan = query(fixture, &format!("explain {sql}")).await?;
    let (_, rest) = plan
        .split_once("bloom pruning: ")
        .unwrap_or_else(|| panic!("no bloom pruning in {plan}"));
    let mut counts = rest
        .split(|c: char| !c.is_ascii_digit())
        .filter(|n| !n.is_empty())
        .map(|n| n.parse::<u64>().unwrap());
    Ok((counts.next().unwrap(), counts.next().unwrap()))
}

/// Bloom pruning of a split block reads each column's filter from the index file of the group
/// that serves the column.
#[tokio::test(flavor = "multi_thread")]
async fn test_bloom_pruning_split_blocks() -> anyhow::Result<()> {
    let fixture = TestFixture::setup().await?;
    // Without statistics of `a`, range pruning keeps every block of `t` for a filter on `a`.
    setup(&fixture, &["a"]).await?;
    let (_, blocks) = block_counts(&fixture, "t").await?;

    // Only the block of id 7 holds `a = 7`. Before the split, `a` of that block was twice its
    // id; the index file of the legacy file still says so and would drop the row.
    let sql = "select id from {t} where a = 7";
    assert_same(&fixture, sql).await?;
    assert_eq!(
        bloom_pruning(&fixture, &sql.replace("{t}", "t")).await?,
        (blocks as u64, 1)
    );
    // `a` was 30 in a block before the split, and is 30 in none now.
    let sql = "select id from {t} where a = 30";
    assert_same(&fixture, sql).await?;
    assert_eq!(
        bloom_pruning(&fixture, &sql.replace("{t}", "t")).await?,
        (blocks as u64, 0)
    );

    // `s` is in a group file and `c` in the legacy file. Proving the disjunction false takes the
    // filters of both files.
    for sql in [
        "select id from {t} where s = 'v1x'",
        "select id from {t} where s = 'v1x' or c = 'c1x'",
    ] {
        assert_same(&fixture, sql).await?;
        let split = bloom_pruning(&fixture, &sql.replace("{t}", "t")).await?;
        let reference = bloom_pruning(&fixture, &sql.replace("{t}", "t_ref")).await?;
        assert_eq!(split, reference, "{sql}");
        assert!(reference.1 < reference.0, "{sql}: {reference:?}");
    }
    Ok(())
}
