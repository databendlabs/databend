//  Copyright 2021 Datafuse Labs.
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

//! Read-path coverage for logical blocks whose columns live in several physical files.
//!
//! Nothing writes such blocks yet, so these tests fabricate one: each column group is serialized
//! independently with a projected sub-schema (which preserves the original column ids), and the
//! resulting `BlockMeta` is published through a hand-built segment and snapshot.

use std::collections::HashMap;
use std::sync::Arc;

use databend_common_base::base::tokio;
use databend_common_exception::Result;
use databend_common_expression::BlockThresholds;
use databend_common_expression::DataBlock;
use databend_common_expression::FromData;
use databend_common_expression::ScalarRef;
use databend_common_expression::TableSchemaRef;
use databend_common_expression::types::Int32Type;
use databend_common_expression::types::StringType;
use databend_common_storages_fuse::FuseStorageFormat;
use databend_common_storages_fuse::FuseTable;
use databend_common_storages_fuse::io::MetaWriter;
use databend_common_storages_fuse::io::WriteSettings;
use databend_common_storages_fuse::io::serialize_block;
use databend_common_storages_fuse::statistics::gen_columns_statistics;
use databend_common_storages_fuse::statistics::reducers::reduce_block_metas;
use databend_query::sessions::TableContextTableAccess;
use databend_query::test_kits::TestFixture;
use databend_storages_common_cache::Table;
use databend_storages_common_table_meta::meta::BlockMeta;
use databend_storages_common_table_meta::meta::ColumnGroupFileMeta;
use databend_storages_common_table_meta::meta::Compression;
use databend_storages_common_table_meta::meta::SegmentInfo;
use databend_storages_common_table_meta::meta::TableSnapshot;
use databend_storages_common_table_meta::meta::Versioned;
use futures_util::TryStreamExt;

/// Rows fabricated into the split block.
const ROWS: [(i32, &str, i32); 3] = [(1, "alice", 10), (2, "bob", 20), (3, "carol", 30)];

const TABLE_NAME: &str = "t_column_groups";

/// Creates a table with three flat columns, so the split needs no nested-column handling, then
/// replaces its contents with one block spread over two physical files.
async fn setup_split_block() -> Result<(TestFixture, String)> {
    let fixture = TestFixture::setup().await?;
    fixture.create_default_database().await?;
    let table = format!("{}.{TABLE_NAME}", fixture.default_db_name());
    fixture
        .execute_command(&format!(
            "create table {table} (id int, name string, score int)"
        ))
        .await?;
    // Establish a base snapshot to build the fabricated one from. Its contents are replaced below.
    fixture
        .execute_command(&format!("insert into {table} values (0, 'placeholder', 0)"))
        .await?;
    publish_split_block(&fixture).await?;
    Ok((fixture, table))
}

#[tokio::test(flavor = "multi_thread")]
async fn test_read_block_split_across_column_groups() -> Result<()> {
    let (fixture, table) = setup_split_block().await?;

    // Full projection: every column group must be read and stitched into one block.
    let rows = query_rows(
        &fixture,
        &format!("select id, name, score from {table} order by id"),
    )
    .await?;
    assert_eq!(rows, vec![
        "1|alice|10".to_string(),
        "2|bob|20".to_string(),
        "3|carol|30".to_string(),
    ]);

    // Projection confined to the first group: `score` lives in the other file and must not be read.
    let rows = query_rows(
        &fixture,
        &format!("select id, name from {table} order by id"),
    )
    .await?;
    assert_eq!(rows, vec![
        "1|alice".to_string(),
        "2|bob".to_string(),
        "3|carol".to_string(),
    ]);

    // Projection confined to the second group.
    let rows = query_rows(
        &fixture,
        &format!("select score from {table} order by score"),
    )
    .await?;
    assert_eq!(rows, vec![
        "10".to_string(),
        "20".to_string(),
        "30".to_string()
    ]);

    // Projection spanning both groups, in an order that matches neither file's layout.
    let rows = query_rows(
        &fixture,
        &format!("select score, id from {table} order by id"),
    )
    .await?;
    assert_eq!(rows, vec![
        "10|1".to_string(),
        "20|2".to_string(),
        "30|3".to_string()
    ]);

    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn test_filter_block_split_across_column_groups() -> Result<()> {
    let (fixture, table) = setup_split_block().await?;

    // Filters run through pruning before the read. The fabricated block carries no Bloom index, so
    // pruning must fall through to a plain scan rather than discarding matching rows.
    let rows = query_rows(
        &fixture,
        &format!("select id, name, score from {table} where score = 20"),
    )
    .await?;
    assert_eq!(rows, vec!["2|bob|20".to_string()]);

    // Filter on a column in one group, projecting a column from the other.
    let rows = query_rows(
        &fixture,
        &format!("select score from {table} where name = 'carol'"),
    )
    .await?;
    assert_eq!(rows, vec!["30".to_string()]);

    let rows = query_rows(
        &fixture,
        &format!("select count(*) from {table} where score > 15"),
    )
    .await?;
    assert_eq!(rows, vec!["2".to_string()]);

    Ok(())
}

/// Writes the rows above as one logical block spread over two physical files, then publishes it as
/// the table's only block.
async fn publish_split_block(fixture: &TestFixture) -> Result<()> {
    let ctx = fixture.new_query_ctx().await?;
    let table = ctx
        .get_table(
            &fixture.default_catalog_name(),
            &fixture.default_db_name(),
            TABLE_NAME,
        )
        .await?;
    let fuse_table = FuseTable::try_from_table(table.as_ref())?;
    let schema = table.schema();
    let operator = fuse_table.get_operator();
    let location_generator = fuse_table.meta_location_generator();
    let table_meta_timestamps = TestFixture::default_table_meta_timestamps();

    let block = DataBlock::new_from_columns(vec![
        Int32Type::from_data(ROWS.iter().map(|row| row.0).collect::<Vec<_>>()),
        StringType::from_data(ROWS.iter().map(|row| row.1).collect::<Vec<_>>()),
        Int32Type::from_data(ROWS.iter().map(|row| row.2).collect::<Vec<_>>()),
    ]);
    let row_count = block.num_rows() as u64;
    let col_stats = gen_columns_statistics(&block, None, &schema, &Default::default())?;

    // The codec recorded on the block must match what the files were written with, or the reader
    // decompresses with the wrong one.
    let write_settings = WriteSettings {
        storage_format: FuseStorageFormat::Parquet,
        ..Default::default()
    };
    let compression: Compression = write_settings.table_compression.into();

    // (id, name) in the first file, (score) in the second.
    let mut groups = Vec::new();
    for field_indices in [vec![0usize, 1], vec![2usize]] {
        let (location, _) = location_generator.gen_block_location(table_meta_timestamps);
        groups.push(
            write_column_group(
                &operator,
                &write_settings,
                &schema,
                &block,
                &field_indices,
                location,
            )
            .await?,
        );
    }

    // Mirror the invariants the writer maintains: `location` anchors the newest group, the sizes are
    // sums across groups, and `col_metas` is the union of every group's active metadata.
    let newest = groups.last().expect("at least one group").location.clone();
    let file_size = groups.iter().map(|group| group.file_size).sum();
    let block_size = groups.iter().map(|group| group.uncompressed_size).sum();
    let col_metas = groups
        .iter()
        .flat_map(|group| group.leaf_column_metas.clone())
        .collect::<HashMap<_, _>>();

    let mut block_meta = BlockMeta::new(
        row_count,
        block_size,
        file_size,
        col_stats,
        col_metas,
        None,
        newest,
        None,
        0,
        None,
        None,
        None,
        None,
        None,
        None,
        None,
        None,
        compression,
        None,
    );
    block_meta.column_groups = groups;

    let block_metas = vec![Arc::new(block_meta)];
    let summary = reduce_block_metas(&block_metas, BlockThresholds::default(), None);
    let segment_info = SegmentInfo::new(block_metas, summary);
    let segment_location =
        location_generator.gen_segment_info_location(table_meta_timestamps, false);
    segment_info
        .write_meta(&operator, &segment_location)
        .await?;

    // Commit a snapshot whose only segment is the fabricated one, so queries see exactly this block.
    let base_snapshot = fuse_table
        .read_table_snapshot()
        .await?
        .expect("base snapshot written by the seed insert");
    let mut snapshot = TableSnapshot::try_from_previous(
        base_snapshot,
        fuse_table.cluster_key_meta(),
        Some(fuse_table.get_table_info().ident.seq),
        table_meta_timestamps,
    )?;
    snapshot.segments = vec![(segment_location, SegmentInfo::VERSION)];
    snapshot.summary = segment_info.summary.clone();
    fuse_table
        .commit_to_meta_server(
            ctx.as_ref(),
            fuse_table.get_table_info(),
            location_generator,
            snapshot,
            None,
            &None,
            &operator,
        )
        .await?;

    Ok(())
}

/// Serializes the projected columns into their own parquet file and describes it as a column group.
async fn write_column_group(
    operator: &opendal::Operator,
    write_settings: &WriteSettings,
    schema: &TableSchemaRef,
    block: &DataBlock,
    field_indices: &[usize],
    location: databend_storages_common_table_meta::meta::Location,
) -> Result<ColumnGroupFileMeta> {
    // `project` clones the fields, so each sub-schema keeps the column ids of the parent table.
    // `serialize_block` keys the returned metadata by those ids, which is what lets a reader find
    // a column in whichever file owns it.
    let group_schema = Arc::new(schema.project(field_indices));
    let group_block = DataBlock::new_from_columns(
        field_indices
            .iter()
            .map(|index| block.get_by_offset(*index).to_column())
            .collect(),
    );
    // Matches how the writer sizes a group, so the summed `block_size` is comparable to a
    // normally-written block.
    let uncompressed_size = group_block.estimate_block_size(group_block.num_columns()) as u64;

    let (leaf_column_metas, buffer) = serialize_block(write_settings, &group_schema, group_block)?;
    let file_size = buffer.len() as u64;
    operator.write(&location.0, buffer).await?;

    let mut active_column_ids = leaf_column_metas.keys().copied().collect::<Vec<_>>();
    active_column_ids.sort_unstable();

    Ok(ColumnGroupFileMeta {
        active_column_ids,
        location,
        file_size,
        uncompressed_size,
        leaf_column_metas,
        bloom: None,
    })
}

/// Runs a query and renders each row as pipe-joined column values. Strings are rendered unquoted so
/// the expectations above stay readable.
async fn query_rows(fixture: &TestFixture, query: &str) -> Result<Vec<String>> {
    let stream = fixture.execute_query(query).await?;
    let blocks: Vec<DataBlock> = stream.try_collect().await?;
    let mut rows = Vec::new();
    for block in blocks {
        for row in 0..block.num_rows() {
            let values = block
                .columns()
                .iter()
                .map(|entry| match entry.value().index(row).unwrap() {
                    ScalarRef::String(value) => value.to_string(),
                    other => other.to_string(),
                })
                .collect::<Vec<_>>();
            rows.push(values.join("|"));
        }
    }
    Ok(rows)
}
