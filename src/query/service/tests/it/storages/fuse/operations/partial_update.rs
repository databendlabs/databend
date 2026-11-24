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

//! UPDATE of a table with `enable_partial_update` writes only the columns it changes, into a new
//! column group file of each block it changes.
//!
//! Each test runs the same statements on `t`, which takes partial UPDATEs, and on `t_ref`, which
//! does not, and compares the results.

use std::collections::BTreeSet;
use std::sync::Arc;

use databend_common_catalog::table::Table;
use databend_common_exception::Result;
use databend_common_expression::ColumnId;
use databend_common_expression::DataBlock;
use databend_common_expression::block_debug::pretty_format_blocks;
use databend_common_storages_fuse::FuseTable;
use databend_common_storages_fuse::io::SegmentsIO;
use databend_query::sessions::QueryContext;
use databend_query::sessions::TableContextTableAccess;
use databend_query::sessions::TableContextTableManagement;
use databend_query::test_kits::*;
use databend_storages_common_cache::CachePolicy;
use databend_storages_common_table_meta::meta::SegmentInfo;
use futures::TryStreamExt;

// `n` is last: a mutation filter on a column after a tuple column reads the wrong column.
const SCHEMA: &str =
    "(id int not null, a int not null, b int, s string, c string, n tuple(x int, y string))";

/// Creates `t` and `t_ref` with `options`, and inserts the same rows into both.
async fn setup(fixture: &TestFixture, options: &str) -> Result<()> {
    fixture
        .execute_command(&format!(
            "create table t {SCHEMA} row_per_block = 4 enable_partial_update = true {options}"
        ))
        .await?;
    fixture
        .execute_command(&format!(
            "create table t_ref {SCHEMA} row_per_block = 4 {options}"
        ))
        .await?;
    for batch in 0..3 {
        execute_on_both(
            fixture,
            &format!(
                "insert into {{t}} select number + {start} as id, number + {start} as a, \
                 number + {start} as b, concat('s', (number + {start})::string) as s, \
                 concat('c', (number + {start})::string) as c, \
                 (number + {start}, concat('y', (number + {start})::string)) as n \
                 from numbers(6)",
                start = batch * 6
            ),
        )
        .await?;
    }
    Ok(())
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

async fn query(fixture: &TestFixture, sql: &str) -> Result<String> {
    let blocks: Vec<DataBlock> = fixture.execute_query(sql).await?.try_collect().await?;
    if blocks.is_empty() {
        return Ok("<no rows>".to_string());
    }
    pretty_format_blocks(&blocks)
}

/// Runs `sql` on `t` and on `t_ref`, `{t}` standing for the table, and compares the results.
async fn assert_same(fixture: &TestFixture, sql: &str) -> Result<()> {
    let partial = query(fixture, &sql.replace("{t}", "t")).await?;
    let reference = query(fixture, &sql.replace("{t}", "t_ref")).await?;
    assert_eq!(partial, reference, "{sql}");
    Ok(())
}

async fn execute_on_both(fixture: &TestFixture, sql: &str) -> Result<()> {
    fixture.execute_command(&sql.replace("{t}", "t")).await?;
    fixture.execute_command(&sql.replace("{t}", "t_ref")).await
}

/// The files of every block of `name`, in the order of the smallest `id` of the blocks: for each
/// file, the fields it serves. A legacy block has one file.
async fn block_files(fixture: &TestFixture, name: &str) -> Result<Vec<Vec<BTreeSet<String>>>> {
    let ctx = fixture.new_query_ctx().await?;
    let table = latest_table(&ctx, name).await?;
    let snapshot = table.read_table_snapshot().await?.unwrap();
    let schema = table.schema_with_stream();
    let field_of = |column_id: &ColumnId| {
        schema
            .fields()
            .iter()
            .find(|field| field.leaf_column_ids().contains(column_id))
            .map(|field| field.name().clone())
            .unwrap()
    };
    let segments = SegmentsIO::create(ctx.clone(), table.get_operator(), table.schema())
        .read_segments::<SegmentInfo>(&snapshot.segments, CachePolicy::READ_ONLY)
        .await?;
    let id = schema.field_with_name("id")?.column_id();
    let mut blocks = Vec::new();
    for segment in segments {
        let segment = segment?;
        for block in &segment.blocks {
            block.check_column_groups()?;
            let files = block
                .data_files()
                .iter()
                .map(|file| file.col_metas.keys().map(field_of).collect())
                .collect::<Vec<_>>();
            blocks.push((block.col_stats[&id].min.clone(), files));
        }
    }
    blocks.sort_by(|a, b| a.0.cmp(&b.0));
    Ok(blocks.into_iter().map(|(_, files)| files).collect())
}

/// Asserts that some blocks of `t` have more than one file, and that the newest file of each of
/// them serves `newest`.
async fn assert_newest_groups(fixture: &TestFixture, newest: &[&str]) -> Result<()> {
    let files = block_files(fixture, "t").await?;
    let split = files
        .iter()
        .filter(|block| block.len() > 1)
        .collect::<Vec<_>>();
    assert!(!split.is_empty());
    for block in split {
        assert_eq!(block.last(), Some(&fields(newest)), "{files:?}");
    }
    Ok(())
}

fn fields(names: &[&str]) -> BTreeSet<String> {
    names.iter().map(|name| name.to_string()).collect()
}

/// Each UPDATE adds a group file of the columns it sets to the blocks it changes. A column is
/// read from the newest file that has it, and a file left with no column is dropped.
#[tokio::test(flavor = "multi_thread")]
async fn test_partial_update_writes_set_columns() -> anyhow::Result<()> {
    let fixture = TestFixture::setup().await?;
    setup(&fixture, "").await?;
    let legacy = fields(&["id", "a", "b", "s", "n", "c"]);

    // Only the block of ids 0..=3 has rows with `id < 2`.
    execute_on_both(&fixture, "update {t} set a = a + 100 where id < 2").await?;
    let files = block_files(&fixture, "t").await?;
    assert_eq!(files[0], vec![
        fields(&["id", "b", "s", "n", "c"]),
        fields(&["a"])
    ]);
    assert!(files[1..].iter().all(|block| block == std::slice::from_ref(&legacy)));
    assert_same(&fixture, "select * from {t} order by id").await?;

    // The design example: UPDATE a, UPDATE b, UPDATE a.
    execute_on_both(
        &fixture,
        "update {t} set b = b * 10, n = (id, 'new') where id < 4",
    )
    .await?;
    execute_on_both(&fixture, "update {t} set a = a + 1 where id = 3").await?;
    let files = block_files(&fixture, "t").await?;
    assert_eq!(files[0], vec![
        fields(&["id", "s", "c"]),
        fields(&["b", "n"]),
        fields(&["a"]),
    ]);
    assert_same(&fixture, "select * from {t} order by id").await?;

    // No row matches: no block changes.
    execute_on_both(&fixture, "update {t} set a = 0 where id > 1000").await?;
    assert_eq!(block_files(&fixture, "t").await?, files);

    // Without a filter every block takes a group file.
    execute_on_both(&fixture, "update {t} set s = concat(s, 'x')").await?;
    let files = block_files(&fixture, "t").await?;
    assert!(files.iter().all(|block| block.len() > 1));
    assert_newest_groups(&fixture, &["s"]).await?;
    assert_same(&fixture, "select * from {t} order by id").await?;
    Ok(())
}

/// Range pruning and Bloom pruning see the new values of the rewritten columns, and the
/// statistics of the other columns stay.
#[tokio::test(flavor = "multi_thread")]
async fn test_partial_update_maintains_statistics_and_bloom() -> anyhow::Result<()> {
    let fixture = TestFixture::setup().await?;
    setup(&fixture, "").await?;
    execute_on_both(
        &fixture,
        "update {t} set a = a + 1000, s = concat('new', id::string) where id % 2 = 0",
    )
    .await?;

    for sql in [
        // Old statistics of `a` would prune the block of the new value.
        "select id, a from {t} where a = 1006",
        "select id, a from {t} where a > 1000 order by id",
        "select id, a from {t} where a = 6",
        "select id, a from {t} where a = 7",
        // An old Bloom filter of `s` would prune the block of the new value.
        "select id, s from {t} where s = 'new6'",
        "select id, s from {t} where s = 's6'",
        "select id, s from {t} where s = 's7'",
        "select id, c from {t} where c = 'c6'",
        "select count(*), min(a), max(a), min(s), max(s) from {t}",
    ] {
        assert_same(&fixture, sql).await?;
    }
    Ok(())
}

/// An UPDATE of every column, or of a cluster key column, rewrites whole blocks.
#[tokio::test(flavor = "multi_thread")]
async fn test_partial_update_falls_back_to_whole_blocks() -> anyhow::Result<()> {
    let fixture = TestFixture::setup().await?;
    setup(&fixture, "").await?;

    execute_on_both(
        &fixture,
        "update {t} set id = id, a = a + 1, b = 1, s = 'x', c = 'x', n = (1, 'x') where id < 2",
    )
    .await?;
    assert!(
        block_files(&fixture, "t")
            .await?
            .iter()
            .all(|block| block.len() == 1)
    );
    assert_same(&fixture, "select * from {t} order by id").await?;

    execute_on_both(&fixture, "alter table {t} cluster by (b)").await?;
    execute_on_both(&fixture, "update {t} set b = b + 1 where id < 2").await?;
    assert!(
        block_files(&fixture, "t")
            .await?
            .iter()
            .all(|block| block.len() == 1)
    );
    execute_on_both(&fixture, "update {t} set a = a + 1 where id < 2").await?;
    assert!(
        block_files(&fixture, "t")
            .await?
            .iter()
            .any(|block| block.len() == 2)
    );
    assert_same(&fixture, "select * from {t} order by id").await?;
    Ok(())
}

/// A column added after the blocks were written is in no file. A partial UPDATE writes it like
/// any other column.
#[tokio::test(flavor = "multi_thread")]
async fn test_partial_update_of_added_column() -> anyhow::Result<()> {
    let fixture = TestFixture::setup().await?;
    setup(&fixture, "").await?;
    execute_on_both(&fixture, "alter table {t} add column z int default 7").await?;
    execute_on_both(&fixture, "update {t} set z = id where id % 3 = 0").await?;
    assert_newest_groups(&fixture, &["z"]).await?;
    assert_same(&fixture, "select * from {t} order by id").await?;
    assert_same(&fixture, "select id from {t} where z = 7 order by id").await?;
    Ok(())
}

/// With change tracking, a partial UPDATE rewrites the stream columns with the SET columns, as
/// an UPDATE that rewrites whole blocks does.
#[tokio::test(flavor = "multi_thread")]
async fn test_partial_update_with_change_tracking() -> anyhow::Result<()> {
    let fixture = TestFixture::setup().await?;
    setup(&fixture, "change_tracking = true").await?;
    let sql =
        "select id, a, b, _origin_version is null, _origin_block_row_num from {t} order by id";

    execute_on_both(&fixture, "update {t} set a = a + 100 where id % 4 = 1").await?;
    assert_newest_groups(&fixture, &[
        "a",
        "_origin_version",
        "_origin_block_id",
        "_origin_block_row_num",
    ])
    .await?;
    assert_same(&fixture, sql).await?;

    execute_on_both(&fixture, "update {t} set b = 0 where id % 4 = 1").await?;
    assert_same(&fixture, sql).await?;
    Ok(())
}

/// Mutations that rewrite whole blocks read the group files and write legacy blocks.
#[tokio::test(flavor = "multi_thread")]
async fn test_rewrite_blocks_after_partial_update() -> anyhow::Result<()> {
    let fixture = TestFixture::setup().await?;
    setup(&fixture, "").await?;
    execute_on_both(
        &fixture,
        "update {t} set a = a + 100, s = 'u' where id % 3 = 0",
    )
    .await?;

    execute_on_both(&fixture, "delete from {t} where id % 4 = 1").await?;
    assert_same(&fixture, "select * from {t} order by id").await?;

    execute_on_both(
        &fixture,
        "alter table {t} set options(row_per_block = 1000)",
    )
    .await?;
    execute_on_both(&fixture, "optimize table {t} compact").await?;
    assert!(
        block_files(&fixture, "t")
            .await?
            .iter()
            .all(|block| block.len() == 1)
    );
    assert_same(&fixture, "select * from {t} order by id").await?;
    Ok(())
}
