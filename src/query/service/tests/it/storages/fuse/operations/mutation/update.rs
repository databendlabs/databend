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

use databend_common_storages_fuse::FuseTable;
use databend_common_storages_fuse::io::MetaReaders;
use databend_query::test_kits::*;
use databend_storages_common_cache::LoadParams;
use databend_storages_common_table_meta::meta::BlockHLL;
use databend_storages_common_table_meta::meta::decode_column_hll;

async fn latest_block_hll(fixture: &TestFixture) -> anyhow::Result<BlockHLL> {
    let table = fixture.latest_default_table().await?;
    let fuse_table = FuseTable::try_from_table(table.as_ref())?;
    let segment = latest_default_segment(fixture).await?;
    let (stats_location, stats_version) = segment.summary.additional_stats_loc().unwrap();
    let stats = MetaReaders::segment_stats_reader(fuse_table.get_operator())
        .read(&LoadParams {
            location: stats_location,
            len_hint: None,
            ver: stats_version,
            put_cache: false,
        })
        .await?;
    Ok(decode_column_hll(&stats.block_hlls[0])?.unwrap())
}

#[tokio::test(flavor = "multi_thread")]
async fn test_update_writes_changed_column_group() -> anyhow::Result<()> {
    let fixture = TestFixture::setup().await?;
    let db = fixture.default_db_name();
    let table_name = fixture.default_table_name();

    fixture.create_default_database().await?;
    fixture
        .execute_command(&format!(
            "create table {db}.{table_name} (id int, value int) engine=fuse \
             bloom_index_columns='id,value' approx_distinct_columns='id,value' \
             enable_partial_update=true change_tracking=true"
        ))
        .await?;
    fixture
        .execute_command(&format!(
            "insert into {db}.{table_name} values (1, 10), (2, 20)"
        ))
        .await?;

    let table = fixture.latest_default_table().await?;
    let schema = table.schema();
    let id_column_id = schema.field(0).column_id();
    let value_column_id = schema.field(1).column_id();
    let origin = latest_default_block_meta(&fixture).await?;
    let origin_hll = latest_block_hll(&fixture).await?;

    fixture
        .execute_command("set enable_partial_update = 1")
        .await?;

    fixture
        .execute_command(&format!(
            "update {db}.{table_name} set value = value + 1 where id = 1"
        ))
        .await?;

    let updated = latest_default_block_meta(&fixture).await?;

    assert_eq!(updated.column_groups.len(), 2);
    let unchanged_group = updated
        .column_groups
        .iter()
        .find(|group| group.location == origin.location)
        .unwrap();
    assert_eq!(unchanged_group.active_column_ids, vec![id_column_id]);
    let changed_group = updated
        .column_groups
        .iter()
        .find(|group| group.location == updated.location)
        .unwrap();
    assert!(changed_group.active_column_ids.contains(&value_column_id));
    assert_eq!(changed_group.active_column_ids.len(), 4);
    assert_eq!(
        updated.col_stats.get(&id_column_id),
        origin.col_stats.get(&id_column_id)
    );
    assert!(updated.bloom_filter_index_location.is_none());
    assert_eq!(updated.bloom_index_files.len(), 2);
    assert_eq!(
        updated.bloom_filter_index_size,
        updated
            .bloom_index_files
            .iter()
            .map(|file| file.file_size)
            .sum::<u64>()
    );
    let updated_hll = latest_block_hll(&fixture).await?;
    assert_eq!(
        updated_hll.get(&id_column_id),
        origin_hll.get(&id_column_id)
    );
    assert_ne!(
        updated_hll.get(&value_column_id),
        origin_hll.get(&value_column_id)
    );

    let rows = fixture
        .execute_query(&format!(
            "select count(distinct column_name) from fuse_page('{db}', '{table_name}') \
             where column_name in ('id', 'value')"
        ))
        .await?;
    assert_eq!(query_count(rows).await?, 2);
    let rows = fixture
        .execute_query(&format!(
            "select count(distinct block_location) from fuse_column('{db}', '{table_name}') \
             where column_name in ('id', 'value')"
        ))
        .await?;
    assert_eq!(query_count(rows).await?, 2);
    let rows = fixture
        .execute_query(&format!(
            "select count(distinct column_name) from fuse_encoding('{db}', '{table_name}') \
             where column_name in ('id', 'value')"
        ))
        .await?;
    assert_eq!(query_count(rows).await?, 2);

    fixture
        .execute_command(&format!(
            "alter table {db}.{table_name} set options(bloom_index_columns='id')"
        ))
        .await?;

    fixture
        .execute_command(&format!(
            "update {db}.{table_name} set value = value + 1 where id = 2"
        ))
        .await?;
    let updated_again = latest_default_block_meta(&fixture).await?;
    assert_eq!(updated_again.column_groups.len(), 2);
    assert!(
        updated_again
            .column_groups
            .iter()
            .any(|group| group.location == origin.location)
    );
    assert!(
        updated_again
            .column_groups
            .iter()
            .all(|group| group.location != updated.location)
    );
    assert!(updated_again.bloom_filter_index_location.is_none());
    assert_eq!(updated_again.bloom_index_files.len(), 1);

    let rows = fixture
        .execute_query(&format!(
            "select count(*) from {db}.{table_name} where value = 21"
        ))
        .await?;
    assert_eq!(query_count(rows).await?, 1);

    let rows = fixture
        .execute_query(&format!(
            "select count(*) from {db}.{table_name} \
             where (id = 1 and value = 11) or (id = 2 and value = 21)"
        ))
        .await?;
    assert_eq!(query_count(rows).await?, 2);

    fixture
        .execute_command("set enable_partial_update = 0")
        .await?;
    fixture
        .execute_command(&format!(
            "update {db}.{table_name} set value = value + 1 where id = 1"
        ))
        .await?;
    let fully_rewritten = latest_default_block_meta(&fixture).await?;
    assert!(fully_rewritten.column_groups.is_empty());
    assert!(fully_rewritten.bloom_index_files.is_empty());

    let rows = fixture
        .execute_query(&format!(
            "select count(*) from {db}.{table_name} \
             where (id = 1 and value = 12) or (id = 2 and value = 21)"
        ))
        .await?;
    assert_eq!(query_count(rows).await?, 2);

    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn test_partial_update_preserves_block_compression() -> anyhow::Result<()> {
    let fixture = TestFixture::setup().await?;
    let db = fixture.default_db_name();
    let table_name = fixture.default_table_name();

    fixture.create_default_database().await?;
    fixture
        .execute_command(&format!(
            "create table {db}.{table_name} (id int, a int, b int) engine=fuse \
             compression='zstd' enable_partial_update=true"
        ))
        .await?;
    fixture
        .execute_command(&format!(
            "insert into {db}.{table_name} values (1, 10, 20), (2, 30, 40)"
        ))
        .await?;
    let origin_compression = latest_default_block_meta(&fixture).await?.compression;

    fixture
        .execute_command(&format!(
            "alter table {db}.{table_name} set options(compression='snappy')"
        ))
        .await?;
    fixture
        .execute_command("set enable_partial_update = 1")
        .await?;
    fixture
        .execute_command(&format!(
            "update {db}.{table_name} set a = a + 1 where id = 1"
        ))
        .await?;
    fixture
        .execute_command(&format!(
            "update {db}.{table_name} set b = b + 1 where id = 2"
        ))
        .await?;

    let updated = latest_default_block_meta(&fixture).await?;
    assert_eq!(updated.compression, origin_compression);
    assert_eq!(updated.column_groups.len(), 3);
    let rows = fixture
        .execute_query(&format!(
            "select count(*) from {db}.{table_name} \
             where (id = 1 and a = 11 and b = 20) \
                or (id = 2 and a = 30 and b = 41)"
        ))
        .await?;
    assert_eq!(query_count(rows).await?, 2);

    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn test_partial_update_reads_legacy_bloom_schema() -> anyhow::Result<()> {
    let fixture = TestFixture::setup().await?;
    let db = fixture.default_db_name();
    let table_name = fixture.default_table_name();

    fixture.create_default_database().await?;
    fixture
        .execute_command(&format!(
            "create table {db}.{table_name} (id int, value int, other int) engine=fuse \
             bloom_index_columns='id' enable_partial_update=true"
        ))
        .await?;
    fixture
        .execute_command(&format!(
            "insert into {db}.{table_name} values (1, 10, 20), (2, 30, 40)"
        ))
        .await?;
    let table = fixture.latest_default_table().await?;
    let id_column_id = table.schema().field_with_name("id")?.column_id();

    fixture
        .execute_command(&format!(
            "alter table {db}.{table_name} set options(bloom_index_columns='value')"
        ))
        .await?;
    fixture
        .execute_command("set enable_partial_update = 1")
        .await?;
    fixture
        .execute_command(&format!(
            "update {db}.{table_name} set other = other + 1 where id = 1"
        ))
        .await?;

    let updated = latest_default_block_meta(&fixture).await?;
    assert!(updated.bloom_filter_index_location.is_none());
    assert_eq!(updated.bloom_index_files.len(), 1);
    assert_eq!(updated.bloom_index_files[0].active_column_ids, vec![
        id_column_id
    ]);

    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn test_partial_update_drops_obsolete_split_bloom_file_after_drop_column()
-> anyhow::Result<()> {
    let fixture = TestFixture::setup().await?;
    let db = fixture.default_db_name();
    let table_name = fixture.default_table_name();

    fixture.create_default_database().await?;
    fixture
        .execute_command(&format!(
            "create table {db}.{table_name} (id int, value int, flag boolean) engine=fuse \
             bloom_index_columns='id,value' enable_partial_update=true"
        ))
        .await?;
    fixture
        .execute_command(&format!(
            "insert into {db}.{table_name} values (1, 10, true), (2, 20, false)"
        ))
        .await?;
    fixture
        .execute_command("set enable_partial_update = 1")
        .await?;
    fixture
        .execute_command(&format!(
            "update {db}.{table_name} set value = value + 1 where id = 1"
        ))
        .await?;

    let table = fixture.latest_default_table().await?;
    let value_column_id = table.schema().field_with_name("value")?.column_id();
    let split_meta = latest_default_block_meta(&fixture).await?;
    let obsolete_bloom_location = split_meta
        .bloom_index_files
        .iter()
        .find(|file| file.active_column_ids.contains(&value_column_id))
        .unwrap()
        .location
        .clone();

    fixture
        .execute_command(&format!("alter table {db}.{table_name} drop column value"))
        .await?;
    // Boolean is not supported by the ordinary Bloom filter. This exercises metadata cleanup
    // even when the current UPDATE neither invalidates nor writes a Bloom filter.
    fixture
        .execute_command(&format!(
            "update {db}.{table_name} set flag = not flag where id = 1"
        ))
        .await?;

    let updated = latest_default_block_meta(&fixture).await?;
    assert!(
        updated
            .bloom_index_files
            .iter()
            .all(|file| file.location != obsolete_bloom_location
                && !file.active_column_ids.contains(&value_column_id))
    );
    assert_eq!(
        updated.bloom_filter_index_size,
        updated
            .bloom_index_files
            .iter()
            .map(|file| file.file_size)
            .sum::<u64>()
    );
    let rows = fixture
        .execute_query(&format!(
            "select count(*) from {db}.{table_name} \
             where (id = 1 and not flag) or (id = 2 and not flag)"
        ))
        .await?;
    assert_eq!(query_count(rows).await?, 2);

    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn test_partial_update_ignores_missing_legacy_bloom_file() -> anyhow::Result<()> {
    let fixture = TestFixture::setup().await?;
    let db = fixture.default_db_name();
    let table_name = fixture.default_table_name();

    fixture.create_default_database().await?;
    fixture
        .execute_command(&format!(
            "create table {db}.{table_name} (id int, value int, flag boolean) engine=fuse \
             bloom_index_columns='id,value' enable_partial_update=true"
        ))
        .await?;
    fixture
        .execute_command(&format!(
            "insert into {db}.{table_name} values (1, 10, true), (2, 20, false)"
        ))
        .await?;

    let table = fixture.latest_default_table().await?;
    let value_column_id = table.schema().field_with_name("value")?.column_id();
    let fuse_table = FuseTable::try_from_table(table.as_ref())?;
    let operator = fuse_table.get_operator();
    let origin = latest_default_block_meta(&fixture).await?;
    let missing_location = origin.bloom_filter_index_location.as_ref().unwrap().clone();
    operator.delete(&missing_location.0).await?;

    fixture
        .execute_command("set enable_partial_update = 1")
        .await?;
    fixture
        .execute_command(&format!(
            "update {db}.{table_name} set value = value + 1 where id = 1"
        ))
        .await?;

    let updated = latest_default_block_meta(&fixture).await?;
    assert!(updated.bloom_filter_index_location.is_none());
    assert_eq!(updated.bloom_index_files.len(), 1);
    assert_eq!(updated.bloom_index_files[0].active_column_ids, vec![
        value_column_id
    ]);
    assert_ne!(updated.bloom_index_files[0].location, missing_location);
    let rows = fixture
        .execute_query(&format!(
            "select count(*) from {db}.{table_name} \
             where (id = 1 and value = 11 and flag) \
                or (id = 2 and value = 20 and not flag)"
        ))
        .await?;
    assert_eq!(query_count(rows).await?, 2);

    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn test_auto_fix_missing_split_bloom_index() -> anyhow::Result<()> {
    let fixture = TestFixture::setup().await?;
    let db = fixture.default_db_name();
    let table_name = fixture.default_table_name();

    fixture.create_default_database().await?;
    fixture
        .execute_command(&format!(
            "create table {db}.{table_name} (id int, value int) engine=fuse \
             bloom_index_columns='id,value' enable_partial_update=true"
        ))
        .await?;
    fixture
        .execute_command(&format!(
            "insert into {db}.{table_name} values (1, 10), (3, 30)"
        ))
        .await?;
    fixture
        .execute_command("set enable_partial_update = 1")
        .await?;
    fixture
        .execute_command(&format!(
            "update {db}.{table_name} set value = value + 1 where id = 1"
        ))
        .await?;

    let table = fixture.latest_default_table().await?;
    let fuse_table = FuseTable::try_from_table(table.as_ref())?;
    let operator = fuse_table.get_operator();
    let value_column_id = table.schema().field_with_name("value")?.column_id();
    let block_meta = latest_default_block_meta(&fixture).await?;
    assert_eq!(block_meta.bloom_index_files.len(), 2);
    let missing_file = block_meta
        .bloom_index_files
        .iter()
        .find(|file| file.active_column_ids.contains(&value_column_id))
        .unwrap();
    let existing_file = block_meta
        .bloom_index_files
        .iter()
        .find(|file| file.location != missing_file.location)
        .unwrap();
    let existing_data = operator.read(&existing_file.location.0).await?.to_bytes();
    operator.delete(&missing_file.location.0).await?;
    assert!(!operator.exists(&missing_file.location.0).await?);

    fixture
        .execute_command("set enable_auto_fix_missing_bloom_index = 1")
        .await?;
    let rows = fixture
        .execute_query(&format!(
            "select count(*) from {db}.{table_name} where value = 20"
        ))
        .await?;
    assert_eq!(query_count(rows).await?, 0);
    assert!(operator.exists(&missing_file.location.0).await?);
    assert_eq!(
        operator.read(&existing_file.location.0).await?.to_bytes(),
        existing_data
    );

    let rows = fixture
        .execute_query(&format!(
            "select count(*) from {db}.{table_name} where id = 1 and value = 11"
        ))
        .await?;
    assert_eq!(query_count(rows).await?, 1);

    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn test_auto_fix_missing_legacy_bloom_index_from_split_data() -> anyhow::Result<()> {
    let fixture = TestFixture::setup().await?;
    let db = fixture.default_db_name();
    let table_name = fixture.default_table_name();

    fixture.create_default_database().await?;
    fixture
        .execute_command(&format!(
            "create table {db}.{table_name} (id int, flag boolean) engine=fuse \
             bloom_index_columns='id' enable_partial_update=true"
        ))
        .await?;
    fixture
        .execute_command(&format!(
            "insert into {db}.{table_name} values (1, true), (3, false)"
        ))
        .await?;
    fixture
        .execute_command("set enable_partial_update = 1")
        .await?;
    fixture
        .execute_command(&format!(
            "update {db}.{table_name} set flag = false where flag"
        ))
        .await?;

    let block_meta = latest_default_block_meta(&fixture).await?;
    assert_eq!(block_meta.column_groups.len(), 2);
    assert!(block_meta.bloom_index_files.is_empty());
    let bloom_location = block_meta.bloom_filter_index_location.as_ref().unwrap();
    let table = fixture.latest_default_table().await?;
    let fuse_table = FuseTable::try_from_table(table.as_ref())?;
    let operator = fuse_table.get_operator();
    operator.delete(&bloom_location.0).await?;
    assert!(!operator.exists(&bloom_location.0).await?);

    fixture
        .execute_command("set enable_auto_fix_missing_bloom_index = 1")
        .await?;
    let rows = fixture
        .execute_query(&format!(
            "select count(*) from {db}.{table_name} where id = 2"
        ))
        .await?;
    assert_eq!(query_count(rows).await?, 0);
    assert!(operator.exists(&bloom_location.0).await?);

    let rows = fixture
        .execute_query(&format!(
            "select count(*) from {db}.{table_name} where id = 1 and not flag"
        ))
        .await?;
    assert_eq!(query_count(rows).await?, 1);

    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn test_partial_update_invalidates_virtual_columns_when_disabled() -> anyhow::Result<()> {
    let fixture = TestFixture::setup().await?;
    let db = fixture.default_db_name();
    let table_name = fixture.default_table_name();

    fixture.create_default_database().await?;
    fixture
        .execute_command(&format!(
            "create table {db}.{table_name} (id int, payload variant) engine=fuse \
             enable_partial_update=true enable_virtual_column=true"
        ))
        .await?;
    fixture
        .execute_command(&format!(
            "insert into {db}.{table_name} values \
             (1, parse_json('{{\"x\": 1}}')), (2, parse_json('{{\"x\": 2}}'))"
        ))
        .await?;
    assert!(
        latest_default_block_meta(&fixture)
            .await?
            .virtual_block_meta
            .is_some()
    );

    fixture
        .execute_command(&format!(
            "alter table {db}.{table_name} set options(enable_virtual_column=false)"
        ))
        .await?;
    fixture
        .execute_command("set enable_partial_update = 1")
        .await?;
    fixture
        .execute_command(&format!(
            "update {db}.{table_name} set payload = parse_json('{{\"x\": 3}}') where id = 1"
        ))
        .await?;

    assert!(
        latest_default_block_meta(&fixture)
            .await?
            .virtual_block_meta
            .is_none()
    );
    let rows = fixture
        .execute_query(&format!(
            "select count(*) from {db}.{table_name} where payload:x::int in (2, 3)"
        ))
        .await?;
    assert_eq!(query_count(rows).await?, 2);

    Ok(())
}
