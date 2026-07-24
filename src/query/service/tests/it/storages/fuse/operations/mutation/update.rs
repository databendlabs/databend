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

use std::sync::Arc;

use databend_common_storages_fuse::FuseTable;
use databend_common_storages_fuse::io::MetaReaders;
use databend_query::test_kits::*;
use databend_storages_common_cache::LoadParams;
use databend_storages_common_table_meta::meta::BlockHLL;
use databend_storages_common_table_meta::meta::BlockMeta;
use databend_storages_common_table_meta::meta::decode_column_hll;

async fn latest_block_meta(fixture: &TestFixture) -> anyhow::Result<Arc<BlockMeta>> {
    let table = fixture.latest_default_table().await?;
    let fuse_table = FuseTable::try_from_table(table.as_ref())?;
    let snapshot = fuse_table.read_table_snapshot().await?.unwrap();
    let segment_reader =
        MetaReaders::segment_info_reader(fuse_table.get_operator(), table.schema());
    let (segment_location, segment_version) = &snapshot.segments[0];
    let segment = segment_reader
        .read(&LoadParams {
            location: segment_location.clone(),
            len_hint: None,
            ver: *segment_version,
            put_cache: false,
        })
        .await?;
    let blocks = segment.block_metas()?;
    assert_eq!(blocks.len(), 1);
    Ok(blocks[0].clone())
}

async fn latest_block_hll(fixture: &TestFixture) -> anyhow::Result<BlockHLL> {
    let table = fixture.latest_default_table().await?;
    let fuse_table = FuseTable::try_from_table(table.as_ref())?;
    let snapshot = fuse_table.read_table_snapshot().await?.unwrap();
    let segment_reader =
        MetaReaders::segment_info_reader(fuse_table.get_operator(), table.schema());
    let (segment_location, segment_version) = &snapshot.segments[0];
    let segment = segment_reader
        .read(&LoadParams {
            location: segment_location.clone(),
            len_hint: None,
            ver: *segment_version,
            put_cache: false,
        })
        .await?;
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
    let origin = latest_block_meta(&fixture).await?;
    let origin_hll = latest_block_hll(&fixture).await?;

    fixture
        .execute_command("set enable_partial_update = 1")
        .await?;

    fixture
        .execute_command(&format!(
            "update {db}.{table_name} set value = value + 1 where id = 1"
        ))
        .await?;

    let updated = latest_block_meta(&fixture).await?;

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
    let updated_again = latest_block_meta(&fixture).await?;
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
    let fully_rewritten = latest_block_meta(&fixture).await?;
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
        latest_block_meta(&fixture)
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
        latest_block_meta(&fixture)
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
