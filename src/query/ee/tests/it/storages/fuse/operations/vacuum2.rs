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

use std::path::Path;
use std::sync::Arc;

use databend_common_exception::ErrorCode;
use databend_common_exception::Result;
use databend_common_storages_fuse::FuseTable;
use databend_common_storages_fuse::io::MetaReaders;
use databend_enterprise_query::test_kits::context::EESetup;
use databend_query::sessions::QueryContext;
use databend_query::sessions::TableContextTableAccess;
use databend_query::test_kits::TestFixture;
use databend_storages_common_cache::LoadParams;
use databend_storages_common_io::dedup_file_locations;
use databend_storages_common_table_meta::meta::BlockMeta;

async fn latest_default_block_meta(fixture: &TestFixture) -> anyhow::Result<Arc<BlockMeta>> {
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

// TODO investigate this
// NOTE: SHOULD specify flavor = "multi_thread", otherwise query execution might be hanged
#[tokio::test(flavor = "multi_thread")]
async fn test_vacuum2_all() -> anyhow::Result<()> {
    let ee_setup = EESetup::new();
    let fixture = TestFixture::setup_with_custom(ee_setup).await?;
    // Adjust retention period to 0, so that dropped tables will be vacuumed immediately
    let session = fixture.default_session();
    session.get_settings().set_data_retention_time_in_days(0)?;

    let ctx = fixture.new_query_ctx().await?;

    let setup_statements = vec![
        // create non-system db1, create fuse and non-fuse table in it.
        "create database db1",
        "create table db1.t1 (c int) as select 1",
        "insert into db1.t1 values (1)",
        "truncate table db1.t1",
        "create table db1.t2 (c int) engine = memory as select 1",
        "truncate table db1.t2",
        // create fuse and non-fuse tables in default db
        "create table default.t1 (c int) as select 1",
        "insert into default.t1 values (1)",
        "truncate table default.t1",
        "create table default.t2 (c int) engine = memory as select 1",
        "truncate table default.t2",
    ];

    for stmt in setup_statements {
        fixture.execute_command(stmt).await?;
    }

    // vacuum them all
    let res = fixture.execute_command("call system$fuse_vacuum2()").await;

    // Check that:

    // 1. non-fuse tables should not stop us

    assert!(res.is_ok());

    //  2. fuse table data should be vacuumed

    let storage_root = fixture.storage_root();

    async fn check_files_left(
        ctx: &QueryContext,
        storage_root: &str,
        db_name: &str,
        tbl_name: &str,
    ) -> Result<()> {
        let tenant = ctx.get_tenant();
        let table = ctx
            .get_default_catalog()?
            .get_table(&tenant, db_name, tbl_name)
            .await?;

        let db = ctx
            .get_default_catalog()?
            .get_database(&tenant, db_name)
            .await?;

        let path = Path::new(storage_root)
            .join(db.get_db_info().database_id.db_id.to_string())
            .join(table.get_id().to_string());

        let walker = walkdir::WalkDir::new(path).into_iter();

        let mut files_left = Vec::new();
        for entry in walker {
            let entry = entry.unwrap();
            if entry.file_type().is_file() {
                files_left.push(entry);
            }
        }

        // There should be one snapshot file and one snapshot hint file left
        assert_eq!(files_left.len(), 2);

        files_left.sort_by(|a, b| a.file_name().cmp(b.file_name()));
        // First is the only snapshot left
        files_left[0].path().to_string_lossy().contains("/_ss/");
        // Second one is the last snapshot location hint
        files_left[1]
            .path()
            .to_string_lossy()
            .contains("last_snapshot_location_hint_v2");
        Ok::<(), ErrorCode>(())
    }

    check_files_left(&ctx, storage_root, "db1", "t1").await?;
    check_files_left(&ctx, storage_root, "default", "t1").await?;

    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn test_vacuum2_preserves_active_partial_update_files() -> anyhow::Result<()> {
    let fixture = TestFixture::setup_with_custom(EESetup::new()).await?;
    fixture
        .default_session()
        .get_settings()
        .set_data_retention_time_in_days(0)?;
    let db = fixture.default_db_name();
    let table_name = fixture.default_table_name();

    fixture.create_default_database().await?;
    fixture
        .execute_command(&format!(
            "create table {db}.{table_name} (id int, a int, b int) engine=fuse \
             bloom_index_columns='id,a,b' enable_partial_update=true"
        ))
        .await?;
    fixture
        .execute_command(&format!(
            "insert into {db}.{table_name} values (1, 10, 20), (2, 30, 40)"
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
    let obsolete_group = latest_default_block_meta(&fixture)
        .await?
        .location
        .0
        .clone();

    fixture
        .execute_command(&format!(
            "update {db}.{table_name} set a = a + 1 where id = 1"
        ))
        .await?;
    fixture
        .execute_command(&format!(
            "update {db}.{table_name} set b = b + 1 where id = 1"
        ))
        .await?;

    let current = latest_default_block_meta(&fixture).await?;
    assert!(
        current
            .column_groups
            .iter()
            .all(|group| group.location.0 != obsolete_group)
    );

    fixture
        .execute_command(&format!("call system$fuse_vacuum2('{db}', '{table_name}')"))
        .await?;

    let table = fixture.latest_default_table().await?;
    let fuse_table = FuseTable::try_from_table(table.as_ref())?;
    let operator = fuse_table.get_operator();
    for group in &current.column_groups {
        operator.stat(&group.location.0).await?;
    }
    for file in &current.bloom_index_files {
        operator.stat(&file.location.0).await?;
    }
    assert!(operator.stat(&obsolete_group).await.is_err());

    let rows = fixture
        .execute_query(&format!(
            "select count(*) from {db}.{table_name} \
             where (id = 1 and a = 12 and b = 21) \
                or (id = 2 and a = 30 and b = 40)"
        ))
        .await?;
    assert_eq!(databend_query::test_kits::query_count(rows).await?, 2);

    Ok(())
}

/// Verifies that dedup_file_locations correctly removes duplicates and reports samples.
#[test]
fn test_dedup_file_locations() {
    // Simulate the vacuum2 scenario: bloom index paths generated from blocks
    // with and without the 'h' prefix map to the same location.
    let mut locations = vec![
        "548052/604310/_i_b_v2/019bdabd0292702a96f51f3b3ea64335_v4.parquet".to_string(),
        "548052/604310/_i_b_v2/019bdabd02927061af2ed18c2562b79e_v4.parquet".to_string(),
        "548052/604310/_i_b_v2/019c0df29e8a7016a6a1be466e094ec3_v4.parquet".to_string(),
        // Duplicates (same paths appearing again)
        "548052/604310/_i_b_v2/019bdabd0292702a96f51f3b3ea64335_v4.parquet".to_string(),
        "548052/604310/_i_b_v2/019bdabd02927061af2ed18c2562b79e_v4.parquet".to_string(),
    ];

    let (duplicates, samples) = dedup_file_locations(&mut locations);

    assert_eq!(duplicates, 2);
    assert_eq!(locations.len(), 3);
    assert_eq!(samples.len(), 2);
    assert_eq!(
        samples[0],
        "548052/604310/_i_b_v2/019bdabd0292702a96f51f3b3ea64335_v4.parquet"
    );
    assert_eq!(
        samples[1],
        "548052/604310/_i_b_v2/019bdabd02927061af2ed18c2562b79e_v4.parquet"
    );
}

#[test]
fn test_dedup_file_locations_no_duplicates() {
    let mut locations = vec![
        "a/b/file1.parquet".to_string(),
        "a/b/file2.parquet".to_string(),
        "a/b/file3.parquet".to_string(),
    ];

    let (duplicates, samples) = dedup_file_locations(&mut locations);

    assert_eq!(duplicates, 0);
    assert_eq!(locations.len(), 3);
    assert!(samples.is_empty());
}
