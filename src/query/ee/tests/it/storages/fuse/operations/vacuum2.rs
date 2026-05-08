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

use std::ffi::OsStr;
use std::path::Path;
use std::path::PathBuf;

use databend_common_exception::ErrorCode;
use databend_common_exception::Result;
use databend_enterprise_query::test_kits::context::EESetup;
use databend_query::sessions::QueryContext;
use databend_query::sessions::TableContextTableAccess;
use databend_query::test_kits::TestFixture;

async fn table_storage_path(
    ctx: &QueryContext,
    storage_root: &str,
    db_name: &str,
    tbl_name: &str,
) -> Result<PathBuf> {
    let tenant = ctx.get_tenant();
    let table = ctx
        .get_default_catalog()?
        .get_table(&tenant, db_name, tbl_name)
        .await?;

    let db = ctx
        .get_default_catalog()?
        .get_database(&tenant, db_name)
        .await?;

    Ok(Path::new(storage_root)
        .join(db.get_db_info().database_id.db_id.to_string())
        .join(table.get_id().to_string()))
}

fn count_files_under_component(path: &Path, component: &str) -> usize {
    let component = OsStr::new(component);
    walkdir::WalkDir::new(path)
        .into_iter()
        .filter_map(|entry| entry.ok())
        .filter(|entry| entry.file_type().is_file())
        .filter(|entry| {
            entry
                .path()
                .components()
                .any(|path_component| path_component.as_os_str() == component)
        })
        .count()
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
        let path = table_storage_path(ctx, storage_root, db_name, tbl_name).await?;

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
async fn test_vacuum2_removes_vector_and_spatial_indexes() -> anyhow::Result<()> {
    let fixture = TestFixture::setup_with_custom(EESetup::new()).await?;
    let session = fixture.default_session();
    session.get_settings().set_data_retention_time_in_days(0)?;
    let ctx = fixture.new_query_ctx().await?;

    fixture
        .execute_command(
            "create table default.vacuum2_index_cleanup(
                id int,
                embedding vector(4),
                geom geometry,
                vector index v_idx(embedding) distance = 'cosine',
                spatial index s_idx(geom)
            )",
        )
        .await?;
    fixture
        .execute_command(
            "insert into default.vacuum2_index_cleanup values
            (1, [0.1, 0.2, 0.3, 0.4], to_geometry('POINT(1 1)'))",
        )
        .await?;
    fixture
        .execute_command(
            "insert into default.vacuum2_index_cleanup values
            (2, [0.2, 0.3, 0.4, 0.5], to_geometry('POINT(2 2)'))",
        )
        .await?;
    fixture
        .execute_command("optimize table default.vacuum2_index_cleanup compact")
        .await?;

    let path = table_storage_path(
        &ctx,
        fixture.storage_root(),
        "default",
        "vacuum2_index_cleanup",
    )
    .await?;
    let vector_indexes_before = count_files_under_component(&path, "_i_v");
    let spatial_indexes_before = count_files_under_component(&path, "_i_s");
    assert!(vector_indexes_before > 1);
    assert!(spatial_indexes_before > 1);

    fixture
        .execute_command("call system$fuse_vacuum2('default', 'vacuum2_index_cleanup')")
        .await?;

    assert_eq!(count_files_under_component(&path, "_i_v"), 1);
    assert_eq!(count_files_under_component(&path, "_i_s"), 1);

    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn test_vacuum2_removes_snapshot_statistics() -> anyhow::Result<()> {
    let fixture = TestFixture::setup_with_custom(EESetup::new()).await?;
    let session = fixture.default_session();
    session.get_settings().set_data_retention_time_in_days(0)?;
    let ctx = fixture.new_query_ctx().await?;

    fixture
        .execute_command("set enable_table_snapshot_stats = 1")
        .await?;
    fixture
        .execute_command("create table default.vacuum2_snapshot_stats(a int)")
        .await?;
    fixture
        .execute_command("insert into default.vacuum2_snapshot_stats values (1), (2)")
        .await?;
    fixture
        .execute_command("analyze table default.vacuum2_snapshot_stats")
        .await?;
    fixture
        .execute_command("insert into default.vacuum2_snapshot_stats values (3)")
        .await?;
    fixture
        .execute_command("analyze table default.vacuum2_snapshot_stats")
        .await?;

    let path = table_storage_path(
        &ctx,
        fixture.storage_root(),
        "default",
        "vacuum2_snapshot_stats",
    )
    .await?;
    let stats_before = count_files_under_component(&path, "_ts");
    assert!(stats_before > 1);

    fixture
        .execute_command("call system$fuse_vacuum2('default', 'vacuum2_snapshot_stats')")
        .await?;

    assert_eq!(count_files_under_component(&path, "_ts"), 1);

    Ok(())
}
