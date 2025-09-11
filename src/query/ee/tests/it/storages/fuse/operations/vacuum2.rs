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

use std::path::Path;

use databend_common_base::base::tokio;
use databend_common_exception::ErrorCode;
use databend_common_exception::Result;
use databend_enterprise_query::test_kits::context::EESetup;
use databend_query::sessions::QueryContext;
use databend_query::sessions::TableContext;
use databend_query::test_kits::TestFixture;

// TODO investigate this
// NOTE: SHOULD specify flavor = "multi_thread", otherwise query execution might be hanged
#[tokio::test(flavor = "multi_thread")]
async fn test_vacuum2_all() -> Result<()> {
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
