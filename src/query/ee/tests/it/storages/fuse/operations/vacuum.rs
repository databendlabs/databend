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

use std::fmt::Debug;
use std::path::Path;
use std::sync::Arc;
use std::time::Duration;

use databend_common_base::base::tokio;
use databend_common_catalog::table_context::CheckAbort;
use databend_common_config::MetaConfig;
use databend_common_exception::ErrorCode;
use databend_common_exception::Result;
use databend_common_meta_api::get_u64_value;
use databend_common_meta_api::kv_pb_api::KVPbApi;
use databend_common_meta_api::send_txn;
use databend_common_meta_api::txn_core_util::txn_replace_exact;
use databend_common_meta_app::principal::AutoIncrementKey;
use databend_common_meta_app::principal::OwnershipObject;
use databend_common_meta_app::principal::TenantOwnershipObjectIdent;
use databend_common_meta_app::schema::AutoIncrementStorageIdent;
use databend_common_meta_app::schema::DBIdTableName;
use databend_common_meta_app::schema::DatabaseId;
use databend_common_meta_app::schema::TableId;
use databend_common_meta_kvapi::kvapi::KvApiExt;
use databend_common_meta_store::MetaStore;
use databend_common_meta_store::MetaStoreProvider;
use databend_common_meta_types::TxnRequest;
use databend_common_storage::DataOperator;
use databend_common_storages_fuse::TableContext;
use databend_common_version::BUILD_INFO;
use databend_enterprise_query::storages::fuse::operations::vacuum_temporary_files::do_vacuum_temporary_files;
use databend_enterprise_query::test_kits::context::EESetup;
use databend_enterprise_vacuum_handler::vacuum_handler::VacuumTempOptions;
use databend_query::test_kits::*;
use databend_storages_common_io::Files;
use databend_storages_common_table_meta::meta::parse_storage_prefix;
use opendal::raw::Access;
use opendal::raw::AccessorInfo;
use opendal::raw::OpStat;
use opendal::raw::RpStat;
use opendal::EntryMode;
use opendal::Metadata;
use opendal::OperatorBuilder;
use tempfile::TempDir;

#[tokio::test(flavor = "multi_thread")]
async fn test_fuse_do_vacuum_drop_tables() -> Result<()> {
    let ee_setup = EESetup::new();
    let fixture = TestFixture::setup_with_custom(ee_setup).await?;

    fixture
        .execute_command("create database test_vacuum")
        .await?;

    fixture
        .execute_command("create table test_vacuum.t (c int) as select * from numbers(100)")
        .await?;

    check_data_dir(
        &fixture,
        "test_fuse_do_vacuum_drop_table: verify generate files",
        1,
        0,
        1,
        1,
        1,
        1,
        None,
        None,
    )
    .await?;

    fixture.execute_command("drop table test_vacuum.t").await?;

    // verify dry run never delete files
    {
        fixture
            .execute_command("vacuum drop table from test_vacuum dry run")
            .await?;
        fixture.execute_command("vacuum drop table dry run").await?;
        check_data_dir(
            &fixture,
            "test_fuse_do_vacuum_drop_table: verify generate files",
            1,
            0,
            1,
            1,
            1,
            1,
            None,
            None,
        )
        .await?;
    }

    {
        fixture
            .execute_command("settings(data_retention_time_in_days = 0) vacuum drop table")
            .await?;

        // after vacuum drop tables, verify the files number
        check_data_dir(
            &fixture,
            "test_fuse_do_vacuum_drop_table: verify generate retention files",
            0,
            0,
            0,
            0,
            0,
            0,
            None,
            None,
        )
        .await?;
    }

    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn test_do_vacuum_temporary_files() -> Result<()> {
    let _fixture = TestFixture::setup().await?;

    let operator = DataOperator::instance().spill_operator();
    operator.write("test_dir/test1", vec![1, 2]).await?;
    operator.write("test_dir/test2", vec![1, 2]).await?;
    operator.write("test_dir/test3", vec![1, 2]).await?;

    let size = operator.list_with("test_dir/").recursive(true).await?.len();
    assert!((3..=4).contains(&size));

    struct NoAbort;
    struct AbortRightNow;
    impl CheckAbort for NoAbort {
        fn try_check_aborting(&self) -> Result<()> {
            Ok(())
        }
    }

    impl CheckAbort for AbortRightNow {
        fn try_check_aborting(&self) -> Result<()> {
            Err(ErrorCode::AbortedQuery(""))
        }
    }

    // check abort

    let r = do_vacuum_temporary_files(
        Arc::new(AbortRightNow),
        "test_dir/".to_string(),
        &VacuumTempOptions::VacuumCommand(Some(Duration::from_secs(2))),
        1,
    )
    .await;

    assert!(r.is_err_and(|e| e.code() == ErrorCode::ABORTED_QUERY));

    let no_abort = Arc::new(NoAbort);
    tokio::time::sleep(Duration::from_secs(2)).await;
    do_vacuum_temporary_files(
        no_abort.clone(),
        "test_dir/".to_string(),
        &VacuumTempOptions::VacuumCommand(Some(Duration::from_secs(2))),
        1,
    )
    .await?;

    let size = operator.list("test_dir/").await?.len();
    assert!((2..=3).contains(&size));

    operator.write("test_dir/test4/test4", vec![1, 2]).await?;
    operator.write("test_dir/test5/test5", vec![1, 2]).await?;
    operator
        .write("test_dir/test5/finished", vec![1, 2])
        .await?;

    do_vacuum_temporary_files(
        no_abort.clone(),
        "test_dir/".to_string(),
        &VacuumTempOptions::VacuumCommand(Some(Duration::from_secs(2))),
        2,
    )
    .await?;
    let size = operator.list("test_dir/").await?.len();
    assert!((2..=3).contains(&size));

    tokio::time::sleep(Duration::from_secs(3)).await;
    do_vacuum_temporary_files(
        no_abort.clone(),
        "test_dir/".to_string(),
        &VacuumTempOptions::VacuumCommand(Some(Duration::from_secs(3))),
        1000,
    )
    .await?;

    dbg!(operator.list_with("test_dir/").await?);

    let size = operator.list("test_dir/").await?.len();
    assert!((0..=1).contains(&size));

    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn test_fuse_do_vacuum_drop_table_deletion_error() -> Result<()> {
    // **** Primary Scenario ********************************************************************
    // Test vacuum dropped table should keep going if some of the table data can not be removed
    // successfully, e.g. bucket of external tables may under heavy load, can not handle the
    // deletion operations in time.
    // But, the metadata of the table that owns those data, should not be removed.
    // *******************************************************************************************

    let meta = new_local_meta().await;
    let endpoints = meta.endpoints.clone();

    // Modify config to use local meta store
    let mut ee_setup = EESetup::new();
    let config = ee_setup.config_mut();
    config.meta.endpoints = endpoints.clone();

    let fixture = TestFixture::setup_with_custom(ee_setup).await?;

    let session = fixture.default_session();
    session.get_settings().set_data_retention_time_in_days(0)?;

    // Prepare 2 tables with some data
    fixture
        .execute_command("create database test_vacuum")
        .await?;

    fixture
        .execute_command("create table test_vacuum.t1 (c int) as select * from numbers(100)")
        .await?;

    fixture
        .execute_command("create table test_vacuum.t2 (c int) as select * from numbers(100)")
        .await?;

    let ctx = fixture.new_query_ctx().await?;
    let tenant = ctx.get_tenant();
    let cat = ctx.get_default_catalog()?;
    let db = cat.get_database(&tenant, "test_vacuum").await?;

    let t1 = cat.get_table(&tenant, "test_vacuum", "t1").await?;
    let t2 = cat.get_table(&tenant, "test_vacuum", "t2").await?;
    let t1_table_id = t1.get_id();
    let t2_table_id = t2.get_id();
    let db_id = db.get_db_info().database_id.db_id;
    let storage_root = fixture.storage_root();

    // Let make t1's data path unremovable
    {
        let path = Path::new(storage_root)
            .join(db_id.to_string())
            .join(t1_table_id.to_string());

        let mut perms = std::fs::metadata(&path)?.permissions();
        perms.set_readonly(true);
        std::fs::set_permissions(&path, perms)?;
    }

    // Drop tables and vacuum them
    fixture.execute_command("drop table test_vacuum.t1").await?;
    fixture.execute_command("drop table test_vacuum.t2").await?;

    // before vacuum, check that tables' data paths exist

    let path = Path::new(storage_root)
        .join(db_id.to_string())
        .join(t1_table_id.to_string());
    assert!(path.exists());

    let path = Path::new(storage_root)
        .join(db_id.to_string())
        .join(t2_table_id.to_string());
    assert!(path.exists());

    // Expects:
    //
    // - the vacuum drop operation returns Ok
    fixture.execute_command("vacuum drop table").await?;

    // - t2's metadata should be removed, and its metadata should also have be removed

    {
        let path = Path::new(storage_root)
            .join(db_id.to_string())
            .join(t2_table_id.to_string());

        assert!(!path.exists());

        let table_id_key = TableId::new(t2_table_id);
        let t2_table_meta = meta.get_pb(&table_id_key).await?;
        assert!(t2_table_meta.is_none());
    }

    // - but t1's metadata should still be there, since its table data is unable to be removed
    let table_id_key = TableId::new(t1_table_id);
    let t1_table_meta = meta.get_pb(&table_id_key).await?;
    assert!(t1_table_meta.is_some());

    // *********************
    // * Appendix scenario *
    // *********************

    // Let make t1's data path removable
    {
        let path = Path::new(storage_root)
            .join(db_id.to_string())
            .join(t1_table_id.to_string());
        let mut perms = std::fs::metadata(&path)?.permissions();
        #[allow(clippy::permissions_set_readonly_false)]
        perms.set_readonly(false);
        std::fs::set_permissions(&path, perms)?;

        // make sure it still there
        let path = Path::new(storage_root)
            .join(db_id.to_string())
            .join(t1_table_id.to_string());

        assert!(path.exists());
    }

    // vacuum again
    fixture.execute_command("vacuum drop table").await?;

    // Expects:
    // - t1's metadata and table data should be removed

    {
        let path = Path::new(storage_root)
            .join(db_id.to_string())
            .join(t1_table_id.to_string());

        assert!(!path.exists());

        let table_id_key = TableId::new(t1_table_id);
        let t1_table_meta = meta.get_pb(&table_id_key).await?;
        assert!(t1_table_meta.is_none());
    }

    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn test_fuse_do_vacuum_drop_table_external_storage() -> Result<()> {
    // Test vacuum works on external tables
    let meta = new_local_meta().await;
    let endpoints = meta.endpoints.clone();

    // Modify config to use local meta store
    let mut ee_setup = EESetup::new();
    let config = ee_setup.config_mut();
    config.meta.endpoints = endpoints.clone();

    let fixture = TestFixture::setup_with_custom(ee_setup).await?;
    let tmp_dir_for_t1 = TempDir::new().unwrap().keep();
    let tmp_dir_for_t2 = TempDir::new().unwrap().keep();

    let external_location_for_t1 = format!("fs://{}/", tmp_dir_for_t1.to_str().unwrap());
    let external_location_for_t2 = format!("fs://{}/", tmp_dir_for_t2.to_str().unwrap());

    fixture
        .execute_command("create database test_vacuum")
        .await?;

    fixture
        .execute_command(&format!("create table test_vacuum.t1 (c int) '{external_location_for_t1}' as select * from numbers(100)"))
        .await?;

    fixture
        .execute_command(&format!("create table test_vacuum.t2 (c int) '{external_location_for_t2}' as select * from numbers(100)"))
        .await?;

    let ctx = fixture.new_query_ctx().await?;
    let cat = ctx.get_default_catalog()?;
    let tenant = ctx.get_tenant();
    let t1 = cat.get_table(&tenant, "test_vacuum", "t1").await?;
    let t2 = cat.get_table(&tenant, "test_vacuum", "t2").await?;

    fixture.execute_command("drop table test_vacuum.t1").await?;
    fixture.execute_command("drop table test_vacuum.t2").await?;

    let t1_storage_path = {
        let storage_prefix = parse_storage_prefix(&t1.get_table_info().meta.options, t1.get_id())?;
        let mut dir = tmp_dir_for_t1.clone();
        dir.push(storage_prefix);
        dir
    };

    let t2_storage_path = {
        let storage_prefix = parse_storage_prefix(&t2.get_table_info().meta.options, t2.get_id())?;
        let mut dir = tmp_dir_for_t2.clone();
        dir.push(storage_prefix);
        dir
    };

    assert!(t1_storage_path.exists());
    assert!(t2_storage_path.exists());

    fixture
        .execute_command("settings (data_retention_time_in_days = 0) vacuum drop table")
        .await?;

    assert!(!t1_storage_path.exists());
    assert!(!t2_storage_path.exists());

    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn test_vacuum_dropped_table_clean_autoincrement() -> Result<()> {
    // 1. Prepare local meta service
    let meta = new_local_meta().await;
    let endpoints = meta.endpoints.clone();

    // Modify config to use local meta store
    let mut ee_setup = EESetup::new();
    let config = ee_setup.config_mut();
    config.meta.endpoints = endpoints.clone();

    // 2. Setup test fixture by using local meta store
    let fixture = TestFixture::setup_with_custom(ee_setup).await?;

    // Adjust retention period to 0, so that dropped tables will be vacuumed immediately
    let session = fixture.default_session();
    session.get_settings().set_data_retention_time_in_days(0)?;

    // 3. Prepare test db and table
    let ctx = fixture.new_query_ctx().await?;
    let db_name = "test_vacuum_clean_ownership";
    let tbl_name = "t";
    fixture
        .execute_command(format!("create database {db_name}").as_str())
        .await?;
    fixture
        .execute_command(
            format!("create table {db_name}.{tbl_name} (a int autoincrement)").as_str(),
        )
        .await?;

    // 4. Ensure that table auto increment sequence exist right after table is created
    let tenant = ctx.get_tenant();
    let table = ctx
        .get_default_catalog()?
        .get_table(&tenant, db_name, tbl_name)
        .await?;

    let auto_increment_key_0 = AutoIncrementKey::new(table.get_id(), 0);
    let sequence_storage_ident_0 =
        AutoIncrementStorageIdent::new_generic(&tenant, auto_increment_key_0);

    let v = meta.get_pb(&sequence_storage_ident_0).await?;
    assert!(v.is_some());

    // 5. Ensure that table auto increment sequence exist right after table is replace
    fixture
        .execute_command(
            format!("create or replace table {db_name}.{tbl_name} (a int autoincrement, b int autoincrement)").as_str(),
        )
        .await?;
    let table = ctx
        .get_default_catalog()?
        .get_table(&tenant, db_name, tbl_name)
        .await?;

    let auto_increment_key_1 = AutoIncrementKey::new(table.get_id(), 0);
    let sequence_storage_ident_1 =
        AutoIncrementStorageIdent::new_generic(&tenant, auto_increment_key_1);
    let auto_increment_key_2 = AutoIncrementKey::new(table.get_id(), 1);
    let sequence_storage_ident_2 =
        AutoIncrementStorageIdent::new_generic(&tenant, auto_increment_key_2);

    let v = meta.get_pb(&sequence_storage_ident_0).await?;
    assert!(v.is_some());
    let v = meta.get_pb(&sequence_storage_ident_1).await?;
    assert!(v.is_some());
    let v = meta.get_pb(&sequence_storage_ident_2).await?;
    assert!(v.is_some());

    // 6. Ensure that column b auto increment sequence exist right after column b is drop
    fixture
        .execute_command(format!("alter table {db_name}.{tbl_name} drop column b").as_str())
        .await?;
    let v = meta.get_pb(&sequence_storage_ident_2).await?;
    assert!(v.is_some());

    // 7. Drop test table
    fixture
        .execute_command(format!("drop table {db_name}.{tbl_name}").as_str())
        .await?;

    // 8. Vacuum dropped tables
    fixture.execute_command("vacuum drop table").await?;

    // 9. Ensure that table auto increment sequence is cleaned up
    let v = meta.get_pb(&sequence_storage_ident_0).await?;
    assert!(v.is_none());
    let v = meta.get_pb(&sequence_storage_ident_1).await?;
    assert!(v.is_none());
    let v = meta.get_pb(&sequence_storage_ident_2).await?;
    assert!(v.is_none());

    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn test_vacuum_dropped_table_clean_ownership() -> Result<()> {
    // 1. Prepare local meta service
    let meta = new_local_meta().await;
    let endpoints = meta.endpoints.clone();

    // Modify config to use local meta store
    let mut ee_setup = EESetup::new();
    let config = ee_setup.config_mut();
    config.meta.endpoints = endpoints.clone();

    // 2. Setup test fixture by using local meta store
    let fixture = TestFixture::setup_with_custom(ee_setup).await?;

    // Adjust retention period to 0, so that dropped tables will be vacuumed immediately
    let session = fixture.default_session();
    session.get_settings().set_data_retention_time_in_days(0)?;

    // 3. Prepare test db and table
    let ctx = fixture.new_query_ctx().await?;
    let db_name = "test_vacuum_clean_ownership";
    let tbl_name = "t";
    fixture
        .execute_command(format!("create database {db_name}").as_str())
        .await?;
    fixture
        .execute_command(format!("create table {db_name}.{tbl_name} (a int)").as_str())
        .await?;

    // 4. Ensure that table ownership exist right after table is created
    let tenant = ctx.get_tenant();
    let table = ctx
        .get_default_catalog()?
        .get_table(&tenant, db_name, tbl_name)
        .await?;

    let db = ctx
        .get_default_catalog()?
        .get_database(&tenant, db_name)
        .await?;

    let db_id = db.get_db_info().database_id.db_id;
    let catalog_name = fixture.default_catalog_name();
    let table_ownership = OwnershipObject::Table {
        catalog_name: catalog_name.clone(),
        db_id,
        table_id: table.get_id(),
    };
    let table_ownership_key = TenantOwnershipObjectIdent::new(tenant.clone(), table_ownership);
    let v = meta.get_pb(&table_ownership_key).await?;
    assert!(v.is_some());

    // 4. Check that DbIdTableName mapping exists
    let db_id_table_name = DBIdTableName {
        db_id,
        table_name: tbl_name.to_owned(),
    };

    let (seq, _) = get_u64_value(&meta, &db_id_table_name).await?;
    assert!(seq > 0);

    // 5. Drop test database
    fixture
        .execute_command(format!("drop database {db_name}").as_str())
        .await?;

    // 6. Vacuum dropped tables
    fixture.execute_command("vacuum drop table").await?;

    // 7. Ensure that table ownership is cleaned up
    let table_ownership = OwnershipObject::Table {
        catalog_name,
        db_id: db.get_db_info().database_id.db_id,
        table_id: table.get_id(),
    };

    let table_ownership_key = TenantOwnershipObjectIdent::new(tenant, table_ownership);
    let v = meta.get_pb(&table_ownership_key).await?;
    assert!(v.is_none());

    // 8. Check that DbIdTableName mapping is cleaned up
    let (seq, _) = get_u64_value(&meta, &db_id_table_name).await?;
    assert_eq!(seq, 0);

    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn test_fuse_vacuum_drop_tables_dry_run_with_obj_not_found_error() -> Result<()> {
    // During dry run, vacuum should ignore obj not found error

    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn test_gc_in_progress_db_not_undroppable() -> Result<()> {
    // 1. Prepare local meta service
    let meta = new_local_meta().await;
    let endpoints = meta.endpoints.clone();

    // Modify config to use local meta store
    let mut ee_setup = EESetup::new();
    let config = ee_setup.config_mut();
    config.meta.endpoints = endpoints.clone();

    // 2. Setup test fixture by using local meta store
    let fixture = TestFixture::setup_with_custom(ee_setup).await?;

    // Adjust retention period to 0, so that dropped tables will be available for vacuum immediately
    let session = fixture.default_session();
    session.get_settings().set_data_retention_time_in_days(0)?;

    // 3. Prepare test db and table
    let ctx = fixture.new_query_ctx().await?;
    let db_name = "test_vacuum_clean_ownership";
    let tbl_name = "t";
    fixture
        .execute_command(format!("create database {db_name}").as_str())
        .await?;
    fixture
        .execute_command(format!("create table {db_name}.{tbl_name} (a int)").as_str())
        .await?;

    let tenant = ctx.get_tenant();
    let db = ctx
        .get_default_catalog()?
        .get_database(&tenant, db_name)
        .await?;

    let db_id = db.get_db_info().database_id.db_id;

    // Drop the database
    fixture
        .execute_command(format!("drop database {db_name}").as_str())
        .await?;

    // 4. Simulate gc_in_progress

    let db_id_key = DatabaseId { db_id };
    let mut seq_db_meta = meta.get_pb(&db_id_key).await?.unwrap();

    seq_db_meta.gc_in_progress = true;

    let mut txn = TxnRequest::default();
    txn_replace_exact(&mut txn, &db_id_key, seq_db_meta.seq, &seq_db_meta.data).unwrap();
    let (success, _) = send_txn(&meta, txn).await?;

    assert!(success, "update gc_in_progress to true should work");

    // 5. Undrop database should fail with UnknownDatabase error
    let res = fixture
        .execute_command(format!("undrop database {db_name}").as_str())
        .await;
    assert!(res.is_err());
    assert_eq!(res.unwrap_err().code(), ErrorCode::UNKNOWN_DATABASE);

    // Database should be inaccessible
    let res = ctx
        .get_default_catalog()?
        .get_database(&tenant, db_name)
        .await;

    let Err(e) = res else {
        panic!("get_database should fail")
    };
    assert_eq!(e.code(), ErrorCode::UNKNOWN_DATABASE);

    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn test_vacuum_drop_create_or_replace() -> Result<()> {
    // vacuum dropped tables by specific database names
    test_vacuum_drop_create_or_replace_impl(&[
        "vacuum drop table from db1",
        "vacuum drop table from db2",
    ])
    .await?;

    // vacuum dropped tables all
    test_vacuum_drop_create_or_replace_impl(&["vacuum drop table"]).await?;
    Ok(())
}

async fn test_vacuum_drop_create_or_replace_impl(vacuum_stmts: &[&str]) -> Result<()> {
    // Setup
    let meta = new_local_meta().await;
    let endpoints = meta.endpoints.clone();

    let mut ee_setup = EESetup::new();
    let config = ee_setup.config_mut();
    config.meta.endpoints = endpoints.clone();

    let fixture = TestFixture::setup_with_custom(ee_setup).await?;

    // Adjust retention period to 0, so that dropped tables will be available for vacuum immediately
    let session = fixture.default_session();
    session.get_settings().set_data_retention_time_in_days(0)?;

    // Prepare test dbs and tables
    //   - 2 db ids
    //   - 6 table ids
    //   - db2.t1 explicitly dropped

    let sqls = vec![
        "create database db1",
        "create or replace table db1.t1 (a int)",
        "create or replace table db1.t1 (a int) as select 1",
        "create or replace table db1.t1 (a int) as select 2",
        "create database db2",
        "create or replace table db2.t1 (a int) as select 1",
        "create or replace table db2.t1 (a int) as select 2",
        "create or replace table db2.t1 (a int) as select 3",
        "DROP TABLE DB2.T1",
    ];

    for sql in sqls {
        fixture.execute_command(sql).await?;
    }

    // there should be 6 table ids : create or replace table 6 times
    let prefix = "__fd_table_by_id";
    let items = meta.list_kv_collect(prefix).await?;
    assert_eq!(items.len(), 6);

    // there should be ownerships for 2 db ids and 5 table ids, one of the ownership is revoked by drop table
    let prefix = "__fd_object_owners";
    let items = meta.list_kv_collect(prefix).await?;
    assert_eq!(items.len(), 7);

    for sql in vacuum_stmts {
        fixture.execute_command(sql).await?;
    }

    // After vacuum, 1 table ids left
    let prefix = "__fd_table_by_id";
    let items = meta.list_kv_collect(prefix).await?;
    assert_eq!(items.len(), 1);

    let prefix = "__fd_table_id_to_name";
    let items = meta.list_kv_collect(prefix).await?;
    assert_eq!(items.len(), 1);

    // There are ownership objs of  2 dbs and 1 tables
    let prefix = "__fd_object_owners";
    let items = meta.list_kv_collect(prefix).await?;
    assert_eq!(items.len(), 3);

    // db1.t1 should still be accessible
    fixture.execute_command("select * from db1.t1").await?;
    // db2.t1 should not exist
    assert!(fixture
        .execute_command("select * from db2.t1")
        .await
        .is_err());
    Ok(())
}

async fn new_local_meta() -> MetaStore {
    let version = &BUILD_INFO;
    let meta_config = MetaConfig::default();
    let meta = {
        let config = meta_config.to_meta_grpc_client_conf(version);
        let provider = Arc::new(MetaStoreProvider::new(config));
        provider.create_meta_store().await.unwrap()
    };
    meta
}

mod test_accessor {
    use std::future::Future;
    use std::sync::atomic::AtomicBool;
    use std::sync::atomic::Ordering;

    use opendal::raw::oio;
    use opendal::raw::oio::Entry;
    use opendal::raw::MaybeSend;
    use opendal::raw::OpDelete;
    use opendal::raw::OpList;
    use opendal::raw::RpDelete;
    use opendal::raw::RpList;

    use super::*;

    // Accessor that throws an error when deleting dir or files.
    #[derive(Debug)]
    pub(crate) struct AccessorFaultyDeletion {
        hit_delete: AtomicBool,
        hit_batch: Arc<AtomicBool>,
        hit_stat: AtomicBool,
        inject_delete_faulty: bool,
        inject_stat_faulty: bool,
    }

    impl AccessorFaultyDeletion {
        pub(crate) fn with_delete_fault() -> Self {
            AccessorFaultyDeletion {
                hit_delete: AtomicBool::new(false),
                hit_batch: Arc::new(AtomicBool::new(false)),
                hit_stat: AtomicBool::new(false),
                inject_delete_faulty: true,
                inject_stat_faulty: false,
            }
        }

        #[allow(dead_code)]
        pub(crate) fn with_stat_fault() -> Self {
            AccessorFaultyDeletion {
                hit_delete: AtomicBool::new(false),
                hit_batch: Arc::new(AtomicBool::new(false)),
                hit_stat: AtomicBool::new(false),
                inject_delete_faulty: false,
                inject_stat_faulty: true,
            }
        }

        pub(crate) fn hit_delete_operation(&self) -> bool {
            self.hit_delete.load(Ordering::Acquire)
        }
    }

    pub struct VecLister(Vec<String>);
    impl oio::List for VecLister {
        fn next(&mut self) -> impl Future<Output = opendal::Result<Option<Entry>>> + MaybeSend {
            let me = &mut self.0;
            async move {
                Ok(me.pop().map(|v| {
                    Entry::new(
                        &v,
                        if v.ends_with('/') {
                            Metadata::new(EntryMode::DIR)
                        } else {
                            Metadata::new(EntryMode::FILE)
                        },
                    )
                }))
            }
        }
    }

    pub struct MockDeleter {
        size: usize,
        hit_batch: Arc<AtomicBool>,
    }

    impl oio::Delete for MockDeleter {
        fn delete(&mut self, _path: &str, _args: OpDelete) -> opendal::Result<()> {
            self.size += 1;
            Ok(())
        }

        async fn flush(&mut self) -> opendal::Result<usize> {
            self.hit_batch.store(true, Ordering::Release);

            let n = self.size;
            self.size = 0;
            Ok(n)
        }
    }

    impl Access for AccessorFaultyDeletion {
        type Reader = ();
        type BlockingReader = ();
        type Writer = ();
        type BlockingWriter = ();
        type Lister = VecLister;
        type BlockingLister = ();
        type Deleter = MockDeleter;
        type BlockingDeleter = ();

        fn info(&self) -> Arc<AccessorInfo> {
            let info = AccessorInfo::default();
            info.set_native_capability(opendal::Capability {
                stat: true,
                create_dir: true,
                delete: true,
                delete_max_size: Some(1000),
                list: true,
                ..Default::default()
            });
            info.into()
        }

        async fn stat(&self, _path: &str, _args: OpStat) -> opendal::Result<RpStat> {
            self.hit_stat.store(true, Ordering::Release);
            if self.inject_stat_faulty {
                Err(opendal::Error::new(
                    opendal::ErrorKind::NotFound,
                    "does not matter (stat)",
                ))
            } else {
                let stat = if _path.ends_with('/') {
                    RpStat::new(Metadata::new(EntryMode::DIR))
                } else {
                    RpStat::new(Metadata::new(EntryMode::FILE))
                };
                Ok(stat)
            }
        }

        async fn delete(&self) -> opendal::Result<(RpDelete, Self::Deleter)> {
            self.hit_delete.store(true, Ordering::Release);

            if self.inject_delete_faulty {
                Err(opendal::Error::new(
                    opendal::ErrorKind::Unexpected,
                    "does not matter (delete)",
                ))
            } else {
                Ok((RpDelete::default(), MockDeleter {
                    size: 0,
                    hit_batch: self.hit_batch.clone(),
                }))
            }
        }

        async fn list(&self, path: &str, _args: OpList) -> opendal::Result<(RpList, Self::Lister)> {
            if self.inject_delete_faulty {
                // While injecting faulty for delete operation, return an empty list;
                // otherwise we need to impl other methods.
                return Ok((RpList::default(), VecLister(vec![])));
            };

            Ok((
                RpList::default(),
                if path.ends_with('/') {
                    VecLister(vec!["a".to_owned(), "b".to_owned()])
                } else {
                    VecLister(vec![])
                },
            ))
        }
    }
    #[tokio::test(flavor = "multi_thread")]
    async fn test_file_util_remove_files_in_batch_do_not_swallow_errors() -> Result<()> {
        // errors should not be swallowed in remove_file_in_batch
        let faulty_accessor = Arc::new(test_accessor::AccessorFaultyDeletion::with_delete_fault());
        let operator = OperatorBuilder::new(faulty_accessor.clone()).finish();
        let fixture = TestFixture::setup().await?;
        let ctx = fixture.new_query_ctx().await?;
        let file_util = Files::create(ctx, operator);

        // files to be deleted does not matter, faulty_accessor will always fail to delete
        let r = file_util.remove_file_in_batch(vec!["1", "2"]).await;
        assert!(r.is_err());

        // verify that accessor.delete() was called
        assert!(faulty_accessor.hit_delete_operation());

        Ok(())
    }
}
