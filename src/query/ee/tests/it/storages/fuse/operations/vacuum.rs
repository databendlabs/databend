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
use std::sync::Arc;
use std::time::Duration;

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
use databend_common_meta_app::schema::TableInfo;
use databend_common_meta_app::schema::TableMeta;
use databend_common_meta_app::storage::StorageParams;
use databend_common_meta_store::MetaStore;
use databend_common_meta_store::MetaStoreProvider;
use databend_common_storage::DataOperator;
use databend_common_storages_fuse::TableContext;
use databend_enterprise_query::storages::fuse::operations::vacuum_drop_tables::do_vacuum_drop_table;
use databend_enterprise_query::storages::fuse::operations::vacuum_drop_tables::vacuum_drop_tables_by_table_info;
use databend_enterprise_query::storages::fuse::operations::vacuum_temporary_files::do_vacuum_temporary_files;
use databend_enterprise_query::storages::fuse::vacuum_drop_tables;
use databend_enterprise_query::test_kits::context::EESetup;
use databend_enterprise_vacuum_handler::vacuum_handler::VacuumTempOptions;
use databend_meta_kvapi::kvapi::KvApiExt;
use databend_meta_kvapi::kvapi::ListOptions;
use databend_meta_runtime::DatabendRuntime;
use databend_meta_types::TxnRequest;
use databend_query::test_kits::*;
use databend_storages_common_io::Files;
use databend_storages_common_table_meta::table::OPT_KEY_DATABASE_ID;
use opendal::EntryMode;
use opendal::Metadata;
use opendal::OperatorBuilder;
use opendal::raw::Access;
use opendal::raw::AccessorInfo;
use opendal::raw::OpStat;
use opendal::raw::RpStat;

#[tokio::test(flavor = "multi_thread")]
async fn test_fuse_do_vacuum_drop_tables() -> anyhow::Result<()> {
    let fixture = TestFixture::setup().await?;

    fixture
        .default_session()
        .get_settings()
        .set_data_retention_time_in_days(0)?;

    fixture.create_default_database().await?;
    fixture.create_default_table().await?;

    let number_of_block = 1;
    append_sample_data(number_of_block, &fixture).await?;

    let table = fixture.latest_default_table().await?;

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

    // do gc.
    let db = fixture.default_db_name();
    let tbl = fixture.default_table_name();
    let qry = format!("drop table {}.{}", db, tbl);
    fixture.execute_command(&qry).await?;
    let ctx = fixture.new_query_ctx().await?;
    let threads_nums = ctx.get_settings().get_max_threads()? as usize;

    // verify dry run never delete files
    {
        vacuum_drop_tables(threads_nums, vec![table.clone()], Some(100)).await?;
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
        vacuum_drop_tables(threads_nums, vec![table], None).await?;

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
async fn test_do_vacuum_temporary_files() -> anyhow::Result<()> {
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

mod test_accessor {
    use std::future::Future;
    use std::sync::atomic::AtomicBool;
    use std::sync::atomic::Ordering;

    use opendal::raw::MaybeSend;
    use opendal::raw::OpDelete;
    use opendal::raw::OpList;
    use opendal::raw::RpDelete;
    use opendal::raw::RpList;
    use opendal::raw::oio;
    use opendal::raw::oio::Entry;

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
        type Writer = ();
        type Lister = VecLister;
        type Deleter = MockDeleter;

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
}

#[tokio::test(flavor = "multi_thread")]
async fn test_fuse_do_vacuum_drop_table_deletion_error() -> anyhow::Result<()> {
    // do_vacuum_drop_table should return Err if file deletion failed

    let mut table_info = TableInfo::default();
    table_info
        .meta
        .options
        .insert(OPT_KEY_DATABASE_ID.to_owned(), "1".to_owned());
    table_info.desc = "`default`.`t`".to_string();

    use test_accessor::AccessorFaultyDeletion;
    // Operator with mocked accessor that will fail on `remove_all`
    //
    // Note that:
    // In real case, `Accessor::batch` will be called (instead of Accessor::delete)
    // but all that we need here is let Operator::remove_all failed
    let faulty_accessor = std::sync::Arc::new(AccessorFaultyDeletion::with_delete_fault());
    let operator = OperatorBuilder::new(faulty_accessor.clone()).finish();

    let tables = vec![(table_info, operator)];
    let result = do_vacuum_drop_table(tables, None).await?;
    assert!(!result.1.is_empty());
    // verify that accessor.delete() was called
    assert!(faulty_accessor.hit_delete_operation());

    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn test_fuse_vacuum_drop_tables_in_parallel_with_deletion_error() -> anyhow::Result<()> {
    let mut table_info = TableInfo::default();
    table_info
        .meta
        .options
        .insert(OPT_KEY_DATABASE_ID.to_owned(), "1".to_owned());
    table_info.desc = "`default`.`t`".to_string();
    use test_accessor::AccessorFaultyDeletion;

    // Case 1: non-parallel vacuum dropped tables
    {
        let faulty_accessor = std::sync::Arc::new(AccessorFaultyDeletion::with_delete_fault());
        let operator = OperatorBuilder::new(faulty_accessor.clone()).finish();

        let table = (table_info.clone(), operator);

        // with one table and one thread, `vacuum_drop_tables_by_table_info` will NOT run in parallel
        let tables = vec![table];
        let num_threads = 1;
        let result = vacuum_drop_tables_by_table_info(num_threads, tables, None).await?;
        // verify that accessor.delete() was called
        assert!(faulty_accessor.hit_delete_operation());

        // verify that errors of deletions are not swallowed
        assert!(!result.1.is_empty());
    }

    // Case 2: parallel vacuum dropped tables
    {
        let faulty_accessor = std::sync::Arc::new(AccessorFaultyDeletion::with_delete_fault());
        let operator = OperatorBuilder::new(faulty_accessor.clone()).finish();

        let table = (table_info, operator);
        // with 2 tables and 2 threads, `vacuum_drop_tables_by_table_info` will run in parallel (one table per thread)
        let tables = vec![table.clone(), table];
        let num_threads = 2;
        let result = vacuum_drop_tables_by_table_info(num_threads, tables, None).await?;
        // verify that accessor.delete() was called
        assert!(faulty_accessor.hit_delete_operation());
        // verify that errors of deletions are not swallowed
        assert!(!result.1.is_empty());
    }

    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn test_fuse_vacuum_drop_tables_dry_run_with_obj_not_found_error() -> anyhow::Result<()> {
    let mut table_info = TableInfo::default();
    table_info
        .meta
        .options
        .insert(OPT_KEY_DATABASE_ID.to_owned(), "1".to_owned());

    use test_accessor::AccessorFaultyDeletion;

    // Case 1: non-parallel vacuum dry-run dropped tables
    {
        let faulty_accessor = Arc::new(AccessorFaultyDeletion::with_stat_fault());
        let operator = OperatorBuilder::new(faulty_accessor.clone()).finish();

        let table = (table_info.clone(), operator);

        // with one table and one thread, `vacuum_drop_tables_by_table_info` will NOT run in parallel
        let tables = vec![table];
        let num_threads = 1;
        let result = vacuum_drop_tables_by_table_info(num_threads, tables, Some(usize::MAX)).await;
        // verify that errors of NotFound are swallowed
        assert!(result.is_ok());
    }

    // Case 2: parallel vacuum dry-run dropped tables
    {
        let faulty_accessor = Arc::new(AccessorFaultyDeletion::with_stat_fault());
        let operator = OperatorBuilder::new(faulty_accessor.clone()).finish();

        let table = (table_info, operator);
        // with 2 tables and 2 threads, `vacuum_drop_tables_by_table_info` will run in parallel (one table per thread)
        let tables = vec![table.clone(), table];
        let num_threads = 2;
        let result = vacuum_drop_tables_by_table_info(num_threads, tables, Some(usize::MAX)).await;
        // verify that errors of NotFound are swallowed
        assert!(result.is_ok());
    }

    Ok(())
}

// fuse table on external storage is same as internal storage.
#[tokio::test(flavor = "multi_thread")]
async fn test_fuse_do_vacuum_drop_table_external_storage() -> anyhow::Result<()> {
    let meta = TableMeta {
        storage_params: Some(StorageParams::default()),
        ..Default::default()
    };

    let table_info = TableInfo {
        desc: "`default`.`t`".to_string(),
        meta,
        ..Default::default()
    };

    // Accessor passed in does NOT matter in this case, `do_vacuum_drop_table` should
    // return Ok(None) before accessor is used.
    use test_accessor::AccessorFaultyDeletion;
    let accessor = std::sync::Arc::new(AccessorFaultyDeletion::with_delete_fault());
    let operator = OperatorBuilder::new(accessor.clone()).finish();

    let tables = vec![(table_info, operator)];
    let result = do_vacuum_drop_table(tables, None).await?;
    assert!(!result.1.is_empty());

    // verify that accessor.delete() was called
    assert!(!accessor.hit_delete_operation());

    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn test_remove_files_in_batch_do_not_swallow_errors() -> anyhow::Result<()> {
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

#[tokio::test(flavor = "multi_thread")]
async fn test_vacuum_dropped_table_clean_autoincrement() -> anyhow::Result<()> {
    // 1. Prepare local meta service
    let meta = new_local_meta().await;
    let endpoints = meta.inner().endpoints.clone();

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
async fn test_vacuum_dropped_table_clean_ownership() -> anyhow::Result<()> {
    // 1. Prepare local meta service
    let meta = new_local_meta().await;
    let endpoints = meta.inner().endpoints.clone();

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
async fn test_gc_in_progress_db_not_undroppable() -> anyhow::Result<()> {
    // 1. Prepare local meta service
    let meta = new_local_meta().await;
    let endpoints = meta.inner().endpoints.clone();

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
async fn test_vacuum_drop_create_or_replace() -> anyhow::Result<()> {
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

async fn test_vacuum_drop_create_or_replace_impl(vacuum_stmts: &[&str]) -> anyhow::Result<()> {
    // Setup
    let meta = new_local_meta().await;
    let endpoints = meta.inner().endpoints.clone();

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
    let items = meta.list_kv_collect(ListOptions::unlimited(prefix)).await?;
    assert_eq!(items.len(), 6);

    // there should be ownerships for 2 db ids and 5 table ids, one of the ownership is revoked by drop table
    let prefix = "__fd_object_owners";
    let items = meta.list_kv_collect(ListOptions::unlimited(prefix)).await?;
    assert_eq!(items.len(), 7);

    for sql in vacuum_stmts {
        fixture.execute_command(sql).await?;
    }

    // After vacuum, 1 table ids left
    let prefix = "__fd_table_by_id";
    let items = meta.list_kv_collect(ListOptions::unlimited(prefix)).await?;
    assert_eq!(items.len(), 1);

    let prefix = "__fd_table_id_to_name";
    let items = meta.list_kv_collect(ListOptions::unlimited(prefix)).await?;
    assert_eq!(items.len(), 1);

    // There are ownership objs of  2 dbs and 1 tables
    let prefix = "__fd_object_owners";
    let items = meta.list_kv_collect(ListOptions::unlimited(prefix)).await?;
    assert_eq!(items.len(), 3);

    // db1.t1 should still be accessible
    fixture.execute_command("select * from db1.t1").await?;
    // db2.t1 should not exist
    assert!(
        fixture
            .execute_command("select * from db2.t1")
            .await
            .is_err()
    );
    Ok(())
}

async fn new_local_meta() -> MetaStore {
    let meta_config = MetaConfig::default();
    let config = meta_config.to_meta_grpc_client_conf();
    let provider = MetaStoreProvider::new(config);
    provider
        .create_meta_store::<DatabendRuntime>()
        .await
        .unwrap()
}

#[tokio::test(flavor = "multi_thread")]
async fn test_vacuum_dropped_table_clean_tag_refs() -> anyhow::Result<()> {
    use databend_common_meta_api::tag_api::TagApi;
    use databend_common_meta_app::schema::CreateTagReq;
    use databend_common_meta_app::schema::SetObjectTagsReq;
    use databend_common_meta_app::schema::TagMeta;
    use databend_common_meta_app::schema::TagNameIdent;
    use databend_common_meta_app::schema::TaggableObject;

    // 1. Prepare local meta service
    let meta = new_local_meta().await;
    let endpoints = meta.inner().endpoints.clone();

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
    let db_name = "test_vacuum_clean_tag_refs";
    let tbl_name = "t";
    fixture
        .execute_command(format!("create database {db_name}").as_str())
        .await?;
    fixture
        .execute_command(format!("create table {db_name}.{tbl_name} (a int)").as_str())
        .await?;

    // 4. Get table and database IDs
    let tenant = ctx.get_tenant();
    let table = ctx
        .get_default_catalog()?
        .get_table(&tenant, db_name, tbl_name)
        .await?;
    let table_id = table.get_id();

    let db = ctx
        .get_default_catalog()?
        .get_database(&tenant, db_name)
        .await?;
    let db_id = db.get_db_info().database_id.db_id;

    // 5. Create a tag and set it on both table and database
    let tag_name = "test_env_tag";
    let tag_ident = TagNameIdent::new(&tenant, tag_name);
    let create_tag_req = CreateTagReq {
        name_ident: tag_ident.clone(),
        meta: TagMeta {
            allowed_values: Some(vec!["dev".to_string(), "prod".to_string()]),
            comment: "test tag".to_string(),
            created_on: chrono::Utc::now(),
            updated_on: None,
            drop_on: None,
        },
    };
    let create_result = meta.create_tag(create_tag_req).await?;
    let tag_id = create_result.unwrap().tag_id;

    // Set tag on table
    let table_object = TaggableObject::Table { table_id };
    let set_table_tag_req = SetObjectTagsReq {
        tenant: tenant.clone(),
        taggable_object: table_object.clone(),
        tags: vec![(tag_id, "dev".to_string())],
    };
    meta.set_object_tags(set_table_tag_req).await?.unwrap();

    // Set tag on database
    let db_object = TaggableObject::Database { db_id };
    let set_db_tag_req = SetObjectTagsReq {
        tenant: tenant.clone(),
        taggable_object: db_object.clone(),
        tags: vec![(tag_id, "prod".to_string())],
    };
    meta.set_object_tags(set_db_tag_req).await?.unwrap();

    // 6. Verify tag references exist
    let table_tags = meta.get_object_tags(&tenant, &table_object).await?;
    assert_eq!(table_tags.len(), 1);
    assert_eq!(table_tags[0].tag_id, tag_id);
    assert_eq!(table_tags[0].tag_value.data.tag_allowed_value, "dev");

    let db_tags = meta.get_object_tags(&tenant, &db_object).await?;
    assert_eq!(db_tags.len(), 1);
    assert_eq!(db_tags[0].tag_id, tag_id);
    assert_eq!(db_tags[0].tag_value.data.tag_allowed_value, "prod");

    // 7. Drop database (this will mark both database and table as dropped)
    // Database tag refs should be cleaned immediately by DROP DATABASE
    // Table tag refs should still exist (cleaned by VACUUM later)
    fixture
        .execute_command(format!("drop database {db_name}").as_str())
        .await?;

    // Database tag refs should be cleaned by DROP DATABASE
    let db_tags = meta.get_object_tags(&tenant, &db_object).await?;
    assert!(
        db_tags.is_empty(),
        "db tag refs should be cleaned by DROP DATABASE"
    );

    // Table tag refs should still exist after DROP DATABASE
    // (only cleaned by VACUUM when table is permanently removed)
    let table_tags = meta.get_object_tags(&tenant, &table_object).await?;
    assert_eq!(
        table_tags.len(),
        1,
        "table tag refs should still exist after DROP DATABASE"
    );

    // 8. Vacuum dropped tables
    fixture.execute_command("vacuum drop table").await?;

    // 9. Verify table tag references are cleaned up after vacuum
    let table_tags = meta.get_object_tags(&tenant, &table_object).await?;
    assert!(
        table_tags.is_empty(),
        "table tag refs should be cleaned up after vacuum"
    );

    // 10. Tag definition should still exist (only references are cleaned)
    let tag = meta.get_tag(&tag_ident).await?;
    assert!(tag.is_some(), "tag definition should still exist");

    // Cleanup: drop the tag
    let _ = meta.drop_tag(&tag_ident).await?;

    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn test_vacuum_dropped_table_clean_policies() -> anyhow::Result<()> {
    use databend_common_meta_app::data_mask::MaskPolicyIdTableId;
    use databend_common_meta_app::data_mask::MaskPolicyTableIdIdent;
    use databend_common_meta_app::row_access_policy::RowAccessPolicyTableIdIdent;
    use databend_common_meta_app::row_access_policy::row_access_policy_table_id_ident::RowAccessPolicyIdTableId;

    // 1. Prepare local meta service
    let meta = new_local_meta().await;
    let endpoints = meta.inner().endpoints.clone();

    // Modify config to use local meta store
    let mut ee_setup = EESetup::new();
    let config = ee_setup.config_mut();
    config.meta.endpoints = endpoints.clone();

    // 2. Setup test fixture by using local meta store
    let fixture = TestFixture::setup_with_custom(ee_setup).await?;

    // Adjust retention period to 0, so that dropped tables will be vacuumed immediately
    let session = fixture.default_session();
    session.get_settings().set_data_retention_time_in_days(0)?;

    // Enable row access policy feature
    fixture
        .execute_command("set global enable_experimental_row_access_policy = 1")
        .await?;

    // 3. Prepare test db and table
    let ctx = fixture.new_query_ctx().await?;
    let db_name = "test_vacuum_clean_policies";
    let tbl_name = "t";
    fixture
        .execute_command(format!("create database {db_name}").as_str())
        .await?;
    fixture
        .execute_command(
            format!("create table {db_name}.{tbl_name} (id int, name string, email string)")
                .as_str(),
        )
        .await?;

    // 4. Create masking policies and row access policy
    fixture
        .execute_command(
            "CREATE MASKING POLICY email_mask AS (val STRING) RETURNS STRING -> CASE WHEN current_role() = 'admin' THEN val ELSE '***' END",
        )
        .await?;

    fixture
        .execute_command(
            "CREATE MASKING POLICY name_mask AS (val STRING) RETURNS STRING -> CASE WHEN current_role() = 'admin' THEN val ELSE 'REDACTED' END",
        )
        .await?;

    fixture
        .execute_command(
            "CREATE ROW ACCESS POLICY row_filter AS (id int) RETURNS boolean -> current_role() = 'admin' OR id > 0",
        )
        .await?;

    // 5. Apply policies to table columns
    fixture
        .execute_command(
            format!(
                "ALTER TABLE {db_name}.{tbl_name} MODIFY COLUMN email SET MASKING POLICY email_mask"
            )
            .as_str(),
        )
        .await?;

    fixture
        .execute_command(
            format!(
                "ALTER TABLE {db_name}.{tbl_name} MODIFY COLUMN name SET MASKING POLICY name_mask"
            )
            .as_str(),
        )
        .await?;

    fixture
        .execute_command(
            format!("ALTER TABLE {db_name}.{tbl_name} ADD ROW ACCESS POLICY row_filter ON (id)")
                .as_str(),
        )
        .await?;

    // 6. Get table ID and verify policy references exist
    let tenant = ctx.get_tenant();
    let table = ctx
        .get_default_catalog()?
        .get_table(&tenant, db_name, tbl_name)
        .await?;
    let table_id = table.get_id();

    // Get policy IDs from table metadata
    let table_meta = table.get_table_info().meta.clone();

    // Verify masking policy references exist
    assert!(
        !table_meta.column_mask_policy_columns_ids.is_empty(),
        "masking policy references should exist after applying policies"
    );

    // Verify row access policy reference exists
    assert!(
        table_meta.row_access_policy_columns_ids.is_some(),
        "row access policy reference should exist after applying policy"
    );

    let mask_policy_ids: Vec<u64> = table_meta
        .column_mask_policy_columns_ids
        .values()
        .map(|policy_map| policy_map.policy_id)
        .collect();

    let row_policy_id = table_meta
        .row_access_policy_columns_ids
        .as_ref()
        .unwrap()
        .policy_id;

    // Verify policy references in meta store
    for policy_id in &mask_policy_ids {
        let mask_policy_key =
            MaskPolicyTableIdIdent::new_generic(tenant.clone(), MaskPolicyIdTableId {
                policy_id: *policy_id,
                table_id,
            });
        let v = meta.get_pb(&mask_policy_key).await?;
        assert!(
            v.is_some(),
            "masking policy reference should exist in meta store"
        );
    }

    let row_policy_key =
        RowAccessPolicyTableIdIdent::new_generic(tenant.clone(), RowAccessPolicyIdTableId {
            policy_id: row_policy_id,
            table_id,
        });
    let v = meta.get_pb(&row_policy_key).await?;
    assert!(
        v.is_some(),
        "row access policy reference should exist in meta store"
    );

    // 7. Drop database (this will mark both database and table as dropped)
    fixture
        .execute_command(format!("drop database {db_name}").as_str())
        .await?;

    // 8. Vacuum dropped tables
    fixture.execute_command("vacuum drop table").await?;

    // 9. Ensure that policy references are cleaned up
    for policy_id in &mask_policy_ids {
        let mask_policy_key =
            MaskPolicyTableIdIdent::new_generic(tenant.clone(), MaskPolicyIdTableId {
                policy_id: *policy_id,
                table_id,
            });
        let v = meta.get_pb(&mask_policy_key).await?;
        assert!(
            v.is_none(),
            "masking policy reference should be cleaned up after vacuum"
        );
    }

    let row_policy_key =
        RowAccessPolicyTableIdIdent::new_generic(tenant.clone(), RowAccessPolicyIdTableId {
            policy_id: row_policy_id,
            table_id,
        });
    let v = meta.get_pb(&row_policy_key).await?;
    assert!(
        v.is_none(),
        "row access policy reference should be cleaned up after vacuum"
    );

    Ok(())
}
