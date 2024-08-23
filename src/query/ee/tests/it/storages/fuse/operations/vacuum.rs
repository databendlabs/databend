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

use databend_common_base::base::tokio;
use databend_common_exception::Result;
use databend_common_meta_app::schema::TableInfo;
use databend_common_meta_app::schema::TableMeta;
use databend_common_meta_app::storage::StorageParams;
use databend_common_storage::DataOperator;
use databend_common_storages_fuse::TableContext;
use databend_enterprise_query::storages::fuse::operations::vacuum_drop_tables::do_vacuum_drop_table;
use databend_enterprise_query::storages::fuse::operations::vacuum_drop_tables::vacuum_drop_tables_by_table_info;
use databend_enterprise_query::storages::fuse::operations::vacuum_temporary_files::do_vacuum_temporary_files;
use databend_enterprise_query::storages::fuse::vacuum_drop_tables;
use databend_query::test_kits::*;
use databend_storages_common_table_meta::table::OPT_KEY_DATABASE_ID;
use opendal::raw::Access;
use opendal::raw::AccessorInfo;
use opendal::raw::OpStat;
use opendal::raw::RpStat;
use opendal::EntryMode;
use opendal::Metadata;
use opendal::OperatorBuilder;

#[tokio::test(flavor = "multi_thread")]
async fn test_fuse_do_vacuum_drop_tables() -> Result<()> {
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

    let operator = DataOperator::instance().operator();
    operator.write("test_dir/test1", vec![1, 2]).await?;
    operator.write("test_dir/test2", vec![1, 2]).await?;
    operator.write("test_dir/test3", vec![1, 2]).await?;

    assert_eq!(
        3,
        operator.list_with("test_dir/").recursive(true).await?.len()
    );

    tokio::time::sleep(Duration::from_secs(2)).await;
    do_vacuum_temporary_files("test_dir/".to_string(), Some(Duration::from_secs(2)), 1).await?;

    assert_eq!(2, operator.list("test_dir/").await?.len());

    operator.write("test_dir/test4/test4", vec![1, 2]).await?;
    operator.write("test_dir/test5/test5", vec![1, 2]).await?;
    operator
        .write("test_dir/test5/finished", vec![1, 2])
        .await?;

    do_vacuum_temporary_files("test_dir/".to_string(), Some(Duration::from_secs(2)), 2).await?;
    assert_eq!(operator.list("test_dir/").await?.len(), 2);

    tokio::time::sleep(Duration::from_secs(3)).await;
    do_vacuum_temporary_files("test_dir/".to_string(), Some(Duration::from_secs(3)), 1000).await?;
    assert!(operator.list_with("test_dir/").await?.is_empty());

    Ok(())
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
        hit_stat: AtomicBool,
        inject_delete_faulty: bool,
        inject_stat_faulty: bool,
    }

    impl AccessorFaultyDeletion {
        pub(crate) fn with_delete_fault() -> Self {
            AccessorFaultyDeletion {
                hit_delete: AtomicBool::new(false),
                hit_stat: AtomicBool::new(false),
                inject_delete_faulty: true,
                inject_stat_faulty: false,
            }
        }

        pub(crate) fn with_stat_fault() -> Self {
            AccessorFaultyDeletion {
                hit_delete: AtomicBool::new(false),
                hit_stat: AtomicBool::new(false),
                inject_delete_faulty: false,
                inject_stat_faulty: true,
            }
        }

        pub(crate) fn hit_delete_operation(&self) -> bool {
            self.hit_delete.load(Ordering::Acquire)
        }

        pub(crate) fn hit_stat_operation(&self) -> bool {
            self.hit_stat.load(Ordering::Acquire)
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

    impl Access for AccessorFaultyDeletion {
        type Reader = ();
        type BlockingReader = ();
        type Writer = ();
        type BlockingWriter = ();
        type Lister = VecLister;
        type BlockingLister = ();

        fn info(&self) -> Arc<AccessorInfo> {
            let mut info = AccessorInfo::default();
            let cap = info.full_capability_mut();
            cap.stat = true;
            cap.create_dir = true;
            cap.batch = true;
            cap.delete = true;
            cap.list = true;
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

        async fn delete(&self, _path: &str, _args: OpDelete) -> opendal::Result<RpDelete> {
            self.hit_delete.store(true, Ordering::Release);
            if self.inject_delete_faulty {
                Err(opendal::Error::new(
                    opendal::ErrorKind::Unexpected,
                    "does not matter (delete)",
                ))
            } else {
                Ok(RpDelete::default())
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
async fn test_fuse_do_vacuum_drop_table_deletion_error() -> Result<()> {
    // do_vacuum_drop_table should return Err if file deletion failed

    let mut table_info = TableInfo::default();
    table_info
        .meta
        .options
        .insert(OPT_KEY_DATABASE_ID.to_owned(), "1".to_owned());

    use test_accessor::AccessorFaultyDeletion;
    // Operator with mocked accessor that will fail on `remove_all`
    //
    // Note that:
    // In real case, `Accessor::batch` will be called (instead of Accessor::delete)
    // but all that we need here is let Operator::remove_all failed
    let faulty_accessor = std::sync::Arc::new(AccessorFaultyDeletion::with_delete_fault());
    let operator = OperatorBuilder::new(faulty_accessor.clone()).finish();

    let tables = vec![(table_info, operator)];
    let result = do_vacuum_drop_table(tables, None).await;
    assert!(result.is_err());

    // verify that accessor.delete() was called
    assert!(faulty_accessor.hit_delete_operation());

    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn test_fuse_vacuum_drop_tables_in_parallel_with_deletion_error() -> Result<()> {
    let mut table_info = TableInfo::default();
    table_info
        .meta
        .options
        .insert(OPT_KEY_DATABASE_ID.to_owned(), "1".to_owned());

    use test_accessor::AccessorFaultyDeletion;

    // Case 1: non-parallel vacuum dropped tables
    {
        let faulty_accessor = std::sync::Arc::new(AccessorFaultyDeletion::with_delete_fault());
        let operator = OperatorBuilder::new(faulty_accessor.clone()).finish();

        let table = (table_info.clone(), operator);

        // with one table and one thread, `vacuum_drop_tables_by_table_info` will NOT run in parallel
        let tables = vec![table];
        let num_threads = 1;
        let result = vacuum_drop_tables_by_table_info(num_threads, tables, None).await;
        // verify that accessor.delete() was called
        assert!(faulty_accessor.hit_delete_operation());

        // verify that errors of deletions are not swallowed
        assert!(result.is_err());
    }

    // Case 2: parallel vacuum dropped tables
    {
        let faulty_accessor = std::sync::Arc::new(AccessorFaultyDeletion::with_delete_fault());
        let operator = OperatorBuilder::new(faulty_accessor.clone()).finish();

        let table = (table_info, operator);
        // with 2 tables and 2 threads, `vacuum_drop_tables_by_table_info` will run in parallel (one table per thread)
        let tables = vec![table.clone(), table];
        let num_threads = 2;
        let result = vacuum_drop_tables_by_table_info(num_threads, tables, None).await;
        // verify that accessor.delete() was called
        assert!(faulty_accessor.hit_delete_operation());
        // verify that errors of deletions are not swallowed
        assert!(result.is_err());
    }

    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn test_fuse_vacuum_drop_tables_dry_run_with_obj_not_found_error() -> Result<()> {
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
        // verify that accessor.stat() was called
        assert!(faulty_accessor.hit_stat_operation());
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
        // verify that accessor.stat() was called
        assert!(faulty_accessor.hit_stat_operation());
        // verify that errors of NotFound are swallowed
        assert!(result.is_ok());
    }

    Ok(())
}

// fuse table on external storage is same as internal storage.
#[tokio::test(flavor = "multi_thread")]
async fn test_fuse_do_vacuum_drop_table_external_storage() -> Result<()> {
    let meta = TableMeta {
        storage_params: Some(StorageParams::default()),
        ..Default::default()
    };

    let table_info = TableInfo {
        meta,
        ..Default::default()
    };

    // Accessor passed in does NOT matter in this case, `do_vacuum_drop_table` should
    // return Ok(None) before accessor is used.
    use test_accessor::AccessorFaultyDeletion;
    let accessor = std::sync::Arc::new(AccessorFaultyDeletion::with_delete_fault());
    let operator = OperatorBuilder::new(accessor.clone()).finish();

    let tables = vec![(table_info, operator)];
    let result = do_vacuum_drop_table(tables, None).await;
    assert!(result.is_err());

    // verify that accessor.delete() was called
    assert!(!accessor.hit_delete_operation());

    Ok(())
}
