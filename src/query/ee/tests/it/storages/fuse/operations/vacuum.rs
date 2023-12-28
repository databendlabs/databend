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

use databend_common_base::base::tokio;
use databend_common_exception::Result;
use databend_common_meta_app::schema::TableInfo;
use databend_common_meta_app::schema::TableMeta;
use databend_common_meta_app::storage::StorageParams;
use databend_enterprise_query::storages::fuse::do_vacuum_drop_tables;
use databend_enterprise_query::storages::fuse::operations::vacuum_drop_tables::do_vacuum_drop_table;
use databend_query::test_kits::*;
use databend_storages_common_table_meta::table::OPT_KEY_DATABASE_ID;
use opendal::raw::Accessor;
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
        .set_retention_period(0)?;

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

    // verify dry run never delete files
    {
        do_vacuum_drop_tables(vec![table.clone()], Some(100)).await?;
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
        do_vacuum_drop_tables(vec![table], None).await?;

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

mod test_accessor {
    use std::sync::atomic::AtomicBool;
    use std::sync::atomic::Ordering;

    use opendal::raw::OpDelete;
    use opendal::raw::OpList;
    use opendal::raw::RpDelete;
    use opendal::raw::RpList;

    use super::*;

    // Accessor that throws an error when deleting dir or files.
    #[derive(Debug)]
    pub(crate) struct AccessorFaultyDeletion {
        hit_delete: AtomicBool,
    }

    impl AccessorFaultyDeletion {
        pub(crate) fn new() -> Self {
            AccessorFaultyDeletion {
                hit_delete: AtomicBool::new(false),
            }
        }

        pub(crate) fn hit_delete_operation(&self) -> bool {
            self.hit_delete.load(Ordering::Acquire)
        }
    }
    #[async_trait::async_trait]
    impl Accessor for AccessorFaultyDeletion {
        type Reader = ();
        type BlockingReader = ();
        type Writer = ();
        type BlockingWriter = ();
        type Lister = ();
        type BlockingLister = ();

        fn info(&self) -> AccessorInfo {
            let mut info = AccessorInfo::default();
            let cap = info.full_capability_mut();
            cap.stat = true;
            cap.create_dir = true;
            cap.batch = true;
            cap.delete = true;
            cap.list = true;
            cap.list_without_recursive = true;
            info
        }

        async fn stat(&self, _path: &str, _args: OpStat) -> opendal::Result<RpStat> {
            let stat = RpStat::new(Metadata::new(EntryMode::DIR));
            Ok(stat)
        }

        async fn delete(&self, _path: &str, _args: OpDelete) -> opendal::Result<RpDelete> {
            self.hit_delete.store(true, Ordering::Release);
            Err(opendal::Error::new(
                opendal::ErrorKind::Unexpected,
                "does not matter (delete)",
            ))
        }

        async fn list(
            &self,
            _path: &str,
            _args: OpList,
        ) -> opendal::Result<(RpList, Self::Lister)> {
            Ok((RpList::default(), ()))
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
    let faulty_accessor = std::sync::Arc::new(AccessorFaultyDeletion::new());
    let operator = OperatorBuilder::new(faulty_accessor.clone()).finish();

    let result = do_vacuum_drop_table(&table_info, &operator, None).await;
    assert!(result.is_err());

    // verify that accessor.delete() was called
    assert!(faulty_accessor.hit_delete_operation());

    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn test_fuse_do_vacuum_drop_table_external_storage() -> Result<()> {
    // do_vacuum_drop_table should return Ok(None) if external storage detected

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
    let accessor = std::sync::Arc::new(AccessorFaultyDeletion::new());
    let operator = OperatorBuilder::new(accessor.clone()).finish();

    let result = do_vacuum_drop_table(&table_info, &operator, None).await;

    // verify that Ok(None) is returned
    assert!(result.is_ok());
    assert!(result.unwrap().is_none());

    // verify that accessor.delete() was NOT called
    assert!(!accessor.hit_delete_operation());

    Ok(())
}
