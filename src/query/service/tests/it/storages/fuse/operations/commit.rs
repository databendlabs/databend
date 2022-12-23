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
use std::any::Any;
use std::sync::atomic::AtomicBool;
use std::sync::Arc;
use std::time::Duration;

use common_base::base::tokio;
use common_base::base::Progress;
use common_base::base::ProgressValues;
use common_catalog::catalog::Catalog;
use common_catalog::cluster_info::Cluster;
use common_catalog::database::Database;
use common_catalog::plan::DataSourcePlan;
use common_catalog::plan::PartInfoPtr;
use common_catalog::plan::Partitions;
use common_catalog::table::Table;
use common_catalog::table_context::ProcessInfo;
use common_catalog::table_context::StageAttachment;
use common_catalog::table_context::TableContext;
use common_datablocks::DataBlock;
use common_exception::ErrorCode;
use common_exception::Result;
use common_functions::scalars::FunctionContext;
use common_io::prelude::FormatSettings;
use common_meta_app::schema::CountTablesReply;
use common_meta_app::schema::CountTablesReq;
use common_meta_app::schema::CreateDatabaseReply;
use common_meta_app::schema::CreateDatabaseReq;
use common_meta_app::schema::CreateTableReq;
use common_meta_app::schema::DropDatabaseReq;
use common_meta_app::schema::DropTableReply;
use common_meta_app::schema::DropTableReq;
use common_meta_app::schema::GetTableCopiedFileReply;
use common_meta_app::schema::GetTableCopiedFileReq;
use common_meta_app::schema::RenameDatabaseReply;
use common_meta_app::schema::RenameDatabaseReq;
use common_meta_app::schema::RenameTableReply;
use common_meta_app::schema::RenameTableReq;
use common_meta_app::schema::TableIdent;
use common_meta_app::schema::TableInfo;
use common_meta_app::schema::TableMeta;
use common_meta_app::schema::TruncateTableReply;
use common_meta_app::schema::TruncateTableReq;
use common_meta_app::schema::UndropDatabaseReply;
use common_meta_app::schema::UndropDatabaseReq;
use common_meta_app::schema::UndropTableReply;
use common_meta_app::schema::UndropTableReq;
use common_meta_app::schema::UpdateTableMetaReply;
use common_meta_app::schema::UpdateTableMetaReq;
use common_meta_app::schema::UpsertTableCopiedFileReply;
use common_meta_app::schema::UpsertTableCopiedFileReq;
use common_meta_app::schema::UpsertTableOptionReply;
use common_meta_app::schema::UpsertTableOptionReq;
use common_meta_types::MetaId;
use common_meta_types::RoleInfo;
use common_meta_types::UserInfo;
use common_settings::Settings;
use common_storage::DataOperator;
use common_storages_fuse::operations::AppendOperationLogEntry;
use common_storages_fuse::FuseTable;
use common_storages_fuse::FUSE_TBL_SNAPSHOT_PREFIX;
use common_storages_table_meta::meta::SegmentInfo;
use common_storages_table_meta::meta::Statistics;
use databend_query::sessions::QueryContext;
use futures::TryStreamExt;
use walkdir::WalkDir;

use crate::storages::fuse::table_test_fixture::execute_query;
use crate::storages::fuse::table_test_fixture::TestFixture;

#[tokio::test]
async fn test_fuse_occ_retry() -> Result<()> {
    let fixture = TestFixture::new().await;
    let db = fixture.default_db_name();
    let tbl = fixture.default_table_name();
    let ctx = fixture.ctx();
    fixture.create_default_table().await?;

    let table = fixture.latest_default_table().await?;

    // insert one row `id = 1` into the table, without committing
    {
        let num_blocks = 1;
        let rows_per_block = 1;
        let value_start_from = 1;
        let stream =
            TestFixture::gen_sample_blocks_stream_ex(num_blocks, rows_per_block, value_start_from);

        let blocks = stream.try_collect().await?;
        fixture
            .append_commit_blocks(table.clone(), blocks, false, false)
            .await?;
    }

    // insert another row `id = 5` into the table, and do commit the insertion
    {
        let num_blocks = 1;
        let rows_per_block = 1;
        let value_start_from = 5;
        let stream =
            TestFixture::gen_sample_blocks_stream_ex(num_blocks, rows_per_block, value_start_from);

        let blocks = stream.try_collect().await?;
        fixture
            .append_commit_blocks(table.clone(), blocks, false, true)
            .await?;
    }

    // let's check it out
    let qry = format!("select * from {}.{} order by id ", db, tbl);
    let blocks = execute_query(ctx.clone(), qry.as_str())
        .await?
        .try_collect::<Vec<DataBlock>>()
        .await?;

    let expected = vec![
        "+----+----------+", //
        "| id | t        |", //
        "+----+----------+", //
        "| 1  | (2, 3)   |", //
        "| 5  | (10, 15) |", //
        "+----+----------+", //
    ];
    common_datablocks::assert_blocks_sorted_eq(expected, blocks.as_slice());

    Ok(())
}

#[tokio::test]
async fn test_last_snapshot_hint() -> Result<()> {
    let fixture = TestFixture::new().await;
    fixture.create_default_table().await?;

    let table = fixture.latest_default_table().await?;

    let num_blocks = 1;
    let rows_per_block = 1;
    let value_start_from = 1;
    let stream =
        TestFixture::gen_sample_blocks_stream_ex(num_blocks, rows_per_block, value_start_from);

    let blocks = stream.try_collect().await?;
    fixture
        .append_commit_blocks(table.clone(), blocks, false, true)
        .await?;

    // check last snapshot hit file
    let table = fixture.latest_default_table().await?;
    let fuse_table = FuseTable::try_from_table(table.as_ref())?;
    let last_snapshot_location = fuse_table.snapshot_loc().await?.unwrap();
    let operator = fuse_table.get_operator();
    let location = fuse_table
        .meta_location_generator()
        .gen_last_snapshot_hint_location();
    let storage_meta_data = operator.metadata();
    let storage_prefix = storage_meta_data.root();

    let expected = format!("{}{}", storage_prefix, last_snapshot_location);
    let content = operator.object(location.as_str()).read().await?;

    assert_eq!(content.as_slice(), expected.as_bytes());

    Ok(())
}

#[tokio::test]
async fn test_abort_on_error() -> Result<()> {
    struct Case {
        update_meta_error: Option<ErrorCode>,
        expected_error: Option<ErrorCode>,
        expected_snapshot_left: usize,
        case_name: &'static str,
        max_retry_time: Option<Duration>,
    }

    impl Case {
        async fn run(&self) -> Result<()> {
            let fixture = TestFixture::new().await;
            fixture.create_default_table().await?;
            let ctx = fixture.ctx();
            let catalog = ctx.get_catalog("default")?;

            let table = fixture.latest_default_table().await?;
            let fuse_table = FuseTable::try_from_table(table.as_ref())?;

            let overwrite = false;
            let log = vec![AppendOperationLogEntry {
                segment_location: "do not care".to_string(),
                segment_info: Arc::new(SegmentInfo::new(vec![], Statistics::default())),
            }];

            let faked_catalog = FakedCatalog {
                cat: catalog,
                error_injection: self.update_meta_error.clone(),
            };
            let ctx = Arc::new(CtxDelegation::new(ctx, faked_catalog));
            let r = fuse_table
                .commit_with_max_retry_elapsed(ctx, log, self.max_retry_time, overwrite)
                .await;
            if self.update_meta_error.is_some() {
                assert_eq!(
                    r.unwrap_err().code(),
                    self.expected_error.as_ref().unwrap().code(),
                    "case name {}",
                    self.case_name
                );
            } else {
                assert!(r.is_ok(), "case name {}", self.case_name);
            }

            let operator = fuse_table.get_operator();
            let table_data_prefix = fuse_table.meta_location_generator().prefix();
            let storage_meta_data = operator.metadata();
            let storage_prefix = storage_meta_data.root();

            let mut ss_count = 0;
            // check snapshot dir
            for entry in WalkDir::new(format!(
                "{}/{}/{}",
                storage_prefix, table_data_prefix, FUSE_TBL_SNAPSHOT_PREFIX
            )) {
                let entry = entry.unwrap();
                if entry.file_type().is_file() {
                    ss_count += 1;
                }
            }
            assert_eq!(
                ss_count, self.expected_snapshot_left,
                "case name {}",
                self.case_name
            );
            Ok(())
        }
    }

    {
        let injected_error = None;
        // no error, expect one snapshot left there
        let expected_snapshot_left = 1;
        let case = Case {
            update_meta_error: injected_error.clone(),
            expected_error: injected_error,
            expected_snapshot_left,
            case_name: "normal, not meta store error",
            max_retry_time: None,
        };
        case.run().await?;
    }

    {
        let injected_error = Some(ErrorCode::MetaStorageError("does not matter".to_owned()));
        // error may have side effects, expect one snapshot
        // left there (snapshot not removed if committing failed)
        // in case the meta store state changed (by this operation)
        let expected_snapshot_left = 1;
        let case = Case {
            update_meta_error: injected_error.clone(),
            expected_error: injected_error,
            expected_snapshot_left,
            case_name: "meta store error which may have side effects",
            max_retry_time: None,
        };
        case.run().await?;
    }

    {
        let injected_error = Some(ErrorCode::TableVersionMismatched(
            "does not matter".to_owned(),
        ));
        // if commit failed and end up with TableVersionMismatched error,
        // an OCCRetryFailure is expected to be popped up
        let expected_error = Some(ErrorCode::OCCRetryFailure("not matter".to_owned()));
        // error may have no side effects, expect no snapshot, the operation be reverted
        let expected_snapshot_left = 0;
        let case = Case {
            update_meta_error: injected_error,
            expected_error,
            expected_snapshot_left,
            case_name: "table version mismatch err",
            // shrink the test time usage
            max_retry_time: Some(Duration::from_millis(100)),
        };
        case.run().await?;
    }

    Ok(())
}

struct CtxDelegation {
    ctx: Arc<dyn TableContext>,
    catalog: Arc<FakedCatalog>,
}

impl CtxDelegation {
    fn new(ctx: Arc<QueryContext>, faked_cat: FakedCatalog) -> Self {
        Self {
            ctx,
            catalog: Arc::new(faked_cat),
        }
    }
}

#[async_trait::async_trait]
impl TableContext for CtxDelegation {
    fn build_table_from_source_plan(&self, _plan: &DataSourcePlan) -> Result<Arc<dyn Table>> {
        todo!()
    }

    fn get_scan_progress(&self) -> Arc<Progress> {
        todo!()
    }

    fn get_scan_progress_value(&self) -> ProgressValues {
        todo!()
    }

    fn get_write_progress(&self) -> Arc<Progress> {
        self.ctx.get_write_progress()
    }

    fn get_write_progress_value(&self) -> ProgressValues {
        todo!()
    }

    fn get_result_progress(&self) -> Arc<Progress> {
        todo!()
    }

    fn get_result_progress_value(&self) -> ProgressValues {
        todo!()
    }

    fn try_get_part(&self) -> Option<PartInfoPtr> {
        todo!()
    }

    fn try_set_partitions(&self, _partitions: Partitions) -> Result<()> {
        todo!()
    }

    fn attach_query_str(&self, _kind: String, _query: &str) {
        todo!()
    }

    fn get_fragment_id(&self) -> usize {
        todo!()
    }

    fn get_catalog(&self, _catalog_name: &str) -> Result<Arc<dyn Catalog>> {
        Ok(self.catalog.clone())
    }

    fn get_id(&self) -> String {
        self.ctx.get_id()
    }

    fn get_current_catalog(&self) -> String {
        "default".to_owned()
    }

    fn get_aborting(&self) -> Arc<AtomicBool> {
        todo!()
    }

    fn get_current_database(&self) -> String {
        self.ctx.get_current_database()
    }

    fn get_current_user(&self) -> Result<UserInfo> {
        todo!()
    }

    fn get_current_role(&self) -> Option<RoleInfo> {
        todo!()
    }

    fn get_fuse_version(&self) -> String {
        todo!()
    }

    fn get_changed_settings(&self) -> Arc<Settings> {
        todo!()
    }

    fn apply_changed_settings(&self, _changed_settings: Arc<Settings>) -> Result<()> {
        todo!()
    }

    fn get_format_settings(&self) -> Result<FormatSettings> {
        todo!()
    }

    fn get_tenant(&self) -> String {
        self.ctx.get_tenant()
    }

    fn get_query_str(&self) -> String {
        todo!()
    }

    fn get_query_kind(&self) -> String {
        todo!()
    }

    fn get_data_operator(&self) -> Result<DataOperator> {
        self.ctx.get_data_operator()
    }

    fn push_precommit_block(&self, _block: DataBlock) {
        todo!()
    }

    fn consume_precommit_blocks(&self) -> Vec<DataBlock> {
        todo!()
    }

    fn try_get_function_context(&self) -> Result<FunctionContext> {
        todo!()
    }

    fn get_connection_id(&self) -> String {
        todo!()
    }

    fn get_settings(&self) -> Arc<Settings> {
        todo!()
    }

    fn get_cluster(&self) -> Arc<Cluster> {
        todo!()
    }

    async fn get_table(
        &self,
        _catalog: &str,
        _database: &str,
        _table: &str,
    ) -> Result<Arc<dyn Table>> {
        todo!()
    }

    fn get_processes_info(&self) -> Vec<ProcessInfo> {
        todo!()
    }

    fn get_stage_attachment(&self) -> Option<StageAttachment> {
        todo!()
    }
}

#[derive(Clone)]
struct FakedCatalog {
    cat: Arc<dyn Catalog>,
    error_injection: Option<ErrorCode>,
}

#[async_trait::async_trait]
impl Catalog for FakedCatalog {
    async fn get_database(&self, _tenant: &str, _db_name: &str) -> Result<Arc<dyn Database>> {
        todo!()
    }

    async fn list_databases(&self, _tenant: &str) -> Result<Vec<Arc<dyn Database>>> {
        todo!()
    }

    async fn create_database(&self, _req: CreateDatabaseReq) -> Result<CreateDatabaseReply> {
        todo!()
    }

    async fn drop_database(&self, _req: DropDatabaseReq) -> Result<()> {
        todo!()
    }

    async fn undrop_database(&self, _req: UndropDatabaseReq) -> Result<UndropDatabaseReply> {
        todo!()
    }

    async fn rename_database(&self, _req: RenameDatabaseReq) -> Result<RenameDatabaseReply> {
        todo!()
    }

    fn get_table_by_info(&self, table_info: &TableInfo) -> Result<Arc<dyn Table>> {
        self.cat.get_table_by_info(table_info)
    }

    async fn get_table_meta_by_id(&self, table_id: MetaId) -> Result<(TableIdent, Arc<TableMeta>)> {
        self.cat.get_table_meta_by_id(table_id).await
    }

    async fn get_table(
        &self,
        _tenant: &str,
        _db_name: &str,
        _table_name: &str,
    ) -> Result<Arc<dyn Table>> {
        todo!()
    }

    async fn list_tables(&self, _tenant: &str, _db_name: &str) -> Result<Vec<Arc<dyn Table>>> {
        todo!()
    }

    async fn list_tables_history(
        &self,
        _tenant: &str,
        _db_name: &str,
    ) -> Result<Vec<Arc<dyn Table>>> {
        todo!()
    }

    async fn create_table(&self, _req: CreateTableReq) -> Result<()> {
        todo!()
    }

    async fn drop_table(&self, _req: DropTableReq) -> Result<DropTableReply> {
        todo!()
    }

    async fn undrop_table(&self, _req: UndropTableReq) -> Result<UndropTableReply> {
        todo!()
    }

    async fn rename_table(&self, _req: RenameTableReq) -> Result<RenameTableReply> {
        todo!()
    }

    async fn upsert_table_option(
        &self,
        _tenant: &str,
        _db_name: &str,
        _req: UpsertTableOptionReq,
    ) -> Result<UpsertTableOptionReply> {
        todo!()
    }

    async fn update_table_meta(
        &self,
        table_info: &TableInfo,
        req: UpdateTableMetaReq,
    ) -> Result<UpdateTableMetaReply> {
        if let Some(e) = &self.error_injection {
            Err(e.clone())
        } else {
            self.cat.update_table_meta(table_info, req).await
        }
    }

    async fn count_tables(&self, _req: CountTablesReq) -> Result<CountTablesReply> {
        todo!()
    }

    async fn get_table_copied_file_info(
        &self,
        _tenant: &str,
        _db_name: &str,
        _req: GetTableCopiedFileReq,
    ) -> Result<GetTableCopiedFileReply> {
        todo!()
    }

    async fn upsert_table_copied_file_info(
        &self,
        _tenant: &str,
        _db_name: &str,
        _req: UpsertTableCopiedFileReq,
    ) -> Result<UpsertTableCopiedFileReply> {
        todo!()
    }

    async fn truncate_table(
        &self,
        _table_info: &TableInfo,
        _req: TruncateTableReq,
    ) -> Result<TruncateTableReply> {
        todo!()
    }

    fn as_any(&self) -> &dyn Any {
        todo!()
    }
}
