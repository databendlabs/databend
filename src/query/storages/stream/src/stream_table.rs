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

use std::any::Any;
use std::sync::Arc;

use databend_common_catalog::catalog::StorageDescription;
use databend_common_catalog::plan::DataSourcePlan;
use databend_common_catalog::plan::PartStatistics;
use databend_common_catalog::plan::Partitions;
use databend_common_catalog::plan::PushDownInfo;
use databend_common_catalog::plan::StreamColumn;
use databend_common_catalog::table::ColumnStatisticsProvider;
use databend_common_catalog::table::Table;
use databend_common_catalog::table::TableStatistics;
use databend_common_catalog::table_context::TableContext;
use databend_common_exception::ErrorCode;
use databend_common_exception::Result;
use databend_common_expression::ColumnId;
use databend_common_expression::BASE_BLOCK_IDS_COLUMN_ID;
use databend_common_expression::BASE_ROW_ID_COLUMN_ID;
use databend_common_expression::ORIGIN_BLOCK_ID_COL_NAME;
use databend_common_expression::ORIGIN_BLOCK_ROW_NUM_COL_NAME;
use databend_common_expression::ORIGIN_VERSION_COL_NAME;
use databend_common_expression::ROW_VERSION_COL_NAME;
use databend_common_meta_app::schema::TableInfo;
use databend_common_pipeline_core::Pipeline;
use databend_common_sql::binder::STREAM_COLUMN_FACTORY;
use databend_common_storages_fuse::FuseTable;
use databend_storages_common_table_meta::table::ChangeType;
use databend_storages_common_table_meta::table::StreamMode;
use databend_storages_common_table_meta::table::OPT_KEY_DATABASE_NAME;
use databend_storages_common_table_meta::table::OPT_KEY_MODE;
use databend_storages_common_table_meta::table::OPT_KEY_SNAPSHOT_LOCATION;
use databend_storages_common_table_meta::table::OPT_KEY_TABLE_ID;
use databend_storages_common_table_meta::table::OPT_KEY_TABLE_NAME;
use databend_storages_common_table_meta::table::OPT_KEY_TABLE_VER;

pub const STREAM_ENGINE: &str = "STREAM";

pub enum StreamStatus {
    MayHaveData,
    NoData,
}

pub struct StreamTable {
    stream_info: TableInfo,

    table_id: u64,
    table_name: String,
    table_database: String,
    table_version: u64,
    mode: StreamMode,
    snapshot_location: Option<String>,
}

impl StreamTable {
    pub fn try_create(table_info: TableInfo) -> Result<Box<dyn Table>> {
        let options = table_info.options();
        let table_name = options
            .get(OPT_KEY_TABLE_NAME)
            .ok_or_else(|| ErrorCode::Internal("table name must be set"))?
            .clone();
        let table_database = options
            .get(OPT_KEY_DATABASE_NAME)
            .ok_or_else(|| ErrorCode::Internal("table database must be set"))?
            .clone();
        let table_id = options
            .get(OPT_KEY_TABLE_ID)
            .ok_or_else(|| ErrorCode::Internal("table id must be set"))?
            .parse::<u64>()?;
        let table_version = options
            .get(OPT_KEY_TABLE_VER)
            .ok_or_else(|| ErrorCode::Internal("table version must be set"))?
            .parse::<u64>()?;
        let mode = options
            .get(OPT_KEY_MODE)
            .and_then(|s| s.parse::<StreamMode>().ok())
            .unwrap_or(StreamMode::AppendOnly);
        let snapshot_location = options.get(OPT_KEY_SNAPSHOT_LOCATION).cloned();
        Ok(Box::new(StreamTable {
            stream_info: table_info,
            table_name,
            table_database,
            table_id,
            table_version,
            mode,
            snapshot_location,
        }))
    }

    pub fn description() -> StorageDescription {
        StorageDescription {
            engine_name: STREAM_ENGINE.to_string(),
            comment: "STREAM Storage Engine".to_string(),
            ..Default::default()
        }
    }

    pub fn try_from_table(tbl: &dyn Table) -> Result<&StreamTable> {
        tbl.as_any().downcast_ref::<StreamTable>().ok_or_else(|| {
            ErrorCode::Internal(format!(
                "expects table of engine STREAM, but got {}",
                tbl.engine()
            ))
        })
    }

    pub async fn source_table(&self, ctx: Arc<dyn TableContext>) -> Result<Arc<dyn Table>> {
        let table = ctx
            .get_table(
                self.stream_info.catalog(),
                &self.table_database,
                &self.table_name,
            )
            .await?;

        if table.get_table_info().ident.table_id != self.table_id {
            return Err(ErrorCode::IllegalStream(format!(
                "Base table '{}'.'{}' dropped, cannot read from stream {}",
                self.table_database, self.table_name, self.stream_info.desc,
            )));
        }

        let fuse_table = FuseTable::try_from_table(table.as_ref())?;
        fuse_table.check_changes_valid(
            &self.table_database,
            &self.table_name,
            self.table_version,
        )?;

        Ok(table)
    }

    pub fn offset(&self) -> u64 {
        self.table_version
    }

    pub fn mode(&self) -> StreamMode {
        self.mode.clone()
    }

    pub fn snapshot_loc(&self) -> Option<String> {
        self.snapshot_location.clone()
    }

    pub fn source_table_name(&self) -> &str {
        &self.table_name
    }

    pub fn source_table_id(&self) -> u64 {
        self.table_id
    }

    pub fn source_table_database(&self) -> &str {
        &self.table_database
    }

    #[async_backtrace::framed]
    async fn do_read_partitions(
        &self,
        ctx: Arc<dyn TableContext>,
        push_downs: Option<PushDownInfo>,
    ) -> Result<(PartStatistics, Partitions)> {
        let table = self.source_table(ctx.clone()).await?;
        let fuse_table = FuseTable::try_from_table(table.as_ref())?;
        let change_type = push_downs.as_ref().map_or(ChangeType::Append, |v| {
            v.change_type.clone().unwrap_or(ChangeType::Append)
        });
        fuse_table
            .do_read_changes_partitions(ctx, push_downs, change_type, &self.snapshot_location)
            .await
    }

    #[minitrace::trace]
    pub async fn check_stream_status(&self, ctx: Arc<dyn TableContext>) -> Result<StreamStatus> {
        let base_table = self.source_table(ctx).await?;
        let status = if base_table.get_table_info().ident.seq == self.table_version {
            StreamStatus::NoData
        } else {
            StreamStatus::MayHaveData
        };
        Ok(status)
    }
}

#[async_trait::async_trait]
impl Table for StreamTable {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn get_table_info(&self) -> &TableInfo {
        &self.stream_info
    }

    fn supported_internal_column(&self, column_id: ColumnId) -> bool {
        (BASE_BLOCK_IDS_COLUMN_ID..=BASE_ROW_ID_COLUMN_ID).contains(&column_id)
    }

    /// whether column prune(projection) can help in table read
    fn support_column_projection(&self) -> bool {
        true
    }

    fn stream_columns(&self) -> Vec<StreamColumn> {
        vec![
            STREAM_COLUMN_FACTORY
                .get_stream_column(ORIGIN_VERSION_COL_NAME)
                .unwrap(),
            STREAM_COLUMN_FACTORY
                .get_stream_column(ORIGIN_BLOCK_ID_COL_NAME)
                .unwrap(),
            STREAM_COLUMN_FACTORY
                .get_stream_column(ORIGIN_BLOCK_ROW_NUM_COL_NAME)
                .unwrap(),
            STREAM_COLUMN_FACTORY
                .get_stream_column(ROW_VERSION_COL_NAME)
                .unwrap(),
        ]
    }

    #[minitrace::trace]
    #[async_backtrace::framed]
    async fn read_partitions(
        &self,
        ctx: Arc<dyn TableContext>,
        push_downs: Option<PushDownInfo>,
        _dry_run: bool,
    ) -> Result<(PartStatistics, Partitions)> {
        self.do_read_partitions(ctx, push_downs).await
    }

    #[minitrace::trace]
    fn read_data(
        &self,
        ctx: Arc<dyn TableContext>,
        plan: &DataSourcePlan,
        pipeline: &mut Pipeline,
        put_cache: bool,
    ) -> Result<()> {
        let table = databend_common_base::runtime::block_on(ctx.get_table(
            self.stream_info.catalog(),
            &self.table_database,
            &self.table_name,
        ))?;
        table.read_data(ctx, plan, pipeline, put_cache)
    }

    async fn table_statistics(
        &self,
        ctx: Arc<dyn TableContext>,
        change_type: Option<ChangeType>,
    ) -> Result<Option<TableStatistics>> {
        let table = self.source_table(ctx.clone()).await?;
        let fuse_table = FuseTable::try_from_table(table.as_ref())?;
        let change_type = change_type.unwrap_or(ChangeType::Append);
        fuse_table
            .changes_table_statistics(ctx, &self.snapshot_location, change_type)
            .await
    }

    #[async_backtrace::framed]
    async fn column_statistics_provider(
        &self,
        ctx: Arc<dyn TableContext>,
    ) -> Result<Box<dyn ColumnStatisticsProvider>> {
        let table = self.source_table(ctx.clone()).await?;
        table.column_statistics_provider(ctx).await
    }

    #[async_backtrace::framed]
    async fn generage_changes_query(
        &self,
        ctx: Arc<dyn TableContext>,
        database_name: &str,
        table_name: &str,
    ) -> Result<String> {
        let table = self.source_table(ctx).await?;
        let fuse_table = FuseTable::try_from_table(table.as_ref())?;
        fuse_table
            .get_changes_query(
                &self.mode,
                &self.snapshot_location,
                format!("{}.{}", database_name, table_name),
                self.offset(),
            )
            .await
    }
}
