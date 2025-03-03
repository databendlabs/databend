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
use std::collections::BTreeMap;
use std::collections::HashMap;
use std::collections::HashSet;
use std::hash::RandomState;
use std::str;
use std::str::FromStr;
use std::sync::Arc;
use std::time::Instant;

use async_channel::Receiver;
use chrono::Duration;
use chrono::TimeDelta;
use databend_common_base::runtime::GlobalIORuntime;
use databend_common_catalog::catalog::StorageDescription;
use databend_common_catalog::plan::DataSourcePlan;
use databend_common_catalog::plan::PartInfoPtr;
use databend_common_catalog::plan::PartStatistics;
use databend_common_catalog::plan::Partitions;
use databend_common_catalog::plan::PushDownInfo;
use databend_common_catalog::plan::ReclusterParts;
use databend_common_catalog::plan::StreamColumn;
use databend_common_catalog::table::Bound;
use databend_common_catalog::table::ColumnRange;
use databend_common_catalog::table::ColumnStatisticsProvider;
use databend_common_catalog::table::CompactionLimits;
use databend_common_catalog::table::DistributionLevel;
use databend_common_catalog::table::NavigationDescriptor;
use databend_common_catalog::table::TimeNavigation;
use databend_common_catalog::table_context::AbortChecker;
use databend_common_catalog::table_context::TableContext;
use databend_common_exception::ErrorCode;
use databend_common_exception::Result;
use databend_common_expression::types::DataType;
use databend_common_expression::BlockThresholds;
use databend_common_expression::ColumnId;
use databend_common_expression::RemoteExpr;
use databend_common_expression::TableField;
use databend_common_expression::TableSchema;
use databend_common_expression::ORIGIN_BLOCK_ID_COL_NAME;
use databend_common_expression::ORIGIN_BLOCK_ROW_NUM_COL_NAME;
use databend_common_expression::ORIGIN_VERSION_COL_NAME;
use databend_common_expression::ROW_VERSION_COL_NAME;
use databend_common_expression::SEARCH_SCORE_COLUMN_ID;
use databend_common_io::constants::DEFAULT_BLOCK_BUFFER_SIZE;
use databend_common_io::constants::DEFAULT_BLOCK_MAX_ROWS;
use databend_common_meta_app::schema::DatabaseType;
use databend_common_meta_app::schema::TableInfo;
use databend_common_meta_app::schema::UpdateStreamMetaReq;
use databend_common_meta_app::schema::UpsertTableCopiedFileReq;
use databend_common_pipeline_core::Pipeline;
use databend_common_sql::binder::STREAM_COLUMN_FACTORY;
use databend_common_sql::parse_cluster_keys;
use databend_common_sql::plans::TruncateMode;
use databend_common_sql::BloomIndexColumns;
use databend_common_storage::init_operator;
use databend_common_storage::DataOperator;
use databend_common_storage::StorageMetrics;
use databend_common_storage::StorageMetricsLayer;
use databend_storages_common_cache::LoadParams;
use databend_storages_common_io::Files;
use databend_storages_common_table_meta::meta::parse_storage_prefix;
use databend_storages_common_table_meta::meta::ClusterKey;
use databend_storages_common_table_meta::meta::CompactSegmentInfo;
use databend_storages_common_table_meta::meta::SnapshotId;
use databend_storages_common_table_meta::meta::TableMetaTimestamps;
use databend_storages_common_table_meta::meta::TableSnapshot;
use databend_storages_common_table_meta::meta::TableSnapshotStatistics;
use databend_storages_common_table_meta::meta::Versioned;
use databend_storages_common_table_meta::table::ChangeType;
use databend_storages_common_table_meta::table::ClusterType;
use databend_storages_common_table_meta::table::TableCompression;
use databend_storages_common_table_meta::table::OPT_KEY_BLOOM_INDEX_COLUMNS;
use databend_storages_common_table_meta::table::OPT_KEY_CHANGE_TRACKING;
use databend_storages_common_table_meta::table::OPT_KEY_CLUSTER_TYPE;
use databend_storages_common_table_meta::table::OPT_KEY_LEGACY_SNAPSHOT_LOC;
use databend_storages_common_table_meta::table::OPT_KEY_SNAPSHOT_LOCATION;
use databend_storages_common_table_meta::table::OPT_KEY_SNAPSHOT_LOCATION_FIXED_FLAG;
use databend_storages_common_table_meta::table::OPT_KEY_STORAGE_FORMAT;
use databend_storages_common_table_meta::table::OPT_KEY_TABLE_ATTACHED_DATA_URI;
use databend_storages_common_table_meta::table::OPT_KEY_TABLE_COMPRESSION;
use futures_util::TryStreamExt;
use itertools::Itertools;
use log::info;
use log::warn;
use opendal::Operator;
use parking_lot::Mutex;

use crate::fuse_column::FuseTableColumnStatisticsProvider;
use crate::fuse_type::FuseTableType;
use crate::io::MetaReaders;
use crate::io::SegmentsIO;
use crate::io::TableMetaLocationGenerator;
use crate::io::TableSnapshotReader;
use crate::io::WriteSettings;
use crate::operations::load_last_snapshot_hint;
use crate::operations::ChangesDesc;
use crate::operations::SnapshotHint;
use crate::statistics::reduce_block_statistics;
use crate::statistics::Trim;
use crate::FuseStorageFormat;
use crate::NavigationPoint;
use crate::Table;
use crate::TableStatistics;
use crate::DEFAULT_BLOCK_PER_SEGMENT;
use crate::DEFAULT_ROW_PER_PAGE;
use crate::DEFAULT_ROW_PER_PAGE_FOR_BLOCKING;
use crate::FUSE_OPT_KEY_ATTACH_COLUMN_IDS;
use crate::FUSE_OPT_KEY_BLOCK_IN_MEM_SIZE_THRESHOLD;
use crate::FUSE_OPT_KEY_BLOCK_PER_SEGMENT;
use crate::FUSE_OPT_KEY_DATA_RETENTION_PERIOD_IN_HOURS;
use crate::FUSE_OPT_KEY_ROW_PER_BLOCK;
use crate::FUSE_OPT_KEY_ROW_PER_PAGE;

#[derive(Clone)]
pub struct FuseTable {
    pub(crate) table_info: TableInfo,
    pub(crate) meta_location_generator: TableMetaLocationGenerator,

    pub(crate) cluster_key_meta: Option<ClusterKey>,
    pub(crate) storage_format: FuseStorageFormat,
    pub(crate) table_compression: TableCompression,
    pub(crate) bloom_index_cols: BloomIndexColumns,

    pub(crate) operator: Operator,
    pub(crate) data_metrics: Arc<StorageMetrics>,

    table_type: FuseTableType,

    // If this is set, reading from fuse_table should only return the increment blocks
    pub(crate) changes_desc: Option<ChangesDesc>,

    pub(crate) pruned_result_receiver: Arc<Mutex<PartInfoReceiver>>,
}

type PartInfoReceiver = Option<Receiver<Result<PartInfoPtr>>>;

// default schema refreshing timeout is 5 seconds.
impl FuseTable {
    pub fn try_create(table_info: TableInfo) -> Result<Box<dyn Table>> {
        Ok(Self::do_create_table_ext(table_info, false)?)
    }

    pub fn do_create(table_info: TableInfo) -> Result<Box<FuseTable>> {
        Self::do_create_table_ext(table_info, true)
    }

    pub fn try_create_ext(table_info: TableInfo, disable_refresh: bool) -> Result<Box<dyn Table>> {
        Ok(Self::do_create_table_ext(table_info, disable_refresh)?)
    }

    pub fn do_create_table_ext(
        mut table_info: TableInfo,
        disable_refresh: bool,
    ) -> Result<Box<FuseTable>> {
        let storage_prefix = Self::parse_storage_prefix_from_table_info(&table_info)?;
        let cluster_key_meta = table_info.cluster_key();
        let (mut operator, table_type) = match table_info.db_type.clone() {
            DatabaseType::NormalDB => {
                let storage_params = table_info.meta.storage_params.clone();
                match storage_params {
                    // External or attached table.
                    Some(sp) => {
                        let table_meta_options = &table_info.meta.options;
                        let operator = init_operator(&sp)?;
                        let table_type = if Self::is_table_attached(table_meta_options) {
                            if !disable_refresh {
                                Self::refresh_table_info(
                                    &mut table_info,
                                    &operator,
                                    &storage_prefix,
                                )?;
                            }
                            FuseTableType::Attached
                        } else {
                            FuseTableType::External
                        };

                        (operator, table_type)
                    }
                    // Normal table.
                    None => {
                        let operator = DataOperator::instance().operator();
                        (operator, FuseTableType::Standard)
                    }
                }
            }
        };

        let data_metrics = Arc::new(StorageMetrics::default());
        operator = operator.layer(StorageMetricsLayer::new(data_metrics.clone()));

        let storage_format = table_info
            .options()
            .get(OPT_KEY_STORAGE_FORMAT)
            .cloned()
            .unwrap_or_default();

        let table_compression = table_info
            .options()
            .get(OPT_KEY_TABLE_COMPRESSION)
            .cloned()
            .unwrap_or_default();

        let bloom_index_cols = table_info
            .options()
            .get(OPT_KEY_BLOOM_INDEX_COLUMNS)
            .and_then(|s| s.parse::<BloomIndexColumns>().ok())
            .unwrap_or(BloomIndexColumns::All);

        let meta_location_generator = TableMetaLocationGenerator::new(storage_prefix);
        if !table_info.meta.part_prefix.is_empty() {
            return Err(ErrorCode::StorageOther(
                "Location_prefix no longer supported. The last version that supports it is: https://github.com/databendlabs/databend/releases/tag/v1.2.653-nightly",
            ));
        }

        Ok(Box::new(FuseTable {
            table_info,
            meta_location_generator,
            cluster_key_meta,
            bloom_index_cols,
            operator,
            data_metrics,
            storage_format: FuseStorageFormat::from_str(storage_format.as_str())?,
            table_compression: table_compression.as_str().try_into()?,
            table_type,
            changes_desc: None,
            pruned_result_receiver: Arc::new(Mutex::new(None)),
        }))
    }

    pub fn description() -> StorageDescription {
        StorageDescription {
            engine_name: "FUSE".to_string(),
            comment: "FUSE Storage Engine".to_string(),
            support_cluster_key: true,
        }
    }

    pub fn is_native(&self) -> bool {
        matches!(self.storage_format, FuseStorageFormat::Native)
    }

    pub fn meta_location_generator(&self) -> &TableMetaLocationGenerator {
        &self.meta_location_generator
    }

    pub fn get_write_settings(&self) -> WriteSettings {
        let default_rows_per_page = if self.operator.info().native_capability().blocking {
            DEFAULT_ROW_PER_PAGE_FOR_BLOCKING
        } else {
            DEFAULT_ROW_PER_PAGE
        };
        let max_page_size = self.get_option(FUSE_OPT_KEY_ROW_PER_PAGE, default_rows_per_page);
        let block_per_seg =
            self.get_option(FUSE_OPT_KEY_BLOCK_PER_SEGMENT, DEFAULT_BLOCK_PER_SEGMENT);

        WriteSettings {
            storage_format: self.storage_format,
            table_compression: self.table_compression,
            max_page_size,
            block_per_seg,
        }
    }

    /// Get max page size.
    /// For native storage format.
    pub fn get_max_page_size(&self) -> Option<usize> {
        match self.storage_format {
            FuseStorageFormat::Parquet => None,
            FuseStorageFormat::Native => Some(self.get_write_settings().max_page_size),
        }
    }

    pub fn parse_storage_prefix_from_table_info(table_info: &TableInfo) -> Result<String> {
        parse_storage_prefix(table_info.options(), table_info.ident.table_id)
    }
    #[fastrace::trace]
    #[async_backtrace::framed]
    pub async fn read_table_snapshot_statistics(
        &self,
        snapshot: Option<&Arc<TableSnapshot>>,
    ) -> Result<Option<Arc<TableSnapshotStatistics>>> {
        match snapshot {
            Some(snapshot) => {
                if let Some(loc) = &snapshot.table_statistics_location {
                    let reader = MetaReaders::table_snapshot_statistics_reader(self.get_operator());

                    let ver = TableMetaLocationGenerator::table_statistics_version(loc);
                    let load_params = LoadParams {
                        location: loc.clone(),
                        len_hint: None,
                        ver,
                        put_cache: true,
                    };

                    Ok(Some(reader.read(&load_params).await?))
                } else {
                    Ok(None)
                }
            }
            None => Ok(None),
        }
    }

    #[fastrace::trace]
    #[async_backtrace::framed]
    pub async fn read_table_snapshot(&self) -> Result<Option<Arc<TableSnapshot>>> {
        let reader = MetaReaders::table_snapshot_reader(self.get_operator());
        let loc = self.snapshot_loc();
        let ver = self.snapshot_format_version(loc.clone())?;
        Self::read_table_snapshot_with_reader(reader, loc, ver).await
    }

    #[fastrace::trace]
    #[async_backtrace::framed]
    pub async fn read_table_snapshot_without_cache(&self) -> Result<Option<Arc<TableSnapshot>>> {
        let reader = MetaReaders::table_snapshot_reader_without_cache(self.get_operator());
        let loc = self.snapshot_loc();
        let ver = self.snapshot_format_version(loc.clone())?;
        Self::read_table_snapshot_with_reader(reader, loc, ver).await
    }

    async fn read_table_snapshot_with_reader(
        reader: TableSnapshotReader,
        snapshot_location: Option<String>,
        ver: u64,
    ) -> Result<Option<Arc<TableSnapshot>>> {
        if let Some(location) = snapshot_location {
            let params = LoadParams {
                location,
                len_hint: None,
                ver,
                put_cache: true,
            };
            Ok(Some(reader.read(&params).await?))
        } else {
            Ok(None)
        }
    }

    #[async_backtrace::framed]
    pub fn snapshot_format_version(&self, location_opt: Option<String>) -> Result<u64> {
        let location_opt = if location_opt.is_some() {
            location_opt
        } else {
            self.snapshot_loc()
        };
        // If no snapshot location here, indicates that there are no data of this table yet
        // in this case, we just return the current snapshot version
        Ok(location_opt.map_or(TableSnapshot::VERSION, |loc| {
            TableMetaLocationGenerator::snapshot_version(loc.as_str())
        }))
    }

    pub fn snapshot_loc(&self) -> Option<String> {
        let options = self.table_info.options();
        options
            .get(OPT_KEY_SNAPSHOT_LOCATION)
            // for backward compatibility, we check the legacy table option
            .or_else(|| options.get(OPT_KEY_LEGACY_SNAPSHOT_LOC))
            .cloned()
    }

    pub fn get_operator(&self) -> Operator {
        self.operator.clone()
    }

    pub fn get_operator_ref(&self) -> &Operator {
        &self.operator
    }

    pub fn try_from_table(tbl: &dyn Table) -> Result<&FuseTable> {
        tbl.as_any().downcast_ref::<FuseTable>().ok_or_else(|| {
            ErrorCode::Internal(format!(
                "expects table of engine FUSE, but got {}",
                tbl.engine()
            ))
        })
    }

    pub fn is_transient(&self) -> bool {
        self.table_info.meta.options.contains_key("TRANSIENT")
    }

    pub fn cluster_key_str(&self) -> Option<&String> {
        self.cluster_key_meta.as_ref().map(|(_, key)| key)
    }

    pub fn cluster_key_id(&self) -> Option<u32> {
        self.cluster_key_meta.clone().map(|v| v.0)
    }

    pub fn linear_cluster_keys(&self, ctx: Arc<dyn TableContext>) -> Vec<RemoteExpr<String>> {
        if self
            .cluster_type()
            .is_none_or(|v| matches!(v, ClusterType::Hilbert))
        {
            return vec![];
        }

        let table_meta = Arc::new(self.clone());
        let cluster_key_exprs = self.resolve_cluster_keys(ctx.clone()).unwrap();
        let exprs = parse_cluster_keys(ctx, table_meta.clone(), cluster_key_exprs).unwrap();
        let cluster_keys = exprs
            .iter()
            .map(|k| {
                k.project_column_ref(|index| table_meta.schema().field(*index).name().to_string())
                    .as_remote_expr()
            })
            .collect();
        cluster_keys
    }

    pub fn bloom_index_cols(&self) -> BloomIndexColumns {
        self.bloom_index_cols.clone()
    }

    // Check if table is attached.
    pub fn is_table_attached(table_meta_options: &BTreeMap<String, String>) -> bool {
        table_meta_options
            .get(OPT_KEY_TABLE_ATTACHED_DATA_URI)
            .is_some()
    }

    pub fn cluster_key_types(&self, ctx: Arc<dyn TableContext>) -> Vec<DataType> {
        let Some(ast_exprs) = self.resolve_cluster_keys(ctx.clone()) else {
            return vec![];
        };
        let cluster_type = self.get_option(OPT_KEY_CLUSTER_TYPE, ClusterType::Linear);
        match cluster_type {
            ClusterType::Hilbert => vec![DataType::Binary],
            ClusterType::Linear => {
                let cluster_keys =
                    parse_cluster_keys(ctx, Arc::new(self.clone()), ast_exprs).unwrap();
                cluster_keys
                    .into_iter()
                    .map(|v| v.data_type().clone())
                    .collect()
            }
        }
    }

    pub fn get_data_retention_period(&self, ctx: &dyn TableContext) -> Result<TimeDelta> {
        let retention_period = if let Some(v) = self
            .table_info
            .meta
            .options
            .get(FUSE_OPT_KEY_DATA_RETENTION_PERIOD_IN_HOURS)
        {
            let retention_period = v.parse::<u64>()?;
            Duration::hours(retention_period as i64)
        } else {
            Duration::days(ctx.get_settings().get_data_retention_time_in_days()? as i64)
        };
        Ok(retention_period)
    }

    pub fn get_storage_format(&self) -> FuseStorageFormat {
        self.storage_format
    }

    pub fn get_storage_prefix(&self) -> &str {
        self.meta_location_generator.prefix()
    }

    fn refresh_schema_from_hint(
        operator: &Operator,
        storage_prefix: &str,
    ) -> Result<Option<(SnapshotHint, TableSchema)>> {
        let refresh_task = async {
            let begin_load_hint = Instant::now();
            let maybe_hint = load_last_snapshot_hint(storage_prefix, operator).await?;
            info!(
                "loaded last snapshot hint, time used {:?}",
                begin_load_hint.elapsed()
            );

            match maybe_hint {
                Some(hint) => {
                    let snapshot_full_path = &hint.snapshot_full_path;
                    let operator_info = operator.info();

                    assert!(snapshot_full_path.starts_with(operator_info.root()));
                    let loc = snapshot_full_path[operator_info.root().len()..].to_string();

                    // refresh table schema by loading the snapshot
                    let begin = Instant::now();
                    let reader = MetaReaders::table_snapshot_reader_without_cache(operator.clone());
                    let ver = TableMetaLocationGenerator::snapshot_version(loc.as_str());
                    let snapshot =
                        Self::read_table_snapshot_with_reader(reader, Some(loc), ver).await?;
                    info!("table snapshot refreshed, time used {:?}", begin.elapsed());

                    let schema = snapshot
                        .ok_or_else(|| {
                            ErrorCode::ShareStorageError(
                                "Failed to load snapshot of read_only attach table".to_string(),
                            )
                        })?
                        .schema
                        .clone();

                    Ok::<_, ErrorCode>(Some((hint, schema)))
                }
                None => {
                    // Table be attached has not last snapshot hint file, treat it as empty table
                    Ok(None)
                }
            }
        };

        GlobalIORuntime::instance().block_on(refresh_task)
    }

    fn refresh_table_info(
        table_info: &mut TableInfo,
        operator: &Operator,
        storage_prefix: &str,
    ) -> Result<()> {
        let table_meta_options = &table_info.meta.options;

        if table_meta_options.contains_key(OPT_KEY_SNAPSHOT_LOCATION_FIXED_FLAG) {
            // If table_info options contains key OPT_KEY_SNAPSHOT_LOCATION_FIXED_FLAG,
            // it means that this table info has been tweaked according to the rules of
            // resolving snapshot location from the hint file, it should not be tweaked again.
            // Otherwise, inconsistent table snapshots may be used while table is being processed in
            // a distributed manner.
            return Ok(());
        }

        info!(
            "extracting snapshot location of table {} with id {:?} from the last snapshot hint file.",
            table_info.desc,
            table_info.ident
        );

        let snapshot_hint = Self::refresh_schema_from_hint(operator, storage_prefix)?;

        info!(
            "extracted snapshot location [{:?}] of table {}, with id {:?} from the last snapshot hint file.",
            snapshot_hint.as_ref().map(|(hint, _)| &hint.snapshot_full_path),
            table_info.desc,
            table_info.ident
        );

        // Adjust snapshot location to the values extracted from the last snapshot hint
        match snapshot_hint {
            None => {
                table_info.options_mut().remove(OPT_KEY_SNAPSHOT_LOCATION);
            }
            Some((hint, base_table_schema)) => {
                let full_location = &hint.snapshot_full_path;

                let operator_info = operator.info();
                assert!(full_location.starts_with(operator_info.root()));
                let location = full_location[operator.info().root().len()..].to_string();

                // update table meta options
                table_info
                    .options_mut()
                    .insert(OPT_KEY_SNAPSHOT_LOCATION.to_string(), location.clone());

                // tweak schema
                if let Some(ids_string) = table_info
                    .schema()
                    .metadata
                    .get(FUSE_OPT_KEY_ATTACH_COLUMN_IDS)
                {
                    // extract ids of column to include
                    let ids: Vec<ColumnId> = ids_string
                        .as_str()
                        .split(",")
                        .map(|s| s.parse::<u32>())
                        .try_collect()?;

                    // retain the columns that are still there
                    let fields: Vec<TableField> = ids
                        .iter()
                        .filter_map(|id| base_table_schema.field_of_column_id(*id).ok().cloned())
                        .collect();

                    if fields.is_empty() {
                        return Err(ErrorCode::StorageOther(format!(
                            "no effective columns found in ATTACH table {}",
                            table_info.desc
                        )));
                    }

                    let mut new_schema = table_info.meta.schema.as_ref().clone();
                    new_schema.metadata = base_table_schema.metadata.clone();
                    new_schema.metadata.insert(
                        FUSE_OPT_KEY_ATTACH_COLUMN_IDS.to_owned(),
                        ids_string.clone(),
                    );
                    new_schema.next_column_id = base_table_schema.next_column_id();
                    new_schema.fields = fields;
                    table_info.meta.schema = Arc::new(new_schema);
                } else {
                    table_info.meta.schema = Arc::new(base_table_schema);
                }

                // tweak table/field comments
                let comments = hint.entity_comments;

                table_info.meta.comment = comments.table_comment;
                // TODO assert about field comments
                table_info.meta.field_comments = comments.field_comments;
            }
        }

        // Mark the snapshot as fixed, indicating it doesn't need to be reloaded from the hint.
        // NOTE:
        // - Attached tables do not commit `table_info` to the meta server,
        //   except when the table is created by a DDL statement for the first time.
        // - As a result, the key `OPT_KEY_SNAPSHOT_LOCATION_FIXED_FLAG` is transient
        //   and will NOT appear when this table is resolved within another query context
        //   for the first time.

        table_info.options_mut().insert(
            OPT_KEY_SNAPSHOT_LOCATION_FIXED_FLAG.to_string(),
            "does not matter".to_string(),
        );
        Ok(())
    }
}

#[async_trait::async_trait]
impl Table for FuseTable {
    fn distribution_level(&self) -> DistributionLevel {
        DistributionLevel::Cluster
    }

    fn as_any(&self) -> &dyn Any {
        self
    }

    fn get_table_info(&self) -> &TableInfo {
        &self.table_info
    }

    fn get_data_metrics(&self) -> Option<Arc<StorageMetrics>> {
        Some(self.data_metrics.clone())
    }

    fn supported_internal_column(&self, column_id: ColumnId) -> bool {
        column_id >= SEARCH_SCORE_COLUMN_ID
    }

    fn support_column_projection(&self) -> bool {
        true
    }

    fn support_distributed_insert(&self) -> bool {
        true
    }

    fn has_exact_total_row_count(&self) -> bool {
        true
    }

    fn storage_format_as_parquet(&self) -> bool {
        matches!(self.storage_format, FuseStorageFormat::Parquet)
    }

    fn cluster_key_meta(&self) -> Option<ClusterKey> {
        self.cluster_key_meta.clone()
    }

    fn change_tracking_enabled(&self) -> bool {
        self.get_option(OPT_KEY_CHANGE_TRACKING, false)
    }

    fn stream_columns(&self) -> Vec<StreamColumn> {
        if self.change_tracking_enabled() {
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
        } else {
            vec![]
        }
    }

    #[fastrace::trace]
    #[async_backtrace::framed]
    async fn read_partitions(
        &self,
        ctx: Arc<dyn TableContext>,
        push_downs: Option<PushDownInfo>,
        dry_run: bool,
    ) -> Result<(PartStatistics, Partitions)> {
        self.do_read_partitions(ctx, push_downs, dry_run).await
    }

    #[fastrace::trace]
    fn read_data(
        &self,
        ctx: Arc<dyn TableContext>,
        plan: &DataSourcePlan,
        pipeline: &mut Pipeline,
        put_cache: bool,
    ) -> Result<()> {
        self.do_read_data(ctx, plan, pipeline, put_cache)
    }

    fn append_data(
        &self,
        ctx: Arc<dyn TableContext>,
        pipeline: &mut Pipeline,
        table_meta_timestamps: TableMetaTimestamps,
    ) -> Result<()> {
        self.do_append_data(ctx, pipeline, table_meta_timestamps)
    }
    fn build_prune_pipeline(
        &self,
        table_ctx: Arc<dyn TableContext>,
        plan: &DataSourcePlan,
        source_pipeline: &mut Pipeline,
    ) -> Result<Option<Pipeline>> {
        self.do_build_prune_pipeline(table_ctx, plan, source_pipeline)
    }

    fn commit_insertion(
        &self,
        ctx: Arc<dyn TableContext>,
        pipeline: &mut Pipeline,
        copied_files: Option<UpsertTableCopiedFileReq>,
        update_stream_meta: Vec<UpdateStreamMetaReq>,
        overwrite: bool,
        prev_snapshot_id: Option<SnapshotId>,
        deduplicated_label: Option<String>,
        table_meta_timestamps: TableMetaTimestamps,
    ) -> Result<()> {
        self.do_commit(
            ctx,
            pipeline,
            copied_files,
            update_stream_meta,
            overwrite,
            prev_snapshot_id,
            deduplicated_label,
            table_meta_timestamps,
        )
    }

    #[fastrace::trace]
    #[async_backtrace::framed]
    async fn truncate(&self, ctx: Arc<dyn TableContext>, pipeline: &mut Pipeline) -> Result<()> {
        self.do_truncate(ctx, pipeline, TruncateMode::Normal).await
    }

    #[fastrace::trace]
    #[async_backtrace::framed]
    async fn purge(
        &self,
        ctx: Arc<dyn TableContext>,
        instant: Option<NavigationPoint>,
        num_snapshot_limit: Option<usize>,
        keep_last_snapshot: bool,
        dry_run: bool,
    ) -> Result<Option<Vec<String>>> {
        match self.navigate_for_purge(&ctx, instant).await {
            Ok((table, files)) => {
                table
                    .do_purge(&ctx, files, num_snapshot_limit, keep_last_snapshot, dry_run)
                    .await
            }
            Err(e) if e.code() == ErrorCode::TABLE_HISTORICAL_DATA_NOT_FOUND => {
                warn!("navigate failed: {:?}", e);
                if dry_run {
                    Ok(Some(vec![]))
                } else {
                    Ok(None)
                }
            }
            Err(e) => Err(e),
        }
    }

    async fn table_statistics(
        &self,
        ctx: Arc<dyn TableContext>,
        require_fresh: bool,
        change_type: Option<ChangeType>,
    ) -> Result<Option<TableStatistics>> {
        if let Some(desc) = &self.changes_desc {
            assert!(change_type.is_some());
            return self
                .changes_table_statistics(ctx, &desc.location, change_type.unwrap())
                .await;
        }

        let stats = match self.table_type {
            FuseTableType::Attached if require_fresh => {
                info!(
                    "refresh table statistics of attached table {}",
                    self.table_info.desc
                );
                let snapshot = self.read_table_snapshot().await?.ok_or_else(|| {
                    // For table created with "ATTACH TABLE ... READ_ONLY"statement, this should be unreachable:
                    // IO or Deserialization related error should have already been thrown, thus
                    // `Internal` error is used.
                    ErrorCode::Internal("Failed to load snapshot of read_only attach table")
                })?;
                let summary = &snapshot.summary;
                TableStatistics {
                    num_rows: Some(summary.row_count),
                    data_size: Some(summary.uncompressed_byte_size),
                    data_size_compressed: Some(summary.compressed_byte_size),
                    index_size: Some(summary.index_size),
                    number_of_blocks: Some(summary.block_count),
                    number_of_segments: Some(snapshot.segments.len() as u64),
                }
            }
            _ => {
                let s = &self.table_info.meta.statistics;
                TableStatistics {
                    num_rows: Some(s.number_of_rows),
                    data_size: Some(s.data_bytes),
                    data_size_compressed: Some(s.compressed_data_bytes),
                    index_size: Some(s.index_data_bytes),
                    number_of_blocks: s.number_of_blocks,
                    number_of_segments: s.number_of_segments,
                }
            }
        };
        Ok(Some(stats))
    }

    #[async_backtrace::framed]
    async fn column_statistics_provider(
        &self,
        _ctx: Arc<dyn TableContext>,
    ) -> Result<Box<dyn ColumnStatisticsProvider>> {
        let provider = if let Some(snapshot) = self.read_table_snapshot().await? {
            let stats = &snapshot.summary.col_stats;
            let table_statistics = self.read_table_snapshot_statistics(Some(&snapshot)).await?;
            if let Some(table_statistics) = table_statistics {
                FuseTableColumnStatisticsProvider::new(
                    stats.clone(),
                    table_statistics.histograms.clone(),
                    Some(table_statistics.column_distinct_values()),
                    snapshot.summary.row_count,
                )
            } else {
                FuseTableColumnStatisticsProvider::new(
                    stats.clone(),
                    HashMap::new(),
                    None,
                    snapshot.summary.row_count,
                )
            }
        } else {
            FuseTableColumnStatisticsProvider::default()
        };
        Ok(Box::new(provider))
    }

    #[async_backtrace::framed]
    async fn accurate_columns_ranges(
        &self,
        ctx: Arc<dyn TableContext>,
        column_ids: &[ColumnId],
    ) -> Result<Option<HashMap<ColumnId, ColumnRange>>> {
        if column_ids.is_empty() {
            return Ok(Some(HashMap::new()));
        }

        let Some(snapshot) = self.read_table_snapshot().await? else {
            return Ok(Some(HashMap::new()));
        };

        let segment_locations = &snapshot.segments;
        let num_segments = snapshot.segments.len();

        if num_segments == 0 {
            return Ok(Some(HashMap::new()));
        }

        let column_ids: HashSet<&ColumnId, RandomState> = HashSet::from_iter(column_ids);

        let schema = self.schema();
        let num_fields = schema.fields.len();
        let segments_io = SegmentsIO::create(ctx.clone(), self.operator.clone(), schema);
        let chunk_size = std::cmp::min(
            ctx.get_settings().get_max_threads()? as usize * 4,
            num_segments,
        )
        .max(1);

        ctx.set_status_info(&format!(
            "processing {} segments, chunk size {}",
            num_segments, chunk_size
        ));

        // Fold column ranges of segments chunk by chunk
        let mut reduced = HashMap::with_capacity(num_fields);

        for (idx, chunk) in segment_locations.chunks(chunk_size).enumerate() {
            let segments = segments_io
                .read_segments::<Arc<CompactSegmentInfo>>(chunk, false)
                .await?;
            let mut partial_col_stats = Vec::with_capacity(chunk_size);
            // 1. Carry the previously reduced ranges
            partial_col_stats.push(reduced);
            // 2. Append ranges of this chunk
            for compacted_seg in segments.into_iter() {
                let segment = compacted_seg?;
                let mut cols_stats = segment.summary.col_stats.clone();
                cols_stats.retain(|k, _| column_ids.contains(k));
                partial_col_stats.push(cols_stats);
            }
            // 3. Reduces them
            reduced = reduce_block_statistics(&partial_col_stats);
            ctx.set_status_info(&format!("processed {} segments", (idx + 1) * chunk_size));
        }

        let r = reduced
            .into_iter()
            .map(|(k, v)| {
                (k, ColumnRange {
                    min: Bound {
                        may_be_truncated: v.min.may_be_trimmed(),
                        value: v.min,
                    },
                    max: Bound {
                        may_be_truncated: v.max.may_be_trimmed(),
                        value: v.max,
                    },
                })
            })
            .collect();
        Ok(Some(r))
    }

    #[fastrace::trace]
    #[async_backtrace::framed]
    async fn navigate_to(
        &self,
        navigation: &TimeNavigation,
        abort_checker: AbortChecker,
    ) -> Result<Arc<dyn Table>> {
        match navigation {
            TimeNavigation::TimeTravel(point) => {
                Ok(self.navigate_to_point(point, abort_checker).await?)
            }
            TimeNavigation::Changes {
                append_only,
                at,
                end,
                desc,
            } => {
                let mut end_point = if let Some(end) = end {
                    self.navigate_to_point(end, abort_checker.clone())
                        .await?
                        .as_ref()
                        .clone()
                } else {
                    self.clone()
                };
                let changes_desc = end_point
                    .get_change_descriptor(*append_only, desc.clone(), Some(at), abort_checker)
                    .await?;
                end_point.changes_desc = Some(changes_desc);
                Ok(Arc::new(end_point))
            }
        }
    }

    #[async_backtrace::framed]
    async fn generate_changes_query(
        &self,
        ctx: Arc<dyn TableContext>,
        database_name: &str,
        table_name: &str,
        _with_options: &str,
    ) -> Result<String> {
        let db_tb_name = format!("'{}'.'{}'", database_name, table_name);
        let Some(ChangesDesc {
            seq,
            desc,
            mode,
            location,
        }) = self.changes_desc.as_ref()
        else {
            return Err(ErrorCode::Internal(format!(
                "No changes descriptor found in table {db_tb_name}"
            )));
        };

        self.check_changes_valid(&db_tb_name, *seq)?;
        let quote = ctx.get_settings().get_sql_dialect()?.default_ident_quote();
        self.get_changes_query(
            ctx,
            mode,
            location,
            format!("{quote}{database_name}{quote}.{quote}{table_name}{quote} {desc}"),
            *seq,
        )
        .await
    }

    fn get_block_thresholds(&self) -> BlockThresholds {
        let max_rows_per_block =
            self.get_option(FUSE_OPT_KEY_ROW_PER_BLOCK, DEFAULT_BLOCK_MAX_ROWS);
        let min_rows_per_block = (max_rows_per_block * 4).div_ceil(5);
        let max_bytes_per_block = self.get_option(
            FUSE_OPT_KEY_BLOCK_IN_MEM_SIZE_THRESHOLD,
            DEFAULT_BLOCK_BUFFER_SIZE,
        );
        BlockThresholds::new(max_rows_per_block, min_rows_per_block, max_bytes_per_block)
    }

    #[async_backtrace::framed]
    async fn compact_segments(
        &self,
        ctx: Arc<dyn TableContext>,
        limit: Option<usize>,
    ) -> Result<()> {
        self.do_compact_segments(ctx, limit).await
    }

    #[async_backtrace::framed]
    async fn compact_blocks(
        &self,
        ctx: Arc<dyn TableContext>,
        limits: CompactionLimits,
    ) -> Result<Option<(Partitions, Arc<TableSnapshot>)>> {
        self.do_compact_blocks(ctx, limits).await
    }

    #[async_backtrace::framed]
    async fn recluster(
        &self,
        ctx: Arc<dyn TableContext>,
        push_downs: Option<PushDownInfo>,
        limit: Option<usize>,
    ) -> Result<Option<(ReclusterParts, Arc<TableSnapshot>)>> {
        self.do_recluster(ctx, push_downs, limit).await
    }

    #[async_backtrace::framed]
    async fn revert_to(
        &self,
        ctx: Arc<dyn TableContext>,
        point: NavigationDescriptor,
    ) -> Result<()> {
        self.do_revert_to(ctx, point).await
    }

    fn support_prewhere(&self) -> bool {
        matches!(self.storage_format, FuseStorageFormat::Native)
    }

    fn support_index(&self) -> bool {
        true
    }

    fn support_virtual_columns(&self) -> bool {
        true
    }

    fn result_can_be_cached(&self) -> bool {
        true
    }

    fn is_read_only(&self) -> bool {
        self.table_type.is_readonly()
    }

    fn use_own_sample_block(&self) -> bool {
        true
    }

    async fn remove_aggregating_index_files(
        &self,
        ctx: Arc<dyn TableContext>,
        index_id: u64,
    ) -> Result<u64> {
        let prefix = format!(
            "{}/{}",
            self.meta_location_generator.agg_index_location_prefix(),
            index_id
        );
        let op = &self.operator;
        info!("remove_aggregating_index_files: {}", prefix);
        let mut lister = op.lister_with(&prefix).recursive(true).await?;
        let mut files = Vec::new();
        while let Some(entry) = lister.try_next().await? {
            if entry.metadata().is_dir() {
                continue;
            }
            files.push(entry.path().to_string());
        }

        let op = Files::create(ctx, self.operator.clone());
        let len = files.len() as u64;
        op.remove_file_in_batch(files).await?;
        Ok(len)
    }

    async fn remove_inverted_index_files(
        &self,
        ctx: Arc<dyn TableContext>,
        index_name: String,
        index_version: String,
    ) -> Result<u64> {
        let prefix = self
            .meta_location_generator
            .gen_specific_inverted_index_prefix(&index_name, &index_version);
        let op = &self.operator;
        info!("remove_inverted_index_files: {}", prefix);
        let mut lister = op.lister_with(&prefix).recursive(true).await?;
        let mut files = Vec::new();
        while let Some(entry) = lister.try_next().await? {
            if entry.metadata().is_dir() {
                continue;
            }
            files.push(entry.path().to_string());
        }
        let op = Files::create(ctx, self.operator.clone());
        let len = files.len() as u64;
        op.remove_file_in_batch(files).await?;
        Ok(len)
    }
}
