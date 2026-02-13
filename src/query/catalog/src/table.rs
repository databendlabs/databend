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
use std::sync::Arc;

use chrono::DateTime;
use chrono::Utc;
use databend_common_ast::ast::Expr;
use databend_common_ast::parser::Dialect;
use databend_common_ast::parser::parse_comma_separated_exprs;
use databend_common_ast::parser::tokenize_sql;
use databend_common_exception::ErrorCode;
use databend_common_exception::Result;
use databend_common_expression::BlockThresholds;
use databend_common_expression::ColumnId;
use databend_common_expression::Scalar;
use databend_common_expression::TableSchema;
use databend_common_meta_app::app_error::AppError;
use databend_common_meta_app::app_error::UnknownTableId;
use databend_common_meta_app::schema::BranchInfo;
use databend_common_meta_app::schema::SnapshotRefType;
use databend_common_meta_app::schema::TableIdent;
use databend_common_meta_app::schema::TableInfo;
use databend_common_meta_app::schema::TableMeta;
use databend_common_meta_app::schema::UpdateStreamMetaReq;
use databend_common_meta_app::schema::UpsertTableCopiedFileReq;
use databend_common_pipeline::core::Pipeline;
use databend_common_statistics::Histogram;
use databend_common_storage::StorageMetrics;
use databend_meta_types::MetaId;
use databend_storages_common_table_meta::meta::ClusterKey;
use databend_storages_common_table_meta::meta::SnapshotId;
use databend_storages_common_table_meta::meta::TableMetaTimestamps;
use databend_storages_common_table_meta::meta::TableSnapshot;
use databend_storages_common_table_meta::table::ChangeType;
use databend_storages_common_table_meta::table::ClusterType;
use databend_storages_common_table_meta::table::OPT_KEY_CLUSTER_TYPE;
use databend_storages_common_table_meta::table::OPT_KEY_TEMP_PREFIX;
use databend_storages_common_table_meta::table_id_ranges::is_temp_table_id;

use crate::plan::DataSourceInfo;
use crate::plan::DataSourcePlan;
use crate::plan::ExtendedTableInfo;
use crate::plan::PartStatistics;
use crate::plan::Partitions;
use crate::plan::PushDownInfo;
use crate::plan::ReclusterParts;
use crate::plan::StreamColumn;
use crate::statistics::BasicColumnStatistics;
use crate::table_args::TableArgs;
use crate::table_context::TableContext;

#[async_trait::async_trait]
pub trait Table: Sync + Send {
    fn name(&self) -> &str {
        &self.get_table_info().name
    }

    fn engine(&self) -> &str {
        self.get_table_info().engine()
    }

    /// Whether the table engine supports the given internal column.
    fn supported_internal_column(&self, _column_id: ColumnId) -> bool {
        false
    }

    fn supported_lazy_materialize(&self) -> bool {
        false
    }

    fn schema(&self) -> Arc<TableSchema> {
        self.get_branch_info()
            .map_or_else(|| self.get_table_info().schema(), |v| v.schema.clone())
    }

    fn options(&self) -> &BTreeMap<String, String> {
        self.get_table_info().options()
    }

    fn field_comments(&self) -> &Vec<String> {
        self.get_table_info().field_comments()
    }

    fn get_id(&self) -> MetaId {
        self.get_table_info().ident.table_id
    }

    /// Returns the effective unique ID for this table reference.
    ///
    /// - If this is a branch, returns the branch ID.
    /// - Otherwise, returns the physical table ID.
    ///
    /// NOTE:
    /// - `branch_id` is **globally unique across all tables**, not scoped per table.
    /// - Branch IDs are allocated from a global ID generator and do NOT collide
    ///   even for branches created on different tables.
    /// - Therefore, using `branch_id` alone as a cache key namespace is safe.
    fn get_unique_id(&self) -> u64 {
        self.get_branch_info()
            .map_or(self.get_id(), |v| v.branch_id())
    }

    fn distribution_level(&self) -> DistributionLevel {
        DistributionLevel::Local
    }

    fn as_any(&self) -> &dyn Any;

    fn get_table_info(&self) -> &TableInfo;

    fn get_branch_info(&self) -> Option<&BranchInfo> {
        None
    }

    fn get_data_source_info(&self) -> DataSourceInfo {
        DataSourceInfo::TableSource(ExtendedTableInfo {
            table_info: self.get_table_info().clone(),
            branch_info: self.get_branch_info().cloned(),
        })
    }

    /// get_data_metrics will get data metrics from table.
    fn get_data_metrics(&self) -> Option<Arc<StorageMetrics>> {
        None
    }

    /// whether column prune(projection) can help in table read
    fn support_column_projection(&self) -> bool {
        false
    }

    fn support_distributed_insert(&self) -> bool {
        false
    }

    /// whether table has the exact number of total rows
    fn has_exact_total_row_count(&self) -> bool {
        false
    }

    fn cluster_key_meta(&self) -> Option<ClusterKey> {
        None
    }

    fn cluster_type(&self) -> Option<ClusterType> {
        self.cluster_key_meta()?;
        if self.get_branch_info().is_some() {
            Some(ClusterType::Linear)
        } else {
            let cluster_type = self
                .options()
                .get(OPT_KEY_CLUSTER_TYPE)
                .and_then(|s| s.parse::<ClusterType>().ok())
                .unwrap_or(ClusterType::Linear);
            Some(cluster_type)
        }
    }

    fn resolve_cluster_keys(&self) -> Option<Vec<Expr>> {
        let Some((_, cluster_key_str)) = &self.cluster_key_meta() else {
            return None;
        };
        let tokens = tokenize_sql(cluster_key_str).unwrap();
        // `cluster_key` is persisted in table metadata and may be created/rewritten under a
        // different session dialect. Parse it with a dialect that accepts both identifier
        // quote styles to keep ALTER behavior stable across sessions.
        let sql_dialect = Dialect::default();
        let mut ast_exprs = parse_comma_separated_exprs(&tokens, sql_dialect).unwrap();
        // unwrap tuple.
        if ast_exprs.len() == 1 {
            if let Expr::Tuple { exprs, .. } = &ast_exprs[0] {
                ast_exprs = exprs.clone();
            }
        } else {
            // Defensive check:
            // `ast_exprs` should always contain one element which can be one of the following:
            // 1. A tuple of composite cluster keys
            // 2. A single cluster key
            unreachable!("invalid cluster key ast expression, {:?}", ast_exprs);
        }
        Some(ast_exprs)
    }

    fn change_tracking_enabled(&self) -> bool {
        false
    }

    fn stream_columns(&self) -> Vec<StreamColumn> {
        vec![]
    }

    fn schema_with_stream(&self) -> Arc<TableSchema> {
        let mut fields = self.schema().fields().clone();
        for stream_column in self.stream_columns().iter() {
            fields.push(stream_column.table_field());
        }
        Arc::new(TableSchema {
            fields,
            ..self.schema().as_ref().clone()
        })
    }

    /// Whether the table engine supports prewhere optimization.
    /// only Fuse Engine supports this.
    fn support_prewhere(&self) -> bool {
        false
    }

    fn support_index(&self) -> bool {
        false
    }

    /// Whether the table engine supports virtual columns optimization.
    fn support_virtual_columns(&self) -> bool {
        false
    }

    fn storage_format_as_parquet(&self) -> bool {
        false
    }

    /// Gather partitions to be scanned according to the push_downs
    #[async_backtrace::framed]
    async fn read_partitions(
        &self,
        ctx: Arc<dyn TableContext>,
        push_downs: Option<PushDownInfo>,
        _dry_run: bool,
    ) -> Result<(PartStatistics, Partitions)> {
        let (_, _) = (ctx, push_downs);

        Err(ErrorCode::Unimplemented(format!(
            "The 'read_partitions' operation is not implemented for table '{}' using the '{}' engine.",
            self.name(),
            self.get_table_info().meta.engine
        )))
    }

    fn table_args(&self) -> Option<TableArgs> {
        None
    }

    /// Assembly the pipeline of reading data from storage, according to the plan
    fn read_data(
        &self,
        ctx: Arc<dyn TableContext>,
        plan: &DataSourcePlan,
        pipeline: &mut Pipeline,
        put_cache: bool,
    ) -> Result<()> {
        let (_, _, _, _) = (ctx, plan, pipeline, put_cache);

        Err(ErrorCode::Unimplemented(format!(
            "The 'read_data' operation is not implemented for the table '{}'. Table engine type: '{}'.",
            self.name(),
            self.get_table_info().meta.engine
        )))
    }

    fn build_prune_pipeline(
        &self,
        table_ctx: Arc<dyn TableContext>,
        plan: &DataSourcePlan,
        source_pipeline: &mut Pipeline,
        plan_id: u32,
    ) -> Result<Option<Pipeline>> {
        let (_, _, _, _) = (table_ctx, plan, source_pipeline, plan_id);

        Ok(None)
    }

    /// Assembly the pipeline of appending data to storage
    fn append_data(
        &self,
        ctx: Arc<dyn TableContext>,
        pipeline: &mut Pipeline,
        _table_meta_timestamps: TableMetaTimestamps,
    ) -> Result<()> {
        let (_, _) = (ctx, pipeline);

        Err(ErrorCode::Unimplemented(format!(
            "The 'append_data' operation is not available for the table '{}'. Current table engine: '{}'.",
            self.name(),
            self.get_table_info().meta.engine
        )))
    }

    fn commit_insertion(
        &self,
        ctx: Arc<dyn TableContext>,
        pipeline: &mut Pipeline,
        copied_files: Option<UpsertTableCopiedFileReq>,
        update_stream_meta: Vec<UpdateStreamMetaReq>,
        overwrite: bool,
        prev_snapshot_id: Option<SnapshotId>,
        _deduplicated_label: Option<String>,
        _table_meta_timestamps: TableMetaTimestamps,
    ) -> Result<()> {
        let (_, _, _, _, _, _) = (
            ctx,
            copied_files,
            update_stream_meta,
            pipeline,
            overwrite,
            prev_snapshot_id,
        );

        Ok(())
    }

    #[async_backtrace::framed]
    async fn truncate(&self, ctx: Arc<dyn TableContext>, pipeline: &mut Pipeline) -> Result<()> {
        let (_, _) = (ctx, pipeline);
        Ok(())
    }

    #[async_backtrace::framed]
    async fn purge(
        &self,
        ctx: Arc<dyn TableContext>,
        instant: Option<NavigationPoint>,
        num_snapshot_limit: Option<usize>,
        dry_run: bool,
    ) -> Result<Option<Vec<String>>> {
        let (_, _, _, _) = (ctx, instant, num_snapshot_limit, dry_run);

        Ok(None)
    }

    async fn table_statistics(
        &self,
        ctx: Arc<dyn TableContext>,
        require_fresh: bool,
        change_type: Option<ChangeType>,
    ) -> Result<Option<TableStatistics>> {
        let (_, _, _) = (ctx, require_fresh, change_type);

        Ok(None)
    }

    #[async_backtrace::framed]
    async fn column_statistics_provider(
        &self,
        ctx: Arc<dyn TableContext>,
    ) -> Result<Box<dyn ColumnStatisticsProvider>> {
        let _ = ctx;

        Ok(Box::new(DummyColumnStatisticsProvider))
    }

    /// - Returns `Some(_)`
    ///    if table has accurate columns ranges information,
    /// - Otherwise returns `None`.
    #[async_backtrace::framed]
    async fn accurate_columns_ranges(
        &self,
        _ctx: Arc<dyn TableContext>,
        _column_ids: &[ColumnId],
    ) -> Result<Option<HashMap<ColumnId, ColumnRange>>> {
        Ok(None)
    }

    #[async_backtrace::framed]
    async fn navigate_to(
        &self,
        ctx: &Arc<dyn TableContext>,
        navigation: &TimeNavigation,
    ) -> Result<Arc<dyn Table>> {
        let _ = navigation;
        let _ = ctx;

        Err(ErrorCode::Unimplemented(format!(
            "Time travel operation is not supported for the table '{}', which uses the '{}' engine.",
            self.name(),
            self.get_table_info().engine(),
        )))
    }

    fn with_branch(&self, branch_name: &str) -> Result<Arc<dyn Table>> {
        let _ = branch_name;
        Err(ErrorCode::Unimplemented(format!(
            "Table branch is not supported for the table '{}', which uses the '{}' engine.",
            self.name(),
            self.get_table_info().engine(),
        )))
    }

    async fn generate_changes_query(
        &self,
        ctx: Arc<dyn TableContext>,
        database_name: &str,
        table_name: &str,
        with_options: &str,
    ) -> Result<String> {
        let (_, _, _, _) = (ctx, database_name, table_name, with_options);

        Err(ErrorCode::Unimplemented(format!(
            "Change tracking operation is not supported for the table '{}', which uses the '{}' engine.",
            self.name(),
            self.get_table_info().engine(),
        )))
    }

    fn get_block_thresholds(&self) -> BlockThresholds {
        BlockThresholds::default()
    }

    #[async_backtrace::framed]
    async fn compact_segments(
        &self,
        ctx: Arc<dyn TableContext>,
        limit: Option<usize>,
    ) -> Result<()> {
        let (_, _) = (ctx, limit);

        Err(ErrorCode::Unimplemented(format!(
            "The operation 'compact_segments' is not supported for the table '{}', which is using the '{}' engine.",
            self.name(),
            self.get_table_info().engine(),
        )))
    }

    #[async_backtrace::framed]
    async fn compact_blocks(
        &self,
        ctx: Arc<dyn TableContext>,
        limits: CompactionLimits,
    ) -> Result<Option<(Partitions, Arc<TableSnapshot>)>> {
        let (_, _) = (ctx, limits);

        Err(ErrorCode::Unimplemented(format!(
            "The 'compact_blocks' operation is not supported for the table '{}'. Table engine: '{}'.",
            self.name(),
            self.get_table_info().engine(),
        )))
    }

    // return the selected block num.
    #[async_backtrace::framed]
    async fn recluster(
        &self,
        ctx: Arc<dyn TableContext>,
        push_downs: Option<PushDownInfo>,
        limit: Option<usize>,
    ) -> Result<Option<(ReclusterParts, Arc<TableSnapshot>)>> {
        let (_, _, _) = (ctx, push_downs, limit);

        Err(ErrorCode::Unimplemented(format!(
            "The 'recluster' operation is not supported for the table '{}'. Table engine: '{}'.",
            self.name(),
            self.get_table_info().engine(),
        )))
    }

    #[async_backtrace::framed]
    async fn revert_to(
        &self,
        ctx: Arc<dyn TableContext>,
        point: NavigationDescriptor,
    ) -> Result<()> {
        let (_, _) = (ctx, point);

        Err(ErrorCode::Unimplemented(format!(
            "The 'revert_to' operation is not supported for the table '{}'. Table engine: '{}'.",
            self.name(),
            self.get_table_info().engine(),
        )))
    }

    fn is_stage_table(&self) -> bool {
        false
    }

    fn result_can_be_cached(&self) -> bool {
        false
    }

    fn broadcast_truncate_to_warehouse(&self) -> bool {
        false
    }

    fn is_read_only(&self) -> bool {
        false
    }

    fn is_temp(&self) -> bool {
        is_temp_table_by_table_info(self.get_table_info())
    }

    fn is_stream(&self) -> bool {
        self.engine() == "STREAM"
    }

    fn use_own_sample_block(&self) -> bool {
        false
    }

    async fn remove_aggregating_index_files(
        &self,
        _ctx: Arc<dyn TableContext>,
        _index_id: u64,
    ) -> Result<u64> {
        Ok(0)
    }

    async fn remove_inverted_index_files(
        &self,
        _ctx: Arc<dyn TableContext>,
        _index_name: String,
        _index_version: String,
    ) -> Result<u64> {
        Ok(0)
    }

    fn is_column_oriented(&self) -> bool {
        false
    }
}

#[async_trait::async_trait]
pub trait TableExt: Table {
    #[async_backtrace::framed]
    async fn refresh(&self, ctx: &dyn TableContext) -> Result<Arc<dyn Table>> {
        let table_info = self.get_table_info();
        let tid = table_info.ident.table_id;
        let catalog = ctx.get_catalog(table_info.catalog()).await?;

        let seqv = catalog.get_table_meta_by_id(tid).await?.ok_or_else(|| {
            let err = UnknownTableId::new(tid, "TableExt::refresh");
            AppError::from(err)
        })?;

        self.refresh_with_seq_meta(ctx, seqv.seq, seqv.data).await
    }

    async fn refresh_with_seq_meta(
        &self,
        ctx: &dyn TableContext,
        seq: u64,
        meta: TableMeta,
    ) -> Result<Arc<dyn Table>> {
        let table_info = self.get_table_info();
        let tid = table_info.ident.table_id;
        let catalog = ctx.get_catalog(table_info.catalog()).await?;

        let table_info = TableInfo {
            ident: TableIdent::new(tid, seq),
            meta,
            ..table_info.clone()
        };
        let table = catalog.get_table_by_info(&table_info)?;
        if let Some(branch) = self.get_branch_info() {
            let branch_name = branch.branch_name();
            table.with_branch(branch_name)
        } else {
            Ok(table)
        }
    }

    fn check_mutable(&self) -> Result<()> {
        if self.is_read_only() {
            let branch_info = self
                .get_branch_info()
                .map(|v| format!(" (Tag: '{}')", v.branch_name()))
                .unwrap_or_default();
            Err(ErrorCode::InvalidOperation(format!(
                "Modification not permitted: Table '{}'{} is READ ONLY, preventing any changes or updates.",
                self.get_table_info().name,
                branch_info
            )))
        } else {
            Ok(())
        }
    }
}
impl<T: ?Sized> TableExt for T where T: Table {}

#[derive(Debug, Clone, Eq, PartialEq)]
pub enum TimeNavigation {
    TimeTravel(NavigationPoint),
    Changes {
        append_only: bool,
        desc: String,
        at: NavigationPoint,
        end: Option<NavigationPoint>,
    },
}

#[derive(Debug, Clone, Eq, PartialEq)]
pub enum NavigationPoint {
    SnapshotID(String),
    TimePoint(DateTime<Utc>),
    StreamInfo(TableInfo),
    TableRef { typ: SnapshotRefType, name: String },
}

#[derive(Debug, Copy, Clone, Default, serde::Serialize, serde::Deserialize)]
pub struct TableStatistics {
    pub num_rows: Option<u64>,
    pub data_size: Option<u64>,
    pub data_size_compressed: Option<u64>,
    pub index_size: Option<u64>,
    pub bloom_index_size: Option<u64>,
    pub ngram_index_size: Option<u64>,
    pub inverted_index_size: Option<u64>,
    pub vector_index_size: Option<u64>,
    pub virtual_column_size: Option<u64>,

    pub number_of_blocks: Option<u64>,
    pub number_of_segments: Option<u64>,
}

#[derive(Debug, Clone)]
pub struct ColumnStatistics {
    pub min: Scalar,
    pub max: Scalar,
    pub null_count: u64,
    pub number_of_distinct_values: u64,
}

pub enum CompactTarget {
    // compact blocks, with optional limit on the number of blocks to be compacted
    Blocks(Option<usize>),
    // compact segments
    Segments,
}

pub trait ColumnStatisticsProvider: Send {
    // returns the statistics of the given column, if any.
    // column_id is just the index of the column in table's schema
    fn column_statistics(&self, column_id: ColumnId) -> Option<&BasicColumnStatistics>;

    // returns the num rows of the table, if any.
    fn num_rows(&self) -> Option<u64>;

    fn stats_num_rows(&self) -> Option<u64>;

    fn average_size(&self, _column_id: ColumnId) -> Option<u64>;

    // return histogram if any
    fn histogram(&self, _column_id: ColumnId) -> Option<Histogram> {
        None
    }
}

pub struct DummyColumnStatisticsProvider;

impl ColumnStatisticsProvider for DummyColumnStatisticsProvider {
    fn column_statistics(&self, _column_id: ColumnId) -> Option<&BasicColumnStatistics> {
        None
    }

    fn num_rows(&self) -> Option<u64> {
        None
    }

    fn average_size(&self, _column_id: ColumnId) -> Option<u64> {
        None
    }

    fn stats_num_rows(&self) -> Option<u64> {
        None
    }
}

pub struct NavigationDescriptor {
    pub database_name: String,
    pub point: NavigationPoint,
}

#[derive(serde::Serialize, serde::Deserialize, Clone, Debug, Default)]
pub struct ParquetTableColumnStatisticsProvider {
    column_stats: HashMap<ColumnId, Option<BasicColumnStatistics>>,
    num_rows: u64,
}

impl ParquetTableColumnStatisticsProvider {
    pub fn new(
        column_stats: HashMap<ColumnId, Option<BasicColumnStatistics>>,
        num_rows: u64,
    ) -> Self {
        Self {
            column_stats,
            num_rows,
        }
    }
}

impl ColumnStatisticsProvider for ParquetTableColumnStatisticsProvider {
    fn column_statistics(&self, column_id: ColumnId) -> Option<&BasicColumnStatistics> {
        self.column_stats.get(&column_id).and_then(|s| s.as_ref())
    }

    fn num_rows(&self) -> Option<u64> {
        Some(self.num_rows)
    }

    fn stats_num_rows(&self) -> Option<u64> {
        Some(self.num_rows)
    }

    fn average_size(&self, column_id: ColumnId) -> Option<u64> {
        self.column_stats.get(&column_id).and_then(|v| {
            v.as_ref()
                .and_then(|s| s.in_memory_size.checked_div(self.num_rows))
        })
    }
}

#[derive(Clone, Debug, Default, PartialEq, Eq, Hash)]
pub struct CompactionLimits {
    pub segment_limit: Option<usize>,
    pub block_limit: Option<usize>,
}

impl CompactionLimits {
    pub fn limits(segment_limit: Option<usize>, block_limit: Option<usize>) -> Self {
        // As n fragmented blocks scattered across at most n segments,
        // when no segment_limit provided, we set it to the same value of block_limit
        let adjusted_segment_limit = segment_limit.or(block_limit);
        CompactionLimits {
            segment_limit: adjusted_segment_limit,
            block_limit,
        }
    }
    pub fn limit_by_num_segments(v: Option<usize>) -> Self {
        CompactionLimits {
            segment_limit: v,
            block_limit: None,
        }
    }

    pub fn limit_by_num_blocks(v: Option<usize>) -> Self {
        let segment_limit = v;
        CompactionLimits {
            segment_limit,
            block_limit: v,
        }
    }
}

#[derive(Debug)]
pub struct Bound {
    pub value: Scalar,
    pub may_be_truncated: bool,
}

#[derive(Debug)]
pub struct ColumnRange {
    pub min: Bound,
    pub max: Bound,
}

#[derive(Debug)]
pub enum DistributionLevel {
    Local,
    Cluster,
    Warehouse,
}

pub fn is_temp_table_by_table_info(table_info: &TableInfo) -> bool {
    let is_temp = table_info.options().contains_key(OPT_KEY_TEMP_PREFIX);
    let is_id_temp = is_temp_table_id(table_info.ident.table_id);
    assert_eq!(is_temp, is_id_temp);
    is_temp
}
