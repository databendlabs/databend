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
use std::collections::HashMap;
use std::collections::HashSet;
use std::sync::Arc;
use std::time::Instant;

use databend_common_catalog::catalog::StorageDescription;
use databend_common_catalog::plan::block_id_from_location;
use databend_common_catalog::plan::DataSourcePlan;
use databend_common_catalog::plan::PartStatistics;
use databend_common_catalog::plan::Partitions;
use databend_common_catalog::plan::PartitionsShuffleKind;
use databend_common_catalog::plan::PushDownInfo;
use databend_common_catalog::plan::StreamColumn;
use databend_common_catalog::plan::StreamTablePart;
use databend_common_catalog::table::ColumnStatisticsProvider;
use databend_common_catalog::table::Table;
use databend_common_catalog::table::TableStatistics;
use databend_common_catalog::table_context::TableContext;
use databend_common_exception::ErrorCode;
use databend_common_exception::Result;
use databend_common_expression::types::decimal::Decimal128Type;
use databend_common_expression::ColumnId;
use databend_common_expression::FromData;
use databend_common_expression::RemoteExpr;
use databend_common_expression::Scalar;
use databend_common_expression::BASE_BLOCK_IDS_COLUMN_ID;
use databend_common_expression::BASE_BLOCK_IDS_COL_NAME;
use databend_common_expression::BASE_ROW_ID_COLUMN_ID;
use databend_common_expression::ORIGIN_BLOCK_ID_COL_NAME;
use databend_common_expression::ORIGIN_BLOCK_ROW_NUM_COL_NAME;
use databend_common_expression::ORIGIN_VERSION_COL_NAME;
use databend_common_expression::ROW_VERSION_COL_NAME;
use databend_common_meta_app::schema::TableInfo;
use databend_common_pipeline_core::Pipeline;
use databend_common_sql::binder::STREAM_COLUMN_FACTORY;
use databend_common_storages_fuse::io::SegmentsIO;
use databend_common_storages_fuse::io::SnapshotsIO;
use databend_common_storages_fuse::pruning::FusePruner;
use databend_common_storages_fuse::FuseTable;
use databend_storages_common_table_meta::meta::BlockMeta;
use databend_storages_common_table_meta::meta::SegmentInfo;
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
                "Table id mismatch, expect {}, got {}",
                self.table_id,
                table.get_table_info().ident.table_id
            )));
        }

        if !table.change_tracking_enabled() {
            return Err(ErrorCode::IllegalStream(format!(
                "Change tracking is not enabled for table '{}.{}'",
                self.table_database, self.table_name
            )));
        }

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

    async fn collect_incremental_blocks(
        &self,
        ctx: Arc<dyn TableContext>,
        fuse_table: &FuseTable,
    ) -> Result<(Vec<Arc<BlockMeta>>, Vec<Arc<BlockMeta>>)> {
        let operator = fuse_table.get_operator();
        let latest_segments = if let Some(snapshot) = fuse_table.read_table_snapshot().await? {
            HashSet::from_iter(snapshot.segments.clone())
        } else {
            HashSet::new()
        };

        let base_segments = if let Some(snapshot_location) = &self.snapshot_location {
            let (base_snapshot, _) =
                SnapshotsIO::read_snapshot(snapshot_location.clone(), operator.clone()).await?;
            HashSet::from_iter(base_snapshot.segments.clone())
        } else {
            HashSet::new()
        };

        let fuse_segment_io =
            SegmentsIO::create(ctx.clone(), operator.clone(), fuse_table.schema());
        let chunk_size = ctx.get_settings().get_max_threads()? as usize * 4;

        let mut base_blocks = HashMap::new();
        let diff_in_base = base_segments
            .difference(&latest_segments)
            .cloned()
            .collect::<Vec<_>>();
        for chunk in diff_in_base.chunks(chunk_size) {
            let segments = fuse_segment_io
                .read_segments::<SegmentInfo>(chunk, true)
                .await?;
            for segment in segments {
                let segment = segment?;
                segment.blocks.into_iter().for_each(|block| {
                    base_blocks.insert(block.location.clone(), block);
                })
            }
        }

        let mut add_blocks = Vec::new();
        let diff_in_latest = latest_segments
            .difference(&base_segments)
            .cloned()
            .collect::<Vec<_>>();
        for chunk in diff_in_latest.chunks(chunk_size) {
            let segments = fuse_segment_io
                .read_segments::<SegmentInfo>(chunk, true)
                .await?;

            for segment in segments {
                let segment = segment?;
                segment.blocks.into_iter().for_each(|block| {
                    if base_blocks.contains_key(&block.location) {
                        base_blocks.remove(&block.location);
                    } else {
                        add_blocks.push(block);
                    }
                });
            }
        }

        let del_blocks = base_blocks.into_values().collect::<Vec<_>>();
        Ok((del_blocks, add_blocks))
    }

    #[async_backtrace::framed]
    async fn do_read_partitions(
        &self,
        ctx: Arc<dyn TableContext>,
        push_downs: Option<PushDownInfo>,
    ) -> Result<(PartStatistics, Partitions)> {
        let start = Instant::now();
        let table = self.source_table(ctx.clone()).await?;
        let fuse_table = FuseTable::try_from_table(table.as_ref())?;

        let (del_blocks, add_blocks) = self
            .collect_incremental_blocks(ctx.clone(), fuse_table)
            .await?;

        let change_type = push_downs.as_ref().map_or(ChangeType::Append, |v| {
            v.change_type.clone().unwrap_or(ChangeType::Append)
        });
        let mut push_downs = push_downs;
        let (blocks, base_block_ids_scalar) = match change_type {
            ChangeType::Append => {
                let mut base_block_ids = Vec::with_capacity(del_blocks.len());
                for base_block in del_blocks {
                    let block_id = block_id_from_location(&base_block.location.0)?;
                    base_block_ids.push(block_id);
                }
                let base_block_ids_scalar =
                    Scalar::Array(Decimal128Type::from_data(base_block_ids));
                push_downs = replace_push_downs(push_downs, &base_block_ids_scalar)?;
                (add_blocks, Some(base_block_ids_scalar))
            }
            ChangeType::Insert => (add_blocks, None),
            ChangeType::Delete => (del_blocks, None),
        };

        let summary = blocks.len();
        if summary == 0 {
            return Ok((PartStatistics::default(), Partitions::default()));
        }

        let table_schema = fuse_table.schema_with_stream();
        let (cluster_keys, cluster_key_meta) =
            if !fuse_table.is_native() || fuse_table.cluster_key_meta().is_none() {
                (vec![], None)
            } else {
                (
                    fuse_table.cluster_keys(ctx.clone()),
                    fuse_table.cluster_key_meta(),
                )
            };
        let bloom_index_cols = fuse_table.bloom_index_cols();
        let mut pruner = FusePruner::create_with_pages(
            &ctx,
            fuse_table.get_operator(),
            table_schema.clone(),
            &push_downs,
            cluster_key_meta,
            cluster_keys,
            bloom_index_cols,
        )?;

        let block_metas = pruner.stream_pruning(blocks).await?;
        let pruning_stats = pruner.pruning_stats();

        log::info!(
            "prune snapshot block end, final block numbers:{}, cost:{}",
            block_metas.len(),
            start.elapsed().as_secs()
        );

        let block_metas = block_metas
            .into_iter()
            .map(|(block_meta_index, block_meta)| (Some(block_meta_index), block_meta))
            .collect::<Vec<_>>();

        let (stats, parts) = fuse_table.read_partitions_with_metas(
            ctx.clone(),
            table_schema,
            push_downs,
            &block_metas,
            summary,
            pruning_stats,
        )?;
        if let Some(base_block_ids_scalar) = base_block_ids_scalar {
            let wrapper = Partitions::create_nolazy(PartitionsShuffleKind::Seq, vec![
                StreamTablePart::create(parts, base_block_ids_scalar),
            ]);
            Ok((stats, wrapper))
        } else {
            Ok((stats, parts))
        }
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
    ) -> Result<Option<TableStatistics>> {
        let table = self.source_table(ctx.clone()).await?;
        let fuse_table = FuseTable::try_from_table(table.as_ref())?;

        let latest_summary = if let Some(snapshot) = fuse_table.read_table_snapshot().await? {
            snapshot.summary.clone()
        } else {
            return Ok(None);
        };

        let base_summary = if let Some(snapshot_location) = &self.snapshot_location {
            let (base_snapshot, _) =
                SnapshotsIO::read_snapshot(snapshot_location.clone(), fuse_table.get_operator())
                    .await?;
            base_snapshot.summary.clone()
        } else {
            return fuse_table.table_statistics(ctx).await;
        };

        let num_rows = latest_summary.row_count.abs_diff(base_summary.row_count);
        let data_size = latest_summary
            .uncompressed_byte_size
            .abs_diff(base_summary.uncompressed_byte_size);
        let data_size_compressed = latest_summary
            .compressed_byte_size
            .abs_diff(base_summary.compressed_byte_size);
        let index_size = latest_summary.index_size.abs_diff(base_summary.index_size);
        let number_of_blocks = latest_summary
            .block_count
            .abs_diff(base_summary.block_count);

        Ok(Some(TableStatistics {
            num_rows: Some(num_rows),
            data_size: Some(data_size),
            data_size_compressed: Some(data_size_compressed),
            index_size: Some(index_size),
            number_of_blocks: Some(number_of_blocks),
            number_of_segments: None,
        }))
    }

    #[async_backtrace::framed]
    async fn column_statistics_provider(
        &self,
        ctx: Arc<dyn TableContext>,
    ) -> Result<Box<dyn ColumnStatisticsProvider>> {
        let table = self.source_table(ctx.clone()).await?;
        table.column_statistics_provider(ctx).await
    }
}

fn replace_push_downs(
    push_downs: Option<PushDownInfo>,
    base_block_ids: &Scalar,
) -> Result<Option<PushDownInfo>> {
    fn visit_expr_column(expr: &mut RemoteExpr<String>, base_block_ids: &Scalar) -> Result<()> {
        match expr {
            RemoteExpr::ColumnRef {
                span,
                id,
                data_type,
                ..
            } => {
                if id == BASE_BLOCK_IDS_COL_NAME {
                    *expr = RemoteExpr::Constant {
                        span: *span,
                        scalar: base_block_ids.clone(),
                        data_type: data_type.clone(),
                    };
                }
            }
            RemoteExpr::Cast { expr, .. } => {
                visit_expr_column(expr, base_block_ids)?;
            }
            RemoteExpr::FunctionCall { args, .. } => {
                for arg in args.iter_mut() {
                    visit_expr_column(arg, base_block_ids)?;
                }
            }
            _ => (),
        }
        Ok(())
    }

    if let Some(mut push_downs) = push_downs {
        if let Some(filters) = &mut push_downs.filters {
            visit_expr_column(&mut filters.filter, base_block_ids)?;
            visit_expr_column(&mut filters.inverted_filter, base_block_ids)?;
        }
        Ok(Some(push_downs))
    } else {
        Ok(None)
    }
}
