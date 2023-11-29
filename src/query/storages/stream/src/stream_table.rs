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
use std::collections::HashSet;
use std::fmt::Display;
use std::str::FromStr;
use std::sync::Arc;
use std::time::Instant;

use common_base::base::tokio::runtime::Handle;
use common_base::base::tokio::task::block_in_place;
use common_catalog::catalog::StorageDescription;
use common_catalog::plan::DataSourcePlan;
use common_catalog::plan::PartStatistics;
use common_catalog::plan::Partitions;
use common_catalog::plan::PushDownInfo;
use common_catalog::plan::StreamColumn;
use common_catalog::table::Table;
use common_catalog::table_context::TableContext;
use common_exception::ErrorCode;
use common_exception::Result;
use common_expression::ORIGIN_BLOCK_ID_COL_NAME;
use common_expression::ORIGIN_BLOCK_ROW_NUM_COL_NAME;
use common_expression::ORIGIN_VERSION_COL_NAME;
use common_meta_app::schema::TableInfo;
use common_pipeline_core::Pipeline;
use common_sql::binder::STREAM_COLUMN_FACTORY;
use common_storages_fuse::io::SegmentsIO;
use common_storages_fuse::io::SnapshotsIO;
use common_storages_fuse::FuseTable;
use storages_common_table_meta::meta::SegmentInfo;
use storages_common_table_meta::table::OPT_KEY_SNAPSHOT_LOCATION;

use crate::stream_pruner::StreamPruner;

pub const STREAM_ENGINE: &str = "STREAM";

pub const OPT_KEY_TABLE_NAME: &str = "table_name";
pub const OPT_KEY_DATABASE_NAME: &str = "table_database";
pub const OPT_KEY_TABLE_ID: &str = "table_id";
pub const OPT_KEY_TABLE_VER: &str = "table_version";
pub const OPT_KEY_MODE: &str = "mode";

pub const MODE_APPEND_ONLY: &str = "append_only";

#[derive(Clone)]
pub enum StreamMode {
    AppendOnly,
}

pub enum StreamStatus {
    MayHaveData,
    NoData,
}

impl FromStr for StreamMode {
    type Err = ErrorCode;
    fn from_str(s: &str) -> Result<Self> {
        match s {
            MODE_APPEND_ONLY => Ok(StreamMode::AppendOnly),
            _ => Err(ErrorCode::IllegalStream(format!(
                "invalid stream mode: {}",
                s
            ))),
        }
    }
}

impl Display for StreamMode {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", match self {
            StreamMode::AppendOnly => MODE_APPEND_ONLY.to_string(),
        })
    }
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
            comment: "STREAM STORAGE Engine".to_string(),
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

    #[async_backtrace::framed]
    async fn do_read_partitions(
        &self,
        ctx: Arc<dyn TableContext>,
        push_downs: Option<PushDownInfo>,
    ) -> Result<(PartStatistics, Partitions)> {
        let start = Instant::now();
        let table = self.source_table(ctx.clone()).await?;
        let fuse_table = FuseTable::try_from_table(table.as_ref())?;
        let latest_snapshot = fuse_table.read_table_snapshot().await?;
        if latest_snapshot.is_none() {
            return Ok((PartStatistics::default(), Partitions::default()));
        }

        let latest_snapshot = latest_snapshot.unwrap();
        let latest_segments = HashSet::from_iter(latest_snapshot.segments.clone());

        let summary = latest_snapshot.summary.block_count as usize;
        drop(latest_snapshot);
        let operator = fuse_table.get_operator();
        let base_segments = if let Some(snapshot_location) = &self.snapshot_location {
            let (base_snapshot, _) =
                SnapshotsIO::read_snapshot(snapshot_location.clone(), operator.clone()).await?;
            HashSet::from_iter(base_snapshot.segments.clone())
        } else {
            HashSet::new()
        };

        let fuse_segment_io =
            SegmentsIO::create(ctx.clone(), operator.clone(), fuse_table.schema());

        let diff_in_base = base_segments
            .difference(&latest_segments)
            .cloned()
            .collect::<Vec<_>>();
        let diff_in_base = fuse_segment_io
            .read_segments::<SegmentInfo>(&diff_in_base, true)
            .await?;
        let mut base_blocks = HashSet::new();
        for segment in diff_in_base {
            let segment = segment?;
            segment.blocks.into_iter().for_each(|block| {
                base_blocks.insert(block.location.clone());
            })
        }

        let diff_in_latest = latest_segments
            .difference(&base_segments)
            .cloned()
            .collect::<Vec<_>>();
        let diff_in_latest = fuse_segment_io
            .read_segments::<SegmentInfo>(&diff_in_latest, true)
            .await?;
        let mut latest_blocks = Vec::new();
        for segment in diff_in_latest {
            let segment = segment?;
            segment.blocks.into_iter().for_each(|block| {
                if !base_blocks.contains(&block.location) {
                    latest_blocks.push(block);
                }
            });
        }
        if latest_blocks.is_empty() {
            return Ok((PartStatistics::default(), Partitions::default()));
        }

        let table_schema = fuse_table.schema_with_stream();
        let bloom_index_cols = fuse_table.bloom_index_cols();
        let (cluster_keys, cluster_key_meta) =
            if !fuse_table.is_native() || fuse_table.cluster_key_meta().is_none() {
                (vec![], None)
            } else {
                (
                    fuse_table.cluster_keys(ctx.clone()),
                    fuse_table.cluster_key_meta(),
                )
            };
        let stream_pruner = StreamPruner::create(
            &ctx,
            operator,
            table_schema.clone(),
            push_downs.clone(),
            cluster_key_meta,
            cluster_keys,
            bloom_index_cols,
        )?;

        let block_metas = stream_pruner.pruning(latest_blocks).await?;
        let pruning_stats = stream_pruner.pruning_stats();

        log::info!(
            "prune snapshot block end, final block numbers:{}, cost:{}",
            block_metas.len(),
            start.elapsed().as_secs()
        );

        let block_metas = block_metas
            .into_iter()
            .map(|(block_meta_index, block_meta)| (Some(block_meta_index), block_meta))
            .collect::<Vec<_>>();

        fuse_table.read_partitions_with_metas(
            ctx.clone(),
            table_schema,
            push_downs,
            &block_metas,
            summary,
            pruning_stats,
        )
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
        let table = block_in_place(|| {
            Handle::current().block_on(ctx.get_table(
                self.stream_info.catalog(),
                &self.table_database,
                &self.table_name,
            ))
        })?;
        table.read_data(ctx, plan, pipeline, put_cache)
    }
}
