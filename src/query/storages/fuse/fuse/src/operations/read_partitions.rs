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

use std::collections::HashMap;
use std::sync::Arc;
use std::time::Instant;

use common_catalog::plan::PartInfoPtr;
use common_catalog::plan::PartStatistics;
use common_catalog::plan::Partitions;
use common_catalog::plan::PartitionsShuffleKind;
use common_catalog::plan::Projection;
use common_catalog::plan::PushDownInfo;
use common_catalog::table_context::TableContext;
use common_datavalues::DataSchemaRef;
use common_exception::Result;
use common_meta_app::schema::TableInfo;
use common_storage::ColumnLeaves;
use common_storages_table_meta::meta::BlockMeta;
use common_storages_table_meta::meta::ColumnMeta;
use common_storages_table_meta::meta::Location;
use common_storages_table_meta::meta::TableSnapshot;
use opendal::Operator;
use tracing::debug;
use tracing::info;

use crate::fuse_lazy_part::FuseLazyPartInfo;
use crate::fuse_part::FusePartInfo;
use crate::pruning::BlockPruner;
use crate::FuseTable;

impl FuseTable {
    #[tracing::instrument(level = "debug", name = "do_read_partitions", skip_all, fields(ctx.id = ctx.get_id().as_str()))]
    pub async fn do_read_partitions(
        &self,
        ctx: Arc<dyn TableContext>,
        push_downs: Option<PushDownInfo>,
    ) -> Result<(PartStatistics, Partitions)> {
        debug!("fuse table do read partitions, push downs:{:?}", push_downs);

        let snapshot = self.read_table_snapshot().await?;
        match snapshot {
            Some(snapshot) => {
                if let Some(result) = self.check_quick_path(&snapshot, &push_downs) {
                    return Ok(result);
                }

                let settings = ctx.get_settings();

                if settings.get_enable_distributed_eval_index()? {
                    let mut segments = Vec::with_capacity(snapshot.segments.len());
                    for segment_location in &snapshot.segments {
                        segments.push(FuseLazyPartInfo::create(segment_location.clone()))
                    }

                    return Ok((
                        PartStatistics::new_estimated(
                            snapshot.summary.row_count as usize,
                            snapshot.summary.compressed_byte_size as usize,
                            snapshot.segments.len(),
                            snapshot.segments.len(),
                        ),
                        Partitions::create(PartitionsShuffleKind::Mod, segments),
                    ));
                }

                let table_info = self.table_info.clone();
                let segments_location = snapshot.segments.clone();
                let summary = snapshot.summary.block_count as usize;
                self.prune_snapshot_blocks(
                    ctx.clone(),
                    self.operator.clone(),
                    push_downs.clone(),
                    table_info,
                    segments_location,
                    summary,
                )
                .await
            }
            None => Ok((PartStatistics::default(), Partitions::default())),
        }
    }

    #[tracing::instrument(level = "debug", name = "prune_snapshot_blocks", skip_all, fields(ctx.id = ctx.get_id().as_str()))]
    pub async fn prune_snapshot_blocks(
        &self,
        ctx: Arc<dyn TableContext>,
        dal: Operator,
        push_downs: Option<PushDownInfo>,
        table_info: TableInfo,
        segments_location: Vec<Location>,
        summary: usize,
    ) -> Result<(PartStatistics, Partitions)> {
        let start = Instant::now();
        info!(
            "prune snapshot block start, segment numbers:{}",
            segments_location.len()
        );

        let block_metas = BlockPruner::prune(
            &ctx,
            dal,
            table_info.schema(),
            &push_downs,
            segments_location,
        )
        .await?
        .into_iter()
        .map(|(_, v)| v)
        .collect::<Vec<_>>();

        info!(
            "prune snapshot block end, final block numbers:{}, cost:{}",
            block_metas.len(),
            start.elapsed().as_secs()
        );

        self.read_partitions_with_metas(ctx, table_info.schema(), push_downs, block_metas, summary)
    }

    pub fn read_partitions_with_metas(
        &self,
        _: Arc<dyn TableContext>,
        schema: DataSchemaRef,
        push_downs: Option<PushDownInfo>,
        block_metas: Vec<Arc<BlockMeta>>,
        partitions_total: usize,
    ) -> Result<(PartStatistics, Partitions)> {
        let arrow_schema = schema.to_arrow();
        let column_leaves = ColumnLeaves::new_from_schema(&arrow_schema);

        let partitions_scanned = block_metas.len();

        let (mut statistics, parts) = Self::to_partitions(&block_metas, &column_leaves, push_downs);

        // Update planner statistics.
        statistics.partitions_total = partitions_total;
        statistics.partitions_scanned = partitions_scanned;

        // Update context statistics.
        self.data_metrics
            .inc_partitions_total(partitions_total as u64);
        self.data_metrics
            .inc_partitions_scanned(partitions_scanned as u64);

        Ok((statistics, parts))
    }

    pub fn to_partitions(
        blocks_metas: &[Arc<BlockMeta>],
        column_leaves: &ColumnLeaves,
        push_down: Option<PushDownInfo>,
    ) -> (PartStatistics, Partitions) {
        let limit = push_down
            .as_ref()
            .filter(|p| p.order_by.is_empty())
            .and_then(|p| p.limit)
            .unwrap_or(usize::MAX);

        let (mut statistics, partitions) = match &push_down {
            None => Self::all_columns_partitions(blocks_metas, limit),
            Some(extras) => match &extras.projection {
                None => Self::all_columns_partitions(blocks_metas, limit),
                Some(projection) => {
                    Self::projection_partitions(blocks_metas, column_leaves, projection, limit)
                }
            },
        };

        statistics.is_exact = statistics.is_exact && Self::is_exact(&push_down);
        (statistics, partitions)
    }

    fn is_exact(push_downs: &Option<PushDownInfo>) -> bool {
        match push_downs {
            None => true,
            Some(extra) => extra.filters.is_empty(),
        }
    }

    pub fn all_columns_partitions(
        metas: &[Arc<BlockMeta>],
        limit: usize,
    ) -> (PartStatistics, Partitions) {
        let mut statistics = PartStatistics::default_exact();
        let mut partitions = Partitions::create(PartitionsShuffleKind::Mod, vec![]);

        if limit == 0 {
            return (statistics, partitions);
        }

        let mut remaining = limit;

        for block_meta in metas {
            let rows = block_meta.row_count as usize;
            partitions
                .partitions
                .push(Self::all_columns_part(block_meta));
            statistics.read_rows += rows;
            statistics.read_bytes += block_meta.block_size as usize;

            if remaining > rows {
                remaining -= rows;
            } else {
                // the last block we shall take
                if remaining != rows {
                    statistics.is_exact = false;
                }
                break;
            }
        }

        (statistics, partitions)
    }

    fn projection_partitions(
        metas: &[Arc<BlockMeta>],
        column_leaves: &ColumnLeaves,
        projection: &Projection,
        limit: usize,
    ) -> (PartStatistics, Partitions) {
        let mut statistics = PartStatistics::default_exact();
        let mut partitions = Partitions::default();

        if limit == 0 {
            return (statistics, partitions);
        }

        let mut remaining = limit;

        for block_meta in metas {
            partitions.partitions.push(Self::projection_part(
                block_meta,
                column_leaves,
                projection,
            ));
            let rows = block_meta.row_count as usize;

            statistics.read_rows += rows;
            let columns = projection.project_column_leaves(column_leaves).unwrap();
            for column in &columns {
                let indices = &column.leaf_ids;
                for index in indices {
                    let col_metas = &block_meta.col_metas[&(*index as u32)];
                    statistics.read_bytes += col_metas.len as usize;
                }
            }

            if remaining > rows {
                remaining -= rows;
            } else {
                // the last block we shall take
                if remaining != rows {
                    statistics.is_exact = false;
                }
                break;
            }
        }
        (statistics, partitions)
    }

    pub fn all_columns_part(meta: &BlockMeta) -> PartInfoPtr {
        let mut columns_meta = HashMap::with_capacity(meta.col_metas.len());

        for (idx, column_meta) in &meta.col_metas {
            columns_meta.insert(*idx as usize, column_meta.clone());
        }

        let rows_count = meta.row_count;
        let location = meta.location.0.clone();
        let format_version = meta.location.1;
        FusePartInfo::create(
            location,
            format_version,
            rows_count,
            columns_meta,
            meta.compression(),
        )
    }

    fn projection_part(
        meta: &BlockMeta,
        column_leaves: &ColumnLeaves,
        projection: &Projection,
    ) -> PartInfoPtr {
        let mut columns_meta = HashMap::with_capacity(projection.len());

        let columns = projection.project_column_leaves(column_leaves).unwrap();
        for column in &columns {
            let indices = &column.leaf_ids;
            for index in indices {
                let column_meta = &meta.col_metas[&(*index as u32)];

                columns_meta.insert(*index, column_meta.clone());
            }
        }

        let rows_count = meta.row_count;
        let location = meta.location.0.clone();
        let format_version = meta.location.1;
        // TODO
        // row_count should be a hint value of  LIMIT,
        // not the count the rows in this partition
        FusePartInfo::create(
            location,
            format_version,
            rows_count,
            columns_meta,
            meta.compression(),
        )
    }

    fn check_quick_path(
        &self,
        snapshot: &TableSnapshot,
        push_down: &Option<PushDownInfo>,
    ) -> Option<(PartStatistics, Partitions)> {
        push_down.as_ref().and_then(|extra| match extra {
            PushDownInfo {
                projection: Some(projs),
                filters,
                ..
            } if projs.is_empty() && filters.is_empty() => {
                let summary = &snapshot.summary;
                let stats = PartStatistics {
                    read_rows: summary.row_count as usize,
                    read_bytes: 0,
                    partitions_scanned: 0,
                    partitions_total: summary.block_count as usize,
                    is_exact: true,
                };
                Some((stats, Partitions::default()))
            }
            _ => None,
        })
    }
}
