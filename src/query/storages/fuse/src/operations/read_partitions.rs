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

use std::collections::HashMap;
use std::sync::Arc;
use std::time::Instant;

use common_catalog::plan::PartInfoPtr;
use common_catalog::plan::PartStatistics;
use common_catalog::plan::Partitions;
use common_catalog::plan::PartitionsShuffleKind;
use common_catalog::plan::Projection;
use common_catalog::plan::PruningStatistics;
use common_catalog::plan::PushDownInfo;
use common_catalog::plan::TopK;
use common_catalog::table::Table;
use common_catalog::table_context::TableContext;
use common_exception::Result;
use common_expression::TableSchemaRef;
use common_meta_app::schema::TableInfo;
use common_storage::ColumnNodes;
use opendal::Operator;
use sha2::Digest;
use sha2::Sha256;
use storages_common_cache::CacheAccessor;
use storages_common_cache_manager::CachedObject;
use storages_common_index::Index;
use storages_common_index::RangeIndex;
use storages_common_pruner::BlockMetaIndex;
use storages_common_table_meta::meta::BlockMeta;
use storages_common_table_meta::meta::Location;
use tracing::debug;
use tracing::info;

use crate::fuse_lazy_part::FuseLazyPartInfo;
use crate::fuse_part::FusePartInfo;
use crate::pruning::FusePruner;
use crate::FuseTable;

impl FuseTable {
    #[tracing::instrument(level = "debug", name = "do_read_partitions", skip_all, fields(ctx.id = ctx.get_id().as_str()))]
    #[async_backtrace::framed]
    pub async fn do_read_partitions(
        &self,
        ctx: Arc<dyn TableContext>,
        push_downs: Option<PushDownInfo>,
    ) -> Result<(PartStatistics, Partitions)> {
        debug!("fuse table do read partitions, push downs:{:?}", push_downs);
        let snapshot = self.read_table_snapshot().await?;
        let is_lazy = push_downs
            .as_ref()
            .map(|p| p.lazy_materialization)
            .unwrap_or_default();
        match snapshot {
            Some(snapshot) => {
                let settings = ctx.get_settings();
                if (settings.get_enable_distributed_eval_index()? && !ctx.get_cluster().is_empty())
                    || is_lazy
                {
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
                        Partitions::create(PartitionsShuffleKind::Mod, segments, true),
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
                    None,
                )
                .await
            }
            None => Ok((PartStatistics::default(), Partitions::default())),
        }
    }

    #[allow(clippy::too_many_arguments)]
    #[tracing::instrument(level = "debug", name = "prune_snapshot_blocks", skip_all, fields(ctx.id = ctx.get_id().as_str()))]
    #[async_backtrace::framed]
    pub async fn prune_snapshot_blocks(
        &self,
        ctx: Arc<dyn TableContext>,
        dal: Operator,
        push_downs: Option<PushDownInfo>,
        table_info: TableInfo,
        segments_location: Vec<Location>,
        summary: usize,
        segment_id_map: Option<HashMap<Location, usize>>,
    ) -> Result<(PartStatistics, Partitions)> {
        let start = Instant::now();
        info!(
            "prune snapshot block start, segment numbers:{}",
            segments_location.len()
        );

        type CacheItem = (PartStatistics, Partitions);

        let derterministic_cache_key =
            push_downs
                .as_ref()
                .filter(|p| p.is_deterministic)
                .map(|push_downs| {
                    format!(
                        "{:x}",
                        Sha256::digest(format!("{:?}_{:?}", segments_location, push_downs))
                    )
                });

        if let Some(cache_key) = derterministic_cache_key.as_ref() {
            if let Some(cache) = CacheItem::cache() {
                if let Some(data) = cache.get(cache_key) {
                    info!(
                        "prune snapshot block from cache, final block numbers:{}, cost:{}",
                        data.1.len(),
                        start.elapsed().as_secs()
                    );
                    return Ok((data.0.clone(), data.1.clone()));
                }
            }
        }

        let pruner = if !self.is_native() || self.cluster_key_meta.is_none() {
            FusePruner::create(&ctx, dal, table_info.schema(), &push_downs)?
        } else {
            let cluster_keys = self.cluster_keys(ctx.clone());

            FusePruner::create_with_pages(
                &ctx,
                dal,
                table_info.schema(),
                &push_downs,
                self.cluster_key_meta.clone(),
                cluster_keys,
            )?
        };
        let snapshot_loc = self.snapshot_loc().await?;
        let block_metas = pruner
            .pruning(segments_location, snapshot_loc, segment_id_map)
            .await?;
        let pruning_stats = pruner.pruning_stats();

        info!(
            "prune snapshot block end, final block numbers:{}, cost:{}",
            block_metas.len(),
            start.elapsed().as_secs()
        );

        let block_metas = block_metas
            .into_iter()
            .map(|(block_meta_index, block_meta)| (Some(block_meta_index), block_meta))
            .collect::<Vec<_>>();

        let result = self.read_partitions_with_metas(
            table_info.schema(),
            push_downs,
            &block_metas,
            summary,
            pruning_stats,
        )?;

        if let Some(cache_key) = derterministic_cache_key {
            if let Some(cache) = CacheItem::cache() {
                cache.put(cache_key, Arc::new(result.clone()));
            }
        }
        Ok(result)
    }

    pub fn read_partitions_with_metas(
        &self,
        schema: TableSchemaRef,
        push_downs: Option<PushDownInfo>,
        block_metas: &[(Option<BlockMetaIndex>, Arc<BlockMeta>)],
        partitions_total: usize,
        pruning_stats: PruningStatistics,
    ) -> Result<(PartStatistics, Partitions)> {
        let arrow_schema = schema.to_arrow();
        let column_nodes = ColumnNodes::new_from_schema(&arrow_schema, Some(&schema));

        let partitions_scanned = block_metas.len();

        let top_k = push_downs
            .as_ref()
            .map(|p| {
                p.top_k(
                    self.schema().as_ref(),
                    self.cluster_key_str(),
                    RangeIndex::supported_type,
                )
            })
            .unwrap_or_default();
        let (mut statistics, parts) =
            Self::to_partitions(Some(&schema), block_metas, &column_nodes, top_k, push_downs);

        // Update planner statistics.
        statistics.partitions_total = partitions_total;
        statistics.partitions_scanned = partitions_scanned;
        statistics.pruning_stats = pruning_stats;

        // Update context statistics.
        self.data_metrics
            .inc_partitions_total(partitions_total as u64);
        self.data_metrics
            .inc_partitions_scanned(partitions_scanned as u64);

        Ok((statistics, parts))
    }

    pub fn to_partitions(
        schema: Option<&TableSchemaRef>,
        block_metas: &[(Option<BlockMetaIndex>, Arc<BlockMeta>)],
        column_nodes: &ColumnNodes,
        top_k: Option<TopK>,
        push_down: Option<PushDownInfo>,
    ) -> (PartStatistics, Partitions) {
        let limit = push_down
            .as_ref()
            .filter(|p| p.order_by.is_empty() && p.filter.is_none())
            .and_then(|p| p.limit)
            .unwrap_or(usize::MAX);

        let mut block_metas = block_metas.to_vec();
        if let Some(top_k) = &top_k {
            block_metas.sort_by(|a, b| {
                let a = a.1.col_stats.get(&top_k.column_id).unwrap();
                let b = b.1.col_stats.get(&top_k.column_id).unwrap();

                if top_k.asc {
                    (a.min.as_ref(), a.max.as_ref()).cmp(&(b.min.as_ref(), b.max.as_ref()))
                } else {
                    (b.max.as_ref(), b.min.as_ref()).cmp(&(a.max.as_ref(), a.min.as_ref()))
                }
            });
        }

        let (mut statistics, mut partitions) = match &push_down {
            None => Self::all_columns_partitions(schema, &block_metas, top_k.clone(), limit),
            Some(extras) => match &extras.projection {
                None => Self::all_columns_partitions(schema, &block_metas, top_k.clone(), limit),
                Some(projection) => Self::projection_partitions(
                    &block_metas,
                    column_nodes,
                    projection,
                    top_k.clone(),
                    limit,
                ),
            },
        };

        if top_k.is_some() {
            partitions.kind = PartitionsShuffleKind::Seq;
        }

        statistics.is_exact = statistics.is_exact && Self::is_exact(&push_down);
        (statistics, partitions)
    }

    fn is_exact(push_downs: &Option<PushDownInfo>) -> bool {
        match push_downs {
            None => true,
            Some(extra) => extra.filter.is_none(),
        }
    }

    fn all_columns_partitions(
        schema: Option<&TableSchemaRef>,
        block_metas: &[(Option<BlockMetaIndex>, Arc<BlockMeta>)],
        top_k: Option<TopK>,
        limit: usize,
    ) -> (PartStatistics, Partitions) {
        let mut statistics = PartStatistics::default_exact();
        let mut partitions = Partitions::create_nolazy(PartitionsShuffleKind::Mod, vec![]);

        if limit == 0 {
            return (statistics, partitions);
        }

        let mut remaining = limit;
        for (block_meta_index, block_meta) in block_metas.iter() {
            let rows = block_meta.row_count as usize;
            partitions.partitions.push(Self::all_columns_part(
                schema,
                block_meta_index,
                &top_k,
                block_meta,
            ));
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
        block_metas: &[(Option<BlockMetaIndex>, Arc<BlockMeta>)],
        column_nodes: &ColumnNodes,
        projection: &Projection,
        top_k: Option<TopK>,
        limit: usize,
    ) -> (PartStatistics, Partitions) {
        let mut statistics = PartStatistics::default_exact();
        let mut partitions = Partitions::default();

        if limit == 0 {
            return (statistics, partitions);
        }

        let columns = projection.project_column_nodes(column_nodes).unwrap();
        let mut remaining = limit;

        for (block_meta_index, block_meta) in block_metas {
            partitions.partitions.push(Self::projection_part(
                block_meta,
                block_meta_index,
                column_nodes,
                top_k.clone(),
                projection,
            ));

            let rows = block_meta.row_count as usize;

            statistics.read_rows += rows;
            for column in &columns {
                for column_id in &column.leaf_column_ids {
                    // ignore all deleted field
                    if let Some(col_metas) = block_meta.col_metas.get(column_id) {
                        let (_, len) = col_metas.offset_length();
                        statistics.read_bytes += len as usize;
                    }
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

    fn all_columns_part(
        schema: Option<&TableSchemaRef>,
        block_meta_index: &Option<BlockMetaIndex>,
        top_k: &Option<TopK>,
        meta: &BlockMeta,
    ) -> PartInfoPtr {
        let mut columns_meta = HashMap::with_capacity(meta.col_metas.len());

        for column_id in meta.col_metas.keys() {
            // ignore all deleted field
            if let Some(schema) = schema {
                if schema.is_column_deleted(*column_id) {
                    continue;
                }
            }

            // ignore column this block dose not exist
            if let Some(meta) = meta.col_metas.get(column_id) {
                columns_meta.insert(*column_id, meta.clone());
            }
        }

        let rows_count = meta.row_count;
        let location = meta.location.0.clone();
        let format_version = meta.location.1;

        let sort_min_max = top_k.as_ref().map(|top_k| {
            let stat = meta.col_stats.get(&top_k.column_id).unwrap();
            (stat.min.clone(), stat.max.clone())
        });

        FusePartInfo::create(
            location,
            format_version,
            rows_count,
            columns_meta,
            meta.compression(),
            sort_min_max,
            block_meta_index.to_owned(),
        )
    }

    pub(crate) fn projection_part(
        meta: &BlockMeta,
        block_meta_index: &Option<BlockMetaIndex>,
        column_nodes: &ColumnNodes,
        top_k: Option<TopK>,
        projection: &Projection,
    ) -> PartInfoPtr {
        let mut columns_meta = HashMap::with_capacity(projection.len());

        let columns = projection.project_column_nodes(column_nodes).unwrap();
        for column in &columns {
            for column_id in &column.leaf_column_ids {
                // ignore column this block dose not exist
                if let Some(column_meta) = meta.col_metas.get(column_id) {
                    columns_meta.insert(*column_id, column_meta.clone());
                }
            }
        }

        let rows_count = meta.row_count;
        let location = meta.location.0.clone();
        let format_version = meta.location.1;

        let sort_min_max = top_k.and_then(|top_k| {
            let stat = meta.col_stats.get(&top_k.column_id);
            stat.map(|stat| (stat.min.clone(), stat.max.clone()))
        });

        // TODO
        // row_count should be a hint value of  LIMIT,
        // not the count the rows in this partition
        FusePartInfo::create(
            location,
            format_version,
            rows_count,
            columns_meta,
            meta.compression(),
            sort_min_max,
            block_meta_index.to_owned(),
        )
    }
}
