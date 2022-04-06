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
//

use std::collections::HashMap;
use std::sync::Arc;

use common_exception::Result;
use common_planners::Extras;
use common_planners::PartInfoPtr;
use common_planners::Partitions;
use common_planners::Statistics;

use crate::sessions::QueryContext;
use crate::storages::fuse::fuse_part::ColumnMeta;
use crate::storages::fuse::fuse_part::FusePartInfo;
use crate::storages::fuse::meta::BlockMeta;
use crate::storages::fuse::meta::TableSnapshot;
use crate::storages::fuse::pruning::BlockPruner;
use crate::storages::fuse::FuseTable;

impl FuseTable {
    #[inline]
    pub async fn do_read_partitions(
        &self,
        ctx: Arc<QueryContext>,
        push_downs: Option<Extras>,
    ) -> Result<(Statistics, Partitions)> {
        let snapshot = self.read_table_snapshot(ctx.as_ref()).await?;
        match snapshot {
            Some(snapshot) => {
                if let Some(result) = self.check_quick_path(&snapshot, &push_downs) {
                    return Ok(result);
                }
                let schema = self.table_info.schema();
                let block_metas = BlockPruner::new(snapshot.clone())
                    .apply(schema, &push_downs, ctx.as_ref())
                    .await?;

                let partitions_scanned = block_metas.len();
                let partitions_total = snapshot.summary.block_count as usize;

                let (mut statistics, parts) = Self::to_partitions(&block_metas, push_downs);

                // Update planner statistics.
                statistics.partitions_total = partitions_total;
                statistics.partitions_scanned = partitions_scanned;

                // Update context statistics.
                ctx.get_dal_context()
                    .get_metrics()
                    .inc_partitions_total(partitions_total as u64);
                ctx.get_dal_context()
                    .get_metrics()
                    .inc_partitions_scanned(partitions_scanned as u64);

                Ok((statistics, parts))
            }
            None => Ok((Statistics::default(), vec![])),
        }
    }

    pub fn to_partitions(
        blocks_metas: &[BlockMeta],
        push_downs: Option<Extras>,
    ) -> (Statistics, Partitions) {
        let limit = push_downs
            .as_ref()
            .and_then(|p| p.limit)
            .unwrap_or(usize::MAX);
        let (mut statistics, partitions) = match &push_downs {
            None => Self::all_columns_partitions(blocks_metas, limit),
            Some(extras) => match &extras.projection {
                None => Self::all_columns_partitions(blocks_metas, limit),
                Some(projection) => Self::projection_partitions(blocks_metas, projection, limit),
            },
        };

        statistics.is_exact = statistics.is_exact && Self::is_exact(&push_downs);
        (statistics, partitions)
    }

    fn is_exact(push_downs: &Option<Extras>) -> bool {
        match push_downs {
            None => true,
            // We don't have limit push down in parquet reader
            Some(extra) => extra.filters.is_empty(),
        }
    }

    fn all_columns_partitions(metas: &[BlockMeta], limit: usize) -> (Statistics, Partitions) {
        let mut statistics = Statistics::default_exact();
        let mut partitions = Partitions::default();

        if limit == 0 {
            return (statistics, partitions);
        }

        let mut remaining = limit;

        for block_meta in metas {
            let rows = block_meta.row_count as usize;
            partitions.push(Self::all_columns_part(block_meta));
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
        metas: &[BlockMeta],
        indices: &[usize],
        limit: usize,
    ) -> (Statistics, Partitions) {
        let mut statistics = Statistics::default_exact();
        let mut partitions = Partitions::default();

        if limit == 0 {
            return (statistics, partitions);
        }

        let mut remaining = limit;

        for block_meta in metas {
            partitions.push(Self::projection_part(block_meta, indices));

            let rows = block_meta.row_count as usize;

            statistics.read_rows += rows;
            for projection_index in indices {
                let column_stats = &block_meta.col_stats;
                let column_stats = &column_stats[&(*projection_index as u32)];
                statistics.read_bytes += column_stats.in_memory_size as usize;
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

    fn all_columns_part(meta: &BlockMeta) -> PartInfoPtr {
        let mut columns_meta = HashMap::with_capacity(meta.col_metas.len());

        for (idx, column_meta) in &meta.col_metas {
            columns_meta.insert(
                *idx as usize,
                ColumnMeta::create(column_meta.offset, column_meta.len, column_meta.num_values),
            );
        }

        let rows_count = meta.row_count;
        let location = meta.location.0.clone();
        let format_version = meta.location.1;
        FusePartInfo::create(location, format_version, rows_count, columns_meta)
    }

    fn projection_part(meta: &BlockMeta, projections: &[usize]) -> PartInfoPtr {
        let mut columns_meta = HashMap::with_capacity(projections.len());

        for projection in projections {
            let column_meta = &meta.col_metas[&(*projection as u32)];

            columns_meta.insert(
                *projection,
                ColumnMeta::create(column_meta.offset, column_meta.len, column_meta.num_values),
            );
        }

        let rows_count = meta.row_count;
        let location = meta.location.0.clone();
        let format_version = meta.location.1;
        FusePartInfo::create(location, format_version, rows_count, columns_meta)
    }

    fn check_quick_path(
        &self,
        snapshot: &TableSnapshot,
        push_down: &Option<Extras>,
    ) -> Option<(Statistics, Partitions)> {
        push_down.as_ref().and_then(|extra| match extra {
            Extras {
                projection: Some(projs),
                filters,
                ..
            } if projs.is_empty() && filters.is_empty() => {
                let summary = &snapshot.summary;
                let stats = Statistics {
                    read_rows: summary.row_count as usize,
                    read_bytes: 0,
                    partitions_scanned: 0,
                    partitions_total: summary.block_count as usize,
                    is_exact: true,
                };
                Some((stats, vec![]))
            }
            _ => None,
        })
    }
}
