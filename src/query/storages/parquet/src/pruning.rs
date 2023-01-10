// Copyright 2023 Datafuse Labs.
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
use std::collections::HashSet;
use std::sync::Arc;

use common_catalog::plan::Partitions;
use common_catalog::plan::PartitionsShuffleKind;
use common_catalog::table_context::TableContext;
use common_exception::Result;
use common_expression::Expr;
use common_expression::TableSchemaRef;
use common_storage::ColumnLeaves;
use storages_common_pruner::RangePrunerCreator;

use crate::parquet_part::ColumnMeta;
use crate::parquet_part::ParquetRowGroupPart;
use crate::ParquetReader;
use crate::ReadOptions;

/// Try to prune parquet files and gernerate the final row group partitions.
///
/// `ctx`: the table context.
///
/// `locations`: the parquet file locations.
///
/// `schema`: the projected table schema.
///
/// `filters`: the pushed-down filters.
///
/// `columns_to_read`: the projected column indices.
///
/// `column_leaves`: the projected column leaves.
///
/// `skip_pruning`: whether to skip pruning.
///
/// `read_options`: more information can be found in [`ReadOptions`].
#[allow(clippy::too_many_arguments)]
pub fn try_prune_parquets(
    ctx: &Arc<dyn TableContext>,
    locations: &[String],
    schema: &TableSchemaRef,
    filters: &Option<&[Expr<String>]>,
    columns_to_read: &HashSet<usize>,
    column_leaves: &ColumnLeaves,
    skip_pruning: bool,
    read_options: &ReadOptions,
) -> Result<()> {
    let mut partitions = Vec::with_capacity(locations.len());

    let row_group_pruner = RangePrunerCreator::try_create(ctx, *filters, schema)?;

    for location in locations {
        let file_meta = ParquetReader::read_meta(location)?;
        let mut row_group_pruned = vec![false; file_meta.row_groups.len()];

        let no_stats = file_meta.row_groups.iter().any(|r| {
            r.columns()
                .iter()
                .any(|c| c.metadata().statistics.is_none())
        });

        if read_options.need_prune() && !skip_pruning && !no_stats {
            // If collecting stats fails or `should_keep` is true, we still read the row group.
            // Otherwise, the row group will be pruned.
            if let Ok(row_group_stats) =
                ParquetReader::collect_row_group_stats(column_leaves, &file_meta.row_groups)
            {
                for (idx, (stats, _rg)) in row_group_stats
                    .iter()
                    .zip(file_meta.row_groups.iter())
                    .enumerate()
                {
                    row_group_pruned[idx] = !row_group_pruner.should_keep(stats);
                }
            }
        }

        for (idx, rg) in file_meta.row_groups.iter().enumerate() {
            if row_group_pruned[idx] {
                continue;
            }
            let mut column_metas = HashMap::with_capacity(columns_to_read.len());
            for index in columns_to_read {
                let c = &rg.columns()[*index];
                let (offset, length) = c.byte_range();
                column_metas.insert(*index, ColumnMeta {
                    offset,
                    length,
                    compression: c.compression(),
                });
            }

            partitions.push(ParquetRowGroupPart::create(
                location.clone(),
                rg.num_rows(),
                column_metas,
            ))
        }
    }
    ctx.try_set_partitions(Partitions::create(PartitionsShuffleKind::Mod, partitions))?;
    Ok(())
}
