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
use std::collections::HashSet;
use std::sync::Arc;

use arrow_schema::Schema as ArrowSchema;
use common_catalog::plan::ParquetReadOptions;
use common_catalog::plan::PartStatistics;
use common_catalog::plan::Projection;
use common_catalog::plan::PushDownInfo;
use common_catalog::plan::TopK;
use common_catalog::table_context::TableContext;
use common_exception::ErrorCode;
use common_exception::Result;
use common_expression::FieldIndex;
use common_expression::TableSchemaRef;
use common_functions::BUILTIN_FUNCTIONS;
use common_storage::parquet_rs::read_metadata;
use common_storage::parquet_rs::read_parquet_metas_in_parallel;
use opendal::Operator;
use parquet::file::metadata::ParquetMetaData;
use parquet::schema::types::SchemaDescPtr;
use parquet::schema::types::SchemaDescriptor;
use storages_common_index::Index;
use storages_common_index::RangeIndex;
use storages_common_pruner::RangePruner;
use storages_common_pruner::RangePrunerCreator;

use super::parquet_table::arrow_to_table_schema;
use super::projection::project_schema_all;
use crate::parquet_part::collect_small_file_parts;
use crate::parquet_part::ColumnMeta;
use crate::parquet_part::ParquetPart;
use crate::parquet_part::ParquetRowGroupPart;
use crate::parquet_rs_old::column_nodes::ColumnNodesRS;
use crate::parquet_rs_old::convert::convert_compression_to_arrow2;
use crate::parquet_rs_old::statistics::collect_row_group_stats;
use crate::processors::SmallFilePrunner;

/// Prune parquet row groups and pages.
pub struct ParquetPartitionPruner {
    /// Table schema.
    pub schema: TableSchemaRef,
    pub schema_descr: SchemaDescPtr,
    pub schema_from: String,
    /// Pruner to prune row groups.
    pub row_group_pruner: Option<Arc<dyn RangePruner + Send + Sync>>,
    /// Pruners to prune pages.
    pub page_pruners: Option<ColumnRangePruners>,
    /// The projected column indices.
    pub columns_to_read: HashSet<FieldIndex>,
    /// The projected column nodes.
    pub column_nodes: ColumnNodesRS,
    /// Whether to skip pruning.
    pub skip_pruning: bool,
    /// top k information from pushed down information. The usize is the offset of top k column in `schema`.
    pub top_k: Option<(TopK, usize)>,
    // TODO: use limit information for pruning
    // Limit of this query. If there is order by and filter, it will not be used (assign to `usize::MAX`).
    // pub limit: usize,
    pub parquet_fast_read_bytes: usize,
    pub compression_ratio: f64,
    pub max_memory_usage: u64,
}

fn check_parquet_schema(
    expect: &SchemaDescriptor,
    actual: &SchemaDescriptor,
    path: &str,
    schema_from: &str,
) -> Result<()> {
    if expect != actual {
        return Err(ErrorCode::BadBytes(format!(
            "infer schema from '{}', but get diff schema in file '{}'. Expected schema: {:?}, actual: {:?}",
            schema_from, path, expect, actual
        )));
    }
    Ok(())
}

impl SmallFilePrunner for ParquetPartitionPruner {
    fn prune_one_file(
        &self,
        path: &str,
        op: &Operator,
        file_size: u64,
    ) -> Result<Vec<ParquetRowGroupPart>> {
        let blocking_op = op.blocking();
        let file_meta = read_metadata(path, &blocking_op, file_size).map_err(|e| {
            ErrorCode::Internal(format!("Read parquet file '{}''s meta error: {}", path, e))
        })?;
        let (_, parts) = self.read_and_prune_file_meta(path, file_meta)?;
        Ok(parts)
    }
}

impl ParquetPartitionPruner {
    #[allow(clippy::too_many_arguments)]
    pub fn try_create(
        ctx: Arc<dyn TableContext>,
        arrow_schema: &ArrowSchema,
        schema_descr: SchemaDescPtr,
        schema_from: &str,
        push_down: &Option<PushDownInfo>,
        read_options: ParquetReadOptions,
        compression_ratio: f64,
        is_small_file: bool,
    ) -> Result<Self> {
        let settings = ctx.get_settings();
        let parquet_fast_read_bytes = if is_small_file {
            0_usize
        } else {
            settings.get_parquet_fast_read_bytes()? as usize
        };
        let projection = if let Some(PushDownInfo {
            projection: Some(prj),
            ..
        }) = push_down
        {
            prj.clone()
        } else {
            let indices = (0..arrow_schema.fields.len()).collect::<Vec<usize>>();
            Projection::Columns(indices)
        };

        let table_schema = arrow_to_table_schema(arrow_schema)?;
        let top_k = push_down
            .as_ref()
            .map(|p| p.top_k(&table_schema, None, RangeIndex::supported_type))
            .unwrap_or_default();

        // Currently, arrow2 doesn't support reading stats of a inner column of a nested type.
        // Therefore, if there is inner fields in projection, we skip the row group pruning.
        let skip_pruning = matches!(projection, Projection::InnerColumns(_));

        // Use `projected_column_nodes` to collect stats from row groups for pruning.
        // `projected_column_nodes` contains the smallest column set that is needed for the query.
        // Use `projected_arrow_schema` to create `row_group_pruner` (`RangePruner`).
        //
        // During pruning evaluation,
        // `RangePruner` will use field name to find the offset in the schema,
        // and use the offset to find the column stat from `StatisticsOfColumns` (HashMap<offset, stat>).
        //
        // How the stats are collected can be found in `ParquetReader::collect_row_group_stats`.
        let (projected_arrow_schema, projected_column_nodes, _, columns_to_read, _) =
            project_schema_all(arrow_schema, &schema_descr, &projection)?;
        let schema = Arc::new(arrow_to_table_schema(&projected_arrow_schema)?);

        let filter = push_down
            .as_ref()
            .and_then(|extra| extra.filter.as_ref().map(|f| f.as_expr(&BUILTIN_FUNCTIONS)));

        let top_k = top_k.map(|top_k| {
            let offset = projected_column_nodes
                .column_nodes
                .iter()
                .position(|node| node.leaf_indices[0] == top_k.column_id as usize)
                .unwrap();
            (top_k, offset)
        });

        let func_ctx = ctx.get_function_context()?;

        let row_group_pruner = if read_options.prune_row_groups() {
            Some(RangePrunerCreator::try_create(
                func_ctx,
                &schema,
                filter.as_ref(),
            )?)
        } else {
            None
        };

        Ok(ParquetPartitionPruner {
            schema,
            schema_descr,
            schema_from: schema_from.to_string(),
            row_group_pruner,
            page_pruners: None,
            columns_to_read,
            column_nodes: projected_column_nodes,
            skip_pruning,
            top_k,
            parquet_fast_read_bytes,
            compression_ratio,
            max_memory_usage: settings.get_max_memory_usage()?,
        })
    }

    #[async_backtrace::framed]
    pub fn read_and_prune_file_meta(
        &self,
        path: &str,
        file_meta: ParquetMetaData,
    ) -> Result<(PartStatistics, Vec<ParquetRowGroupPart>)> {
        check_parquet_schema(
            &self.schema_descr,
            &file_meta.file_metadata().schema_descr_ptr(),
            path,
            &self.schema_from,
        )?;
        let mut stats = PartStatistics::default();
        let mut partitions = vec![];

        let mut row_group_pruned = vec![false; file_meta.row_groups().len()];

        let no_stats = file_meta
            .row_groups()
            .iter()
            .any(|r| r.columns().iter().any(|c| c.statistics().is_none()));

        let row_group_stats = if no_stats {
            None
        } else if self.row_group_pruner.is_some() && !self.skip_pruning {
            let pruner = self.row_group_pruner.as_ref().unwrap();
            // If collecting stats fails or `should_keep` is true, we still read the row group.
            // Otherwise, the row group will be pruned.
            if let Ok(row_group_stats) =
                collect_row_group_stats(&self.column_nodes, file_meta.row_groups())
            {
                for (idx, (stats, _rg)) in row_group_stats
                    .iter()
                    .zip(file_meta.row_groups().iter())
                    .enumerate()
                {
                    row_group_pruned[idx] = !pruner.should_keep(stats, None);
                }
                Some(row_group_stats)
            } else {
                None
            }
        } else if self.top_k.is_some() {
            collect_row_group_stats(&self.column_nodes, file_meta.row_groups()).ok()
        } else {
            None
        };

        for (rg_idx, rg) in file_meta.row_groups().iter().enumerate() {
            if row_group_pruned[rg_idx] {
                continue;
            }

            stats.read_rows += rg.num_rows() as usize;
            stats.read_bytes += rg.total_byte_size() as usize;
            stats.partitions_scanned += 1;

            let mut column_metas = HashMap::with_capacity(self.columns_to_read.len());
            for index in self.columns_to_read.iter() {
                let c = &rg.columns()[*index];
                let (offset, length) = c.byte_range();

                let min_max = self
                    .top_k
                    .as_ref()
                    .filter(|(tk, _)| tk.column_id as usize == *index)
                    .zip(row_group_stats.as_ref())
                    .map(|((_, offset), stats)| {
                        let stat = stats[rg_idx].get(&(*offset as u32)).unwrap();
                        (stat.min().clone(), stat.max().clone())
                    });

                column_metas.insert(*index, ColumnMeta {
                    offset,
                    length,
                    num_values: c.num_values(),
                    compression: convert_compression_to_arrow2(c.compression()),
                    uncompressed_size: c.uncompressed_size() as u64,
                    min_max,
                    has_dictionary: c.dictionary_page_offset().is_some(),
                });
            }

            partitions.push(ParquetRowGroupPart {
                location: path.to_string(),
                num_rows: rg.num_rows() as usize,
                column_metas,
                row_selection: None,
                sort_min_max: None,
            })
        }
        Ok((stats, partitions))
    }

    /// Try to read parquet meta to generate row-group-wise partitions.
    /// And prune row groups an pages to generate the final row group partitions.
    #[async_backtrace::framed]
    pub async fn read_and_prune_partitions(
        &self,
        operator: Operator,
        locations: &[(String, u64)],
    ) -> Result<(PartStatistics, Vec<ParquetPart>)> {
        // part stats
        let mut stats = PartStatistics::default();

        let mut large_files = vec![];
        let mut small_files = vec![];
        for (location, size) in locations {
            if *size > self.parquet_fast_read_bytes as u64 {
                large_files.push((location.clone(), *size));
            } else {
                small_files.push((location.clone(), *size));
            }
        }

        let mut partitions = Vec::with_capacity(locations.len());

        let is_blocking_io = operator.info().can_blocking();

        // 1. Read parquet meta data. Distinguish between sync and async reading.
        let file_metas = if is_blocking_io {
            let mut file_metas = Vec::with_capacity(locations.len());
            for (path, size) in &large_files {
                let file_meta = read_metadata(path, &operator.blocking(), *size).map_err(|e| {
                    ErrorCode::Internal(format!("Read parquet file '{}''s meta error: {}", path, e))
                })?;
                file_metas.push(file_meta);
            }
            file_metas
        } else {
            read_parquet_metas_in_parallel(
                operator.clone(),
                large_files.clone(),
                16,
                64,
                self.max_memory_usage,
            )
            .await?
        };

        // 2. Use file meta to prune row groups or pages.
        let mut max_compression_ratio = self.compression_ratio;
        let mut max_compressed_size = 0u64;

        // If one row group does not have stats, we cannot use the stats for topk optimization.
        for (file_id, file_meta) in file_metas.into_iter().enumerate() {
            stats.partitions_total += file_meta.row_groups().len();
            let (sub_stats, parts) =
                self.read_and_prune_file_meta(&large_files[file_id].0, file_meta)?;
            for p in parts {
                max_compression_ratio = max_compression_ratio
                    .max(p.uncompressed_size() as f64 / p.compressed_size() as f64);
                max_compressed_size = max_compressed_size.max(p.compressed_size());
                partitions.push(ParquetPart::RowGroup(p));
            }
            stats.partitions_total += sub_stats.partitions_total;
            stats.partitions_scanned += sub_stats.partitions_scanned;
            stats.read_bytes += sub_stats.read_bytes;
            stats.read_rows += sub_stats.read_rows;
        }

        let num_large_partitions = partitions.len();

        collect_small_file_parts(
            small_files,
            max_compression_ratio,
            max_compressed_size,
            &mut partitions,
            &mut stats,
            self.columns_to_read.len(),
        );

        log::info!(
            "copy {num_large_partitions} large partitions and {} small partitions.",
            partitions.len() - num_large_partitions
        );

        Ok((stats, partitions))
    }
}

/// [`RangePruner`]s for each column
type ColumnRangePruners = Vec<(usize, Arc<dyn RangePruner + Send + Sync>)>;
