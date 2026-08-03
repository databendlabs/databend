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

//! Independent vertical recluster executor.
//!
//! `SortBlocks` owns one source and one output writer at a time. `MergeBlocks`
//! builds a task-wide row-source stream from batched key inputs, then rewrites
//! one physical field at a time. This module deliberately does not share the
//! horizontal sort/compact/serialize processors.

use std::cmp::Ordering;
use std::collections::BTreeSet;
use std::ops::Range;
use std::sync::Arc;

use databend_common_catalog::plan::ReclusterTask;
use databend_common_catalog::plan::VerticalReclusterKind;
use databend_common_catalog::table::Table;
use databend_common_catalog::table_context::TableContext;
use databend_common_exception::ErrorCode;
use databend_common_exception::Result;
use databend_common_expression::BlockEntry;
use databend_common_expression::Column;
use databend_common_expression::ColumnBuilder;
use databend_common_expression::DataBlock;
use databend_common_expression::Expr;
use databend_common_expression::FunctionContext;
use databend_common_expression::Scalar;
use databend_common_expression::TableSchemaRef;
use databend_common_expression::compare_columns;
use databend_common_expression::is_stream_column_id;
use databend_common_expression::types::DataType;
use databend_common_pipeline::sources::SyncSource;
use databend_common_sql::DefaultExprBinder;
use databend_common_sql::executor::physical_plans::MutationKind;
use databend_common_sql::parse_cluster_keys;
use databend_storages_common_table_meta::meta::BlockMeta;
use databend_storages_common_table_meta::meta::ExtendedBlockMeta;
use databend_storages_common_table_meta::meta::TableMetaTimestamps;
use databend_storages_common_table_meta::meta::Versioned;

use crate::FuseBlockPartInfo;
use crate::FuseStorageFormat;
use crate::FuseTable;
use crate::io::FuseBlockWriteOptions;
use crate::io::FuseLowLevelBlockReadOptions;
use crate::io::FuseLowLevelBlockReader;
use crate::io::FuseLowLevelBlockWriter;
use crate::io::FuseLowLevelClusterKeyReader;
use crate::io::FuseLowLevelColumnBatchReader;
use crate::operations::MutationLogEntry;
use crate::operations::MutationLogs;

const MERGE_BATCH_ROWS: usize = 8192;
const MAPPING_SAFETY_BYTES: usize = 256 * 1024;

pub struct VerticalReclusterSource {
    ctx: Arc<dyn TableContext>,
    table: FuseTable,
    task: ReclusterTask,
    table_meta_timestamps: TableMetaTimestamps,
    finished: bool,
}

impl VerticalReclusterSource {
    pub fn create(
        ctx: Arc<dyn TableContext>,
        table: FuseTable,
        task: ReclusterTask,
        table_meta_timestamps: TableMetaTimestamps,
    ) -> Self {
        Self {
            ctx,
            table,
            task,
            table_meta_timestamps,
            finished: false,
        }
    }

    fn execute(&self) -> Result<DataBlock> {
        let Some(kind) = &self.task.vertical_kind else {
            return Err(ErrorCode::Internal(
                "vertical recluster source received a horizontal task",
            ));
        };

        if self.table.get_write_settings().storage_format != FuseStorageFormat::Parquet {
            return Err(ErrorCode::Unimplemented(
                "vertical recluster supports Parquet FUSE blocks only".to_string(),
            ));
        }

        let schema = self.table.schema_with_stream();
        let schema = Arc::new(schema.remove_virtual_computed_fields());

        let source_metas = self.source_metas()?;
        let key_exprs = self.cluster_key_exprs(schema.clone())?;

        if key_exprs.is_empty() {
            return Err(ErrorCode::Unimplemented(
                "vertical recluster can't supports empty keys".to_string(),
            ));
        }

        let mut key_types = Vec::with_capacity(key_exprs.len());
        for key_expr in &key_exprs {
            let data_type = key_expr.data_type().clone();
            if data_type.remove_nullable().is_vector() {
                return Err(ErrorCode::Unimplemented(
                    "vertical recluster supports pure scalar cluster keys only".to_string(),
                ));
            }
            key_types.push(data_type);
        }
        let cluster_key_func_ctx = self.ctx.get_function_context()?;

        let block_options = FuseBlockWriteOptions::try_create(
            self.ctx.clone(),
            &self.table,
            MutationKind::Recluster,
            self.table_meta_timestamps,
        )?;

        let outputs = match kind {
            VerticalReclusterKind::SortBlocks => {
                if source_metas.len() != 1 {
                    return Err(ErrorCode::Internal(format!(
                        "SortBlocks requires one source, got {}",
                        source_metas.len()
                    )));
                }

                let source_meta = source_metas.into_iter().next().unwrap();
                let source_rows = usize::try_from(source_meta.row_count).map_err(|_| {
                    ErrorCode::BadArguments(format!(
                        "SortBlocks source row count {} does not fit usize",
                        source_meta.row_count
                    ))
                })?;
                let settings = self.ctx.get_settings();
                let thresholds = self.table.get_block_thresholds();
                let max_output_rows =
                    (settings.get_max_block_size()? as usize).min(thresholds.max_rows_per_block);
                let max_output_bytes =
                    (settings.get_max_block_bytes()? as usize).min(thresholds.max_bytes_per_block);
                let output_rows = output_rows_by_size(
                    source_rows,
                    self.task.total_bytes,
                    max_output_rows,
                    max_output_bytes,
                );
                self.execute_sort_blocks(
                    schema,
                    source_meta,
                    &key_exprs,
                    &cluster_key_func_ctx,
                    &key_types,
                    block_options,
                    output_rows,
                )?
            }
            VerticalReclusterKind::MergeBlocks => {
                if !self.task.all_ordered {
                    return Err(ErrorCode::Internal(
                        "MergeBlocks requires planner-verified ordered sources",
                    ));
                }

                self.execute_merge_blocks(
                    schema,
                    source_metas,
                    &key_exprs,
                    &cluster_key_func_ctx,
                    &key_types,
                    block_options,
                )?
            }
        };

        let input_rows = self.task.total_rows as u64;
        let mut output_row_count = 0u64;
        for output in &outputs {
            let Some(rows) = output_row_count.checked_add(output.block_meta.row_count) else {
                return Err(ErrorCode::Internal(
                    "vertical recluster output row count overflowed u64",
                ));
            };
            output_row_count = rows;
        }
        if input_rows != output_row_count {
            return Err(ErrorCode::Internal(format!(
                "vertical recluster row-count mismatch: input {input_rows}, output {output_row_count}"
            )));
        }

        let mut entries = Vec::with_capacity(outputs.len());
        for output in outputs {
            entries.push(MutationLogEntry::AppendBlock {
                block_meta: Arc::new(output),
                merge_hll: false,
            });
        }
        Ok(MutationLogs {
            entries,
            logical_updated_rows: 0,
            logical_deleted_rows: 0,
        }
        .into())
    }

    fn source_metas(&self) -> Result<Vec<Arc<BlockMeta>>> {
        let partitions = &self.task.parts.partitions;
        let mut metas = Vec::with_capacity(partitions.len());
        for partition in partitions {
            let part = FuseBlockPartInfo::from_part(partition)?;
            let meta = BlockMeta {
                row_count: part.nums_rows as u64,
                block_size: 0,
                file_size: 0,
                col_stats: part.columns_stat.clone().unwrap_or_default(),
                col_metas: part.columns_meta.clone(),
                cluster_stats: None,
                // Reconstructed only for reading source blocks; FuseBlockPartInfo does not
                // carry PARTITION BY values.
                partition_stats: None,
                location: (part.location.clone(), DataBlock::VERSION),
                bloom_filter_index_location: part.bloom_filter_index_location.clone(),
                bloom_filter_index_size: part.bloom_filter_index_size,
                inverted_index_size: None,
                ngram_filter_index_size: None,
                vector_index_size: None,
                vector_index_location: None,
                spatial_index_size: None,
                spatial_index_location: None,
                spatial_stats: None,
                granule_index: part.granule_index.clone(),
                vector_stats: None,
                virtual_block_meta: None,
                compression: part.compression,
                create_on: part.create_on,
            };
            metas.push(Arc::new(meta));
        }
        Ok(metas)
    }

    fn cluster_key_exprs(&self, schema: TableSchemaRef) -> Result<Vec<Expr<usize>>> {
        let Some(ast) = self.table.resolve_cluster_keys() else {
            return Err(ErrorCode::InvalidClusterKeys(
                "vertical recluster requires cluster keys",
            ));
        };
        let table_schema = self.table.schema();
        let parsed = parse_cluster_keys(self.ctx.clone(), Arc::new(self.table.clone()), ast)?;
        let mut projected = Vec::with_capacity(parsed.len());
        for expr in parsed {
            let expr = expr.project_column_ref(|index| {
                let name = table_schema.field(*index).name();
                schema.index_of(name)
            })?;
            projected.push(expr);
        }
        Ok(projected)
    }

    fn execute_sort_blocks(
        &self,
        schema: TableSchemaRef,
        source: Arc<BlockMeta>,
        cluster_key_exprs: &[Expr<usize>],
        cluster_key_func_ctx: &FunctionContext,
        key_types: &[DataType],
        block_options: Arc<FuseBlockWriteOptions>,
        output_rows: usize,
    ) -> Result<Vec<ExtendedBlockMeta>> {
        let Ok(rows) = usize::try_from(source.row_count) else {
            return Err(ErrorCode::BadArguments(format!(
                "SortBlocks source row count {} does not fit usize",
                source.row_count
            )));
        };

        let reader_bytes = FuseLowLevelBlockReader::retained_window_bytes(
            schema
                .to_leaf_column_ids()
                .into_iter()
                .filter(|column_id| source.col_metas.contains_key(column_id))
                .count(),
        );
        let writer_bytes = block_options.retained_index_bytes(output_rows);
        let batch_bytes = sort_batch_working_bytes(self.task.total_bytes, rows, output_rows)?;
        let retained_bytes =
            checked_retained_sum("SortBlocks", &[reader_bytes, writer_bytes, batch_bytes])?;
        ensure_memory_budget("SortBlocks", retained_bytes, self.task.memory_budget)?;

        let block_reader = self.create_block_reader(
            schema.clone(),
            source,
            Some((cluster_key_exprs, cluster_key_func_ctx)),
        )?;
        let mut cluster_key_reader = block_reader.read_cluster_keys()?;

        let mut outputs = Vec::new();
        for range in ranges(rows, output_rows) {
            let (keys, mut source_columns) = cluster_key_reader.read_rows(range.len())?;
            let permutation = compare_columns(keys.clone(), range.len())?;
            let sorted_keys = take_columns(&keys, permutation.as_slice())?;

            let write_options = block_options.create_low_level_options(
                self.table.get_operator(),
                key_types.to_vec(),
                self.table.cluster_key_id().unwrap(),
                self.task.level + 1,
                range.len(),
            )?;

            let block_writer = FuseLowLevelBlockWriter::create(write_options)?;
            let mut key_writer = block_writer.write_cluster_keys()?;
            key_writer.write_columns(&sorted_keys)?;
            let block_writer = key_writer.finish()?;
            drop(keys);
            drop(sorted_keys);

            let mut data_writer = block_writer.write_data()?;
            for field_idx in 0..schema.num_fields() {
                let source_column = match source_columns.remove(&field_idx) {
                    Some(column) => column,
                    None => cluster_key_reader.read_column_rows(field_idx, range.len())?,
                };
                let mut reordered_columns = take_columns(&[source_column], permutation.as_slice())?;
                let Some(reordered) = reordered_columns.pop() else {
                    return Err(ErrorCode::Internal(
                        "SortBlocks produced no reordered column",
                    ));
                };

                let mut column_writer = data_writer.next_column()?;
                column_writer.write(&reordered)?;
                data_writer = column_writer.finish()?;
            }

            let block_writer = data_writer.finish()?;
            let output = block_writer.finish()?;
            outputs.push(ExtendedBlockMeta {
                block_meta: output.block_meta,
                draft_virtual_block_meta: output.draft_virtual_block_meta,
                column_hlls: output.column_hlls,
                column_top_n: output.column_top_n,
            });
        }

        cluster_key_reader.finish()?;
        Ok(outputs)
    }

    fn execute_merge_blocks(
        &self,
        schema: TableSchemaRef,
        sources: Vec<Arc<BlockMeta>>,
        cluster_key_exprs: &[Expr<usize>],
        cluster_key_func_ctx: &FunctionContext,
        key_types: &[DataType],
        block_options: Arc<FuseBlockWriteOptions>,
    ) -> Result<Vec<ExtendedBlockMeta>> {
        if sources.len() > u32::MAX as usize {
            return Err(ErrorCode::Internal(format!(
                "MergeBlocks has {} sources, exceeding row-source stream capacity",
                sources.len()
            )));
        }

        let mut total_rows = 0usize;
        let mut source_rows = Vec::with_capacity(sources.len());

        for source in &sources {
            let Ok(rows) = usize::try_from(source.row_count) else {
                return Err(ErrorCode::Internal(format!(
                    "MergeBlocks source row count {} does not fit usize",
                    source.row_count
                )));
            };

            let Some(next_total_rows) = total_rows.checked_add(rows) else {
                return Err(ErrorCode::Internal(
                    "MergeBlocks total source row count overflowed usize",
                ));
            };
            total_rows = next_total_rows;

            source_rows.push(rows);
        }

        if total_rows == 0 {
            return Err(ErrorCode::Internal(
                "MergeBlocks must produce one non-empty output block",
            ));
        }

        let max_output_bytes = self.table.get_block_thresholds().max_bytes_per_block;
        let output_rows = output_rows_by_size(
            total_rows,
            self.task.total_bytes,
            total_rows,
            max_output_bytes,
        );
        let output_ranges = ranges(total_rows, output_rows);

        let mapping_bytes = row_source_mapping_bytes(total_rows)?;
        let cluster_key_fields = cluster_key_exprs
            .iter()
            .flat_map(|expr| expr.column_refs().into_keys())
            .collect::<BTreeSet<_>>();
        let key_leaf_count = sources
            .iter()
            .map(|source| cluster_key_physical_leaf_count(&cluster_key_fields, &schema, source))
            .sum();
        let key_reader_bytes = FuseLowLevelBlockReader::retained_window_bytes(key_leaf_count);
        let payload_leaf_count = schema
            .fields()
            .iter()
            .map(|field| {
                let leaf_ids = field.leaf_column_ids();
                sources
                    .iter()
                    .map(|source| {
                        leaf_ids
                            .iter()
                            .filter(|column_id| source.col_metas.contains_key(column_id))
                            .count()
                    })
                    .sum()
            })
            .max()
            .unwrap_or(0);
        let payload_reader_bytes =
            FuseLowLevelBlockReader::retained_window_bytes(payload_leaf_count);
        let writer_bytes = output_ranges.iter().try_fold(0usize, |total, range| {
            total
                .checked_add(block_options.retained_index_bytes(range.len()))
                .ok_or_else(|| {
                    ErrorCode::MemoryExceedsLimit(
                        "MergeBlocks writer retained-memory estimate overflowed usize",
                    )
                })
        })?;
        let resident_rows =
            source_rows
                .iter()
                .try_fold(MERGE_BATCH_ROWS, |rows, source_rows| {
                    rows.checked_add((*source_rows).min(MERGE_BATCH_ROWS))
                        .ok_or_else(|| {
                            ErrorCode::MemoryExceedsLimit(
                                "MergeBlocks resident batch row count overflowed usize",
                            )
                        })
                })?;
        let batch_bytes = proportional_working_bytes(
            "MergeBlocks",
            self.task.total_bytes,
            total_rows,
            resident_rows,
        )?;
        let key_phase_bytes = checked_retained_sum("MergeBlocks key phase", &[
            key_reader_bytes,
            writer_bytes,
            batch_bytes,
        ])?;
        let payload_phase_bytes = checked_retained_sum("MergeBlocks payload phase", &[
            payload_reader_bytes,
            writer_bytes,
            batch_bytes,
        ])?;
        let retained_bytes = checked_retained_sum("MergeBlocks", &[
            mapping_bytes,
            key_phase_bytes.max(payload_phase_bytes),
        ])?;
        ensure_memory_budget("MergeBlocks", retained_bytes, self.task.memory_budget)?;

        let mut row_sources = Vec::new();
        row_sources.try_reserve_exact(total_rows).map_err(|error| {
            ErrorCode::MemoryExceedsLimit(format!(
                "cannot allocate MergeBlocks row-source mapping for {total_rows} rows: {error}"
            ))
        })?;

        let mut key_writers = Vec::with_capacity(output_ranges.len());
        for range in &output_ranges {
            let write_options = block_options.create_low_level_options(
                self.table.get_operator(),
                key_types.to_vec(),
                self.table.cluster_key_id().unwrap(),
                self.task.level + 1,
                range.len(),
            )?;
            key_writers.push(FuseLowLevelBlockWriter::create(write_options)?.write_cluster_keys()?);
        }

        let mut key_inputs = Vec::with_capacity(sources.len());
        for index in 0..sources.len() {
            let block_reader = self.create_block_reader(
                schema.clone(),
                sources[index].clone(),
                Some((cluster_key_exprs, cluster_key_func_ctx)),
            )?;
            let key_reader = block_reader.read_cluster_keys()?;
            key_inputs.push((key_reader, source_rows[index]));
        }

        let mut key_stream =
            MergeKeyStream::try_create(key_inputs, key_types.to_vec(), MERGE_BATCH_ROWS)?;
        let mut output_index = 0usize;
        while let Some((key_columns, batch_row_sources)) = key_stream.next_batch()? {
            let mut batch_offset = 0usize;
            while batch_offset < batch_row_sources.len() {
                let global_row = row_sources.len();
                let Some(range) = output_ranges.get(output_index) else {
                    return Err(ErrorCode::Internal(format!(
                        "MergeBlocks key stream emitted more than {total_rows} rows"
                    )));
                };
                let rows = (range.end - global_row).min(batch_row_sources.len() - batch_offset);
                let end = batch_offset + rows;
                let columns = key_columns
                    .iter()
                    .map(|column| column.slice(batch_offset..end))
                    .collect::<Vec<_>>();
                key_writers[output_index].write_columns(&columns)?;
                row_sources.extend_from_slice(&batch_row_sources[batch_offset..end]);
                batch_offset = end;
                if row_sources.len() == range.end {
                    output_index += 1;
                }
            }
        }
        key_stream.finish()?;

        if row_sources.len() != total_rows || output_index != output_ranges.len() {
            return Err(ErrorCode::Internal(format!(
                "MergeBlocks row-source stream has {} rows across {} outputs, expected {total_rows} rows across {} outputs",
                row_sources.len(),
                output_index,
                output_ranges.len(),
            )));
        }

        let mut data_writers = Vec::with_capacity(key_writers.len());
        for key_writer in key_writers {
            data_writers.push(key_writer.finish()?.write_data()?);
        }

        for field_idx in 0..schema.num_fields() {
            // All output writers share this one set of source readers for the
            // current column. Ranges are consumed in order, so every source
            // Parquet column is read exactly once without seeking or reopening.
            let mut readers = Vec::with_capacity(sources.len());
            for source in &sources {
                readers.push(
                    self.create_block_reader(schema.clone(), source.clone(), None)?
                        .read_column(field_idx)?,
                );
            }

            let data_type: DataType = schema.field(field_idx).data_type().into();
            let mut next_data_writers = Vec::with_capacity(data_writers.len());
            for (data_writer, range) in data_writers.into_iter().zip(&output_ranges) {
                let mut column_writer = data_writer.next_column()?;
                for batch in row_sources[range.clone()].chunks(MERGE_BATCH_ROWS) {
                    let output = gather_stream_batch(&mut readers, batch, &data_type)?;
                    column_writer.write(&output)?;
                }
                next_data_writers.push(column_writer.finish()?);
            }
            data_writers = next_data_writers;

            for reader in readers {
                reader.finish()?;
            }
        }

        let mut outputs = Vec::with_capacity(data_writers.len());
        for data_writer in data_writers {
            let output = data_writer.finish()?.finish()?;
            outputs.push(ExtendedBlockMeta {
                block_meta: output.block_meta,
                draft_virtual_block_meta: output.draft_virtual_block_meta,
                column_hlls: output.column_hlls,
                column_top_n: output.column_top_n,
            });
        }
        Ok(outputs)
    }

    fn create_block_reader(
        &self,
        schema: TableSchemaRef,
        source: Arc<BlockMeta>,
        cluster_keys: Option<(&[Expr<usize>], &FunctionContext)>,
    ) -> Result<FuseLowLevelBlockReader> {
        let mut defaults = Vec::with_capacity(schema.num_fields());
        let mut binder = DefaultExprBinder::try_new(self.ctx.clone())?;

        for field in schema.fields() {
            let default = match is_stream_column_id(field.column_id()) {
                true => Scalar::Null,
                false => binder.get_scalar(field)?,
            };

            defaults.push(default);
        }

        let mut options =
            FuseLowLevelBlockReadOptions::new(self.table.get_operator(), schema, source)
                .with_default_values(defaults)
                .with_stream_table_version(self.table.get_table_info().ident.seq);
        if let Some((exprs, func_ctx)) = cluster_keys {
            options = options.with_cluster_keys(exprs.to_vec(), func_ctx.clone());
        }
        FuseLowLevelBlockReader::create(options)
    }
}

impl SyncSource for VerticalReclusterSource {
    const NAME: &'static str = "VerticalReclusterSource";

    fn generate(&mut self) -> Result<Option<DataBlock>> {
        if self.finished {
            return Ok(None);
        }

        self.finished = true;
        let block = self.execute()?;
        Ok(Some(block))
    }
}

fn cluster_key_physical_leaf_count(
    fields: &BTreeSet<usize>,
    schema: &TableSchemaRef,
    block_meta: &BlockMeta,
) -> usize {
    fields
        .iter()
        .flat_map(|&field_index| schema.field(field_index).leaf_column_ids())
        .filter(|column_id| block_meta.col_metas.contains_key(column_id))
        .count()
}

fn output_rows_by_size(
    source_rows: usize,
    source_bytes: usize,
    max_output_rows: usize,
    max_output_bytes: usize,
) -> usize {
    let source_rows = source_rows.max(1);
    let rows_by_bytes = if source_bytes == 0 {
        source_rows
    } else {
        let rows = (max_output_bytes as u128 * source_rows as u128) / source_bytes as u128;
        rows.min(usize::MAX as u128) as usize
    };

    source_rows
        .min(max_output_rows.max(1))
        .min(rows_by_bytes.max(1))
}

fn row_source_mapping_bytes(rows: usize) -> Result<usize> {
    rows.checked_mul(std::mem::size_of::<u32>())
        .and_then(|bytes| bytes.checked_add(MAPPING_SAFETY_BYTES))
        .ok_or_else(|| ErrorCode::Internal("MergeBlocks row-source mapping size overflowed usize"))
}

fn sort_batch_working_bytes(
    source_bytes: usize,
    source_rows: usize,
    output_rows: usize,
) -> Result<usize> {
    let resident_rows = output_rows.checked_mul(3).ok_or_else(|| {
        ErrorCode::MemoryExceedsLimit("SortBlocks resident batch row count overflowed usize")
    })?;
    proportional_working_bytes("SortBlocks", source_bytes, source_rows, resident_rows)
}

fn proportional_working_bytes(
    operation: &str,
    source_bytes: usize,
    source_rows: usize,
    resident_rows: usize,
) -> Result<usize> {
    if source_bytes == 0 || source_rows == 0 || resident_rows == 0 {
        return Ok(0);
    }
    let bytes = (source_bytes as u128)
        .checked_mul(resident_rows as u128)
        .and_then(|bytes| bytes.checked_add(source_rows as u128 - 1))
        .map(|bytes| bytes / source_rows as u128)
        .filter(|bytes| *bytes <= usize::MAX as u128)
        .ok_or_else(|| {
            ErrorCode::MemoryExceedsLimit(format!(
                "{} resident batch size overflowed usize",
                operation
            ))
        })?;
    Ok(bytes as usize)
}

fn checked_retained_sum(operation: &str, components: &[usize]) -> Result<usize> {
    components.iter().try_fold(0usize, |total, component| {
        total.checked_add(*component).ok_or_else(|| {
            ErrorCode::MemoryExceedsLimit(format!(
                "{} retained-memory estimate overflowed usize",
                operation
            ))
        })
    })
}

fn ensure_memory_budget(operation: &str, retained_bytes: usize, budget: usize) -> Result<()> {
    if budget != 0 && retained_bytes > budget {
        return Err(ErrorCode::MemoryExceedsLimit(format!(
            "{} requires approximately {} retained bytes, budget is {} bytes",
            operation, retained_bytes, budget
        )));
    }
    Ok(())
}

fn ranges(rows: usize, target: usize) -> Vec<Range<usize>> {
    let mut result = Vec::new();
    let mut start = 0;
    while start < rows {
        let end = start.saturating_add(target).min(rows);
        result.push(start..end);
        start = end;
    }
    result
}

fn take_columns(columns: &[Column], permutation: &[u32]) -> Result<Vec<Column>> {
    let mut entries = Vec::with_capacity(columns.len());
    for column in columns {
        entries.push(BlockEntry::from(column.clone()));
    }
    let rows = match columns.first() {
        Some(column) => column.len(),
        None => 0,
    };
    let block = DataBlock::new(entries, rows);
    let reordered = block.take(permutation)?;
    let entries = reordered.take_columns();
    let mut result = Vec::with_capacity(entries.len());
    for entry in entries {
        result.push(entry.to_column());
    }
    Ok(result)
}

struct MergeKeyCursor {
    source_index: u32,
    reader: FuseLowLevelClusterKeyReader,
    key_types: Arc<[DataType]>,
    batch_rows: usize,
    expected_rows: usize,
    consumed_rows: usize,
    key_columns: Vec<Column>,
    position: usize,
}

impl MergeKeyCursor {
    fn try_create(
        source_index: u32,
        mut reader: FuseLowLevelClusterKeyReader,
        expected_rows: usize,
        key_types: Arc<[DataType]>,
        batch_rows: usize,
    ) -> Result<Self> {
        let rows = expected_rows.min(batch_rows);
        let key_columns = if rows == 0 {
            Vec::new()
        } else {
            let (keys, _) = reader.read_rows(rows)?;
            keys
        };
        validate_key_batch(&key_columns, rows, &key_types)?;

        Ok(Self {
            source_index,
            reader,
            key_types,
            batch_rows,
            expected_rows,
            consumed_rows: 0,
            key_columns,
            position: 0,
        })
    }

    fn visible_rows(&self) -> usize {
        let rows = self.key_columns.first().map_or(0, Column::len);
        rows.checked_sub(self.position)
            .expect("merge-key cursor position exceeds its loaded rows")
    }

    fn remaining_rows(&self, offset: usize) -> usize {
        self.visible_rows()
            .checked_sub(offset)
            .expect("merge-key virtual offset exceeds visible rows")
    }

    fn compare(&self, offset: usize, other: &Self, other_offset: usize) -> Ordering {
        debug_assert!(self.remaining_rows(offset) > 0);
        debug_assert!(other.remaining_rows(other_offset) > 0);

        let row = self.position + offset;
        let other_row = other.position + other_offset;
        compare_key_rows(
            &self.key_columns,
            row,
            self.source_index,
            &other.key_columns,
            other_row,
            other.source_index,
        )
    }

    fn consume_rows(&mut self, row_count: usize, output: &mut [ColumnBuilder]) -> Result<()> {
        let visible_rows = self.remaining_rows(0);
        if row_count == 0 || row_count > visible_rows {
            return Err(ErrorCode::Internal(format!(
                "MergeBlocks source {} cannot consume {row_count} of {visible_rows} visible key rows",
                self.source_index
            )));
        }
        if output.len() != self.key_columns.len() {
            return Err(ErrorCode::Internal(format!(
                "MergeBlocks key output has {} columns, expected {}",
                output.len(),
                self.key_columns.len()
            )));
        }

        let start = self.position;
        let end = start + row_count;
        for (index, builder) in output.iter_mut().enumerate() {
            let column = self.key_columns[index].slice(start..end);
            builder.append_column(&column);
        }

        self.position = end;
        self.consumed_rows += row_count;
        if self.visible_rows() == 0 && self.consumed_rows < self.expected_rows {
            self.load_next_window()?;
        }
        Ok(())
    }

    fn load_next_window(&mut self) -> Result<()> {
        let remaining = self.expected_rows - self.consumed_rows;
        let rows = remaining.min(self.batch_rows);
        if rows == 0 {
            return Err(ErrorCode::Internal(
                "MergeBlocks attempted to load an empty key window",
            ));
        }

        let (keys, _) = self.reader.read_rows(rows)?;
        self.key_columns = keys;
        validate_key_batch(&self.key_columns, rows, &self.key_types)?;
        self.position = 0;
        Ok(())
    }

    fn finish(self) -> Result<()> {
        if self.consumed_rows != self.expected_rows || self.remaining_rows(0) != 0 {
            return Err(ErrorCode::Internal(format!(
                "MergeBlocks source {} consumed {} of {} key rows",
                self.source_index, self.consumed_rows, self.expected_rows
            )));
        }
        self.reader.finish()
    }
}

fn validate_key_batch(keys: &[Column], rows: usize, key_types: &[DataType]) -> Result<()> {
    if key_types.is_empty() {
        return Err(ErrorCode::Internal(
            "MergeBlocks requires at least one cluster key",
        ));
    }
    if rows == 0 {
        if !keys.is_empty() {
            return Err(ErrorCode::Internal(
                "MergeBlocks empty key batch contains columns",
            ));
        }
        return Ok(());
    }
    if keys.len() != key_types.len() {
        return Err(ErrorCode::Internal(format!(
            "MergeBlocks evaluated {} keys, expected {}",
            keys.len(),
            key_types.len()
        )));
    }
    for field in 0..keys.len() {
        let column = &keys[field];
        let expected = &key_types[field];
        if column.len() != rows || column.data_type() != *expected {
            return Err(ErrorCode::Internal(format!(
                "MergeBlocks key {field} has type {:?} and {} rows, expected {expected:?} and {rows} rows",
                column.data_type(),
                column.len()
            )));
        }
    }
    Ok(())
}

#[derive(Clone, Copy, Debug)]
struct SourceRows {
    source_index: u32,
    row_count: usize,
}

struct MergeKeyQueue {
    cursors: Vec<MergeKeyCursor>,
    heap: Vec<u32>,
    offsets: Vec<usize>,
    pending_source_rows: Vec<SourceRows>,
    blocked_source: Option<u32>,
}

impl MergeKeyQueue {
    fn try_create(
        inputs: Vec<(FuseLowLevelClusterKeyReader, usize)>,
        key_types: Arc<[DataType]>,
        batch_rows: usize,
    ) -> Result<Self> {
        if inputs.len() > u32::MAX as usize {
            return Err(ErrorCode::Internal(format!(
                "MergeBlocks has {} key sources, exceeding source index capacity",
                inputs.len()
            )));
        }

        let mut cursors = Vec::with_capacity(inputs.len());
        for (source_index, (reader, expected_rows)) in inputs.into_iter().enumerate() {
            let cursor = MergeKeyCursor::try_create(
                source_index as u32,
                reader,
                expected_rows,
                key_types.clone(),
                batch_rows,
            )?;
            cursors.push(cursor);
        }

        let mut heap = Vec::with_capacity(cursors.len());
        for (source_index, cursor) in cursors.iter().enumerate() {
            if cursor.remaining_rows(0) > 0 {
                heap.push(source_index as u32);
            }
        }
        let offsets = vec![0; cursors.len()];
        for root in (0..heap.len() / 2).rev() {
            sift_down_sources(&mut heap, root, &cursors, &offsets);
        }

        Ok(Self {
            cursors,
            heap,
            offsets,
            pending_source_rows: Vec::new(),
            blocked_source: None,
        })
    }

    fn next_source_rows(&mut self, max_rows: usize) -> Result<Option<&[SourceRows]>> {
        if !self.pending_source_rows.is_empty() {
            return Err(ErrorCode::Internal(
                "MergeBlocks pending source rows must be committed before selecting more",
            ));
        }
        if max_rows == 0 {
            return Err(ErrorCode::Internal(
                "MergeBlocks cannot select zero key rows",
            ));
        }
        if self.heap.is_empty() {
            return Ok(None);
        }

        let mut selected_rows = 0;
        while selected_rows < max_rows {
            let source = self.heap[0];
            let source_index = source as usize;
            let offset = self.offsets[source_index];
            let remaining = self.cursors[source_index].remaining_rows(offset);
            debug_assert!(remaining > 0);

            let winner_rows = match self.runner_up() {
                None => remaining,
                Some(runner) => winner_prefix_len(&self.cursors, &self.offsets, source, runner),
            };
            let output_remaining = max_rows - selected_rows;
            let row_count = winner_rows.min(output_remaining);
            debug_assert!(row_count > 0);
            if let Some(previous) = self.pending_source_rows.last() {
                debug_assert_ne!(previous.source_index, source);
            }

            self.pending_source_rows.push(SourceRows {
                source_index: source,
                row_count,
            });
            self.offsets[source_index] += row_count;
            selected_rows += row_count;

            if self.cursors[source_index].remaining_rows(self.offsets[source_index]) == 0 {
                self.blocked_source = Some(source);
                break;
            }

            sift_down_sources(&mut self.heap, 0, &self.cursors, &self.offsets);
            if selected_rows == max_rows {
                break;
            }
        }

        Ok(Some(&self.pending_source_rows))
    }

    fn commit_pending(&mut self, output: &mut [ColumnBuilder]) -> Result<()> {
        if self.pending_source_rows.is_empty() {
            return Err(ErrorCode::Internal(
                "MergeBlocks has no pending source rows to commit",
            ));
        }

        for index in 0..self.pending_source_rows.len() {
            let source_rows = self.pending_source_rows[index];
            let source_index = source_rows.source_index as usize;
            self.cursors[source_index].consume_rows(source_rows.row_count, output)?;
        }
        for index in 0..self.pending_source_rows.len() {
            let source_index = self.pending_source_rows[index].source_index as usize;
            self.offsets[source_index] = 0;
        }
        self.pending_source_rows.clear();

        if let Some(source) = self.blocked_source.take() {
            debug_assert_eq!(self.heap.first().copied(), Some(source));
            let source_index = source as usize;
            if self.cursors[source_index].remaining_rows(0) == 0 {
                self.heap.swap_remove(0);
            }
            if !self.heap.is_empty() {
                sift_down_sources(&mut self.heap, 0, &self.cursors, &self.offsets);
            }
        }
        Ok(())
    }

    fn runner_up(&self) -> Option<u32> {
        match self.heap.len() {
            0 | 1 => None,
            2 => Some(self.heap[1]),
            _ => {
                let left = self.heap[1];
                let right = self.heap[2];
                let order = compare_sources(&self.cursors, &self.offsets, left, right);
                if order == Ordering::Less {
                    Some(left)
                } else {
                    Some(right)
                }
            }
        }
    }

    fn finish(self) -> Result<()> {
        if !self.pending_source_rows.is_empty() || self.blocked_source.is_some() {
            return Err(ErrorCode::Internal(
                "MergeBlocks key queue finished with pending source rows",
            ));
        }
        if !self.heap.is_empty() {
            return Err(ErrorCode::Internal(
                "MergeBlocks key queue finished before all sources were consumed",
            ));
        }
        for offset in &self.offsets {
            if *offset != 0 {
                return Err(ErrorCode::Internal(
                    "MergeBlocks key queue finished with a non-zero virtual offset",
                ));
            }
        }
        for cursor in self.cursors {
            cursor.finish()?;
        }
        Ok(())
    }
}

struct MergeKeyStream {
    queue: MergeKeyQueue,
    key_types: Arc<[DataType]>,
    batch_rows: usize,
    expected_rows: usize,
    emitted_rows: usize,
}

impl MergeKeyStream {
    fn try_create(
        inputs: Vec<(FuseLowLevelClusterKeyReader, usize)>,
        key_types: Vec<DataType>,
        batch_rows: usize,
    ) -> Result<Self> {
        if batch_rows == 0 {
            return Err(ErrorCode::Internal(
                "MergeBlocks key batch size must be positive",
            ));
        }
        if key_types.is_empty() {
            return Err(ErrorCode::Internal(
                "MergeBlocks requires at least one cluster key",
            ));
        }

        let mut expected_rows = 0usize;
        for (_, source_rows) in &inputs {
            let Some(total_rows) = expected_rows.checked_add(*source_rows) else {
                return Err(ErrorCode::Internal(
                    "MergeBlocks total key row count overflowed usize",
                ));
            };
            expected_rows = total_rows;
        }

        let key_types: Arc<[DataType]> = key_types.into();
        let queue = MergeKeyQueue::try_create(inputs, key_types.clone(), batch_rows)?;
        Ok(Self {
            queue,
            key_types,
            batch_rows,
            expected_rows,
            emitted_rows: 0,
        })
    }

    fn next_batch(&mut self) -> Result<Option<(Vec<Column>, Vec<u32>)>> {
        let mut key_builders = Vec::with_capacity(self.key_types.len());
        for data_type in self.key_types.iter() {
            key_builders.push(ColumnBuilder::with_capacity(data_type, self.batch_rows));
        }
        let mut row_sources = Vec::with_capacity(self.batch_rows);

        while row_sources.len() < self.batch_rows {
            let remaining = self.batch_rows - row_sources.len();
            {
                let selected = self.queue.next_source_rows(remaining)?;
                let Some(selected) = selected else {
                    break;
                };
                for source_rows in selected {
                    row_sources.extend(std::iter::repeat_n(
                        source_rows.source_index,
                        source_rows.row_count,
                    ));
                }
            }
            self.queue.commit_pending(&mut key_builders)?;
        }

        if row_sources.is_empty() {
            return Ok(None);
        }

        let mut key_columns = Vec::with_capacity(key_builders.len());
        for builder in key_builders {
            key_columns.push(builder.build());
        }
        validate_key_batch(&key_columns, row_sources.len(), &self.key_types)?;

        let Some(emitted_rows) = self.emitted_rows.checked_add(row_sources.len()) else {
            return Err(ErrorCode::Internal(
                "MergeBlocks emitted row count overflowed usize",
            ));
        };
        self.emitted_rows = emitted_rows;
        Ok(Some((key_columns, row_sources)))
    }

    fn finish(self) -> Result<()> {
        if self.emitted_rows != self.expected_rows {
            return Err(ErrorCode::Internal(format!(
                "MergeBlocks key stream emitted {} of {} rows",
                self.emitted_rows, self.expected_rows
            )));
        }
        self.queue.finish()
    }
}

fn sift_down_sources(
    sources: &mut [u32],
    mut root: usize,
    cursors: &[MergeKeyCursor],
    offsets: &[usize],
) {
    loop {
        let left = root * 2 + 1;
        if left >= sources.len() {
            break;
        }
        let right = left + 1;
        let child = if right < sources.len()
            && compare_sources(cursors, offsets, sources[right], sources[left]) == Ordering::Less
        {
            right
        } else {
            left
        };
        if compare_sources(cursors, offsets, sources[child], sources[root]) != Ordering::Less {
            break;
        }
        sources.swap(root, child);
        root = child;
    }
}

/// Return the winner rows preceding the runner's current row.
fn winner_prefix_len(
    cursors: &[MergeKeyCursor],
    offsets: &[usize],
    winner: u32,
    runner: u32,
) -> usize {
    let winner_index = winner as usize;
    let winner_offset = offsets[winner_index];
    let remaining = cursors[winner_index].remaining_rows(winner_offset);

    debug_assert!(remaining > 0);
    debug_assert!(winner_row_precedes_runner(
        cursors, offsets, winner, runner, 0
    ));
    if winner_row_precedes_runner(cursors, offsets, winner, runner, remaining - 1) {
        return remaining;
    }

    let mut batch_size = 1;
    if batch_size >= remaining
        || !winner_row_precedes_runner(cursors, offsets, winner, runner, batch_size)
    {
        return batch_size;
    }
    batch_size += 1;

    let mut detected = 0;
    while detected < 16
        && batch_size < remaining
        && winner_row_precedes_runner(cursors, offsets, winner, runner, batch_size)
    {
        batch_size += 1;
        detected += 1;
    }
    if detected < 16 {
        return batch_size;
    }

    let mut low = batch_size;
    let mut high = remaining;
    while low < high {
        let middle = low + (high - low) / 2;
        if winner_row_precedes_runner(cursors, offsets, winner, runner, middle) {
            low = middle + 1;
        } else {
            high = middle;
        }
    }
    low
}

fn winner_row_precedes_runner(
    cursors: &[MergeKeyCursor],
    offsets: &[usize],
    winner: u32,
    runner: u32,
    winner_row: usize,
) -> bool {
    let winner_index = winner as usize;
    let runner_index = runner as usize;
    let winner_offset = offsets[winner_index] + winner_row;
    let runner_offset = offsets[runner_index];
    let order = cursors[winner_index].compare(winner_offset, &cursors[runner_index], runner_offset);
    order != Ordering::Greater
}

fn compare_sources(
    cursors: &[MergeKeyCursor],
    offsets: &[usize],
    left: u32,
    right: u32,
) -> Ordering {
    let left_index = left as usize;
    let right_index = right as usize;
    cursors[left_index].compare(
        offsets[left_index],
        &cursors[right_index],
        offsets[right_index],
    )
}

fn gather_stream_batch(
    readers: &mut [FuseLowLevelColumnBatchReader],
    row_sources: &[u32],
    data_type: &DataType,
) -> Result<Column> {
    let Some(&first_source) = row_sources.first() else {
        return Ok(ColumnBuilder::with_capacity(data_type, 0).build());
    };

    if row_sources.iter().all(|source| *source == first_source) {
        return readers[first_source as usize].read_rows(row_sources.len());
    }

    let mut start = 0;
    let mut builder = ColumnBuilder::with_capacity(data_type, row_sources.len());

    while start < row_sources.len() {
        let source = row_sources[start];
        let mut end = start + 1;
        while end < row_sources.len() && row_sources[end] == source {
            end += 1;
        }
        let column = readers[source as usize].read_rows(end - start)?;
        builder.append_column(&column);
        start = end;
    }
    Ok(builder.build())
}

fn compare_key_rows(
    left: &[Column],
    left_row: usize,
    left_source: u32,
    right: &[Column],
    right_row: usize,
    right_source: u32,
) -> Ordering {
    for (left, right) in left.iter().zip(right) {
        let order = compare_key_column_rows(left, left_row, right, right_row);
        if order != Ordering::Equal {
            return order;
        }
    }
    left_source.cmp(&right_source)
}

fn compare_key_column_rows(
    left: &Column,
    left_row: usize,
    right: &Column,
    right_row: usize,
) -> Ordering {
    use databend_common_expression::types::decimal::DecimalColumn;
    use databend_common_expression::types::number::NumberColumn;

    macro_rules! compare_buffers {
        ($left:expr, $left_row:expr, $right:expr, $right_row:expr) => {{
            unsafe {
                $left
                    .get_unchecked($left_row)
                    .cmp($right.get_unchecked($right_row))
            }
        }};
    }

    match (left, right) {
        (Column::Number(left), Column::Number(right)) => match (left, right) {
            (NumberColumn::UInt8(left), NumberColumn::UInt8(right)) => {
                compare_buffers!(left, left_row, right, right_row)
            }
            (NumberColumn::UInt16(left), NumberColumn::UInt16(right)) => {
                compare_buffers!(left, left_row, right, right_row)
            }
            (NumberColumn::UInt32(left), NumberColumn::UInt32(right)) => {
                compare_buffers!(left, left_row, right, right_row)
            }
            (NumberColumn::UInt64(left), NumberColumn::UInt64(right)) => {
                compare_buffers!(left, left_row, right, right_row)
            }
            (NumberColumn::Int8(left), NumberColumn::Int8(right)) => {
                compare_buffers!(left, left_row, right, right_row)
            }
            (NumberColumn::Int16(left), NumberColumn::Int16(right)) => {
                compare_buffers!(left, left_row, right, right_row)
            }
            (NumberColumn::Int32(left), NumberColumn::Int32(right)) => {
                compare_buffers!(left, left_row, right, right_row)
            }
            (NumberColumn::Int64(left), NumberColumn::Int64(right)) => {
                compare_buffers!(left, left_row, right, right_row)
            }
            (NumberColumn::Float32(left), NumberColumn::Float32(right)) => {
                compare_buffers!(left, left_row, right, right_row)
            }
            (NumberColumn::Float64(left), NumberColumn::Float64(right)) => {
                compare_buffers!(left, left_row, right, right_row)
            }
            _ => unreachable!("validated MergeBlocks number types differ"),
        },
        (Column::Decimal(left), Column::Decimal(right)) => match (left, right) {
            (DecimalColumn::Decimal64(left, _), DecimalColumn::Decimal64(right, _)) => {
                compare_buffers!(left, left_row, right, right_row)
            }
            (DecimalColumn::Decimal128(left, _), DecimalColumn::Decimal128(right, _)) => {
                compare_buffers!(left, left_row, right, right_row)
            }
            (DecimalColumn::Decimal256(left, _), DecimalColumn::Decimal256(right, _)) => {
                compare_buffers!(left, left_row, right, right_row)
            }
            _ => unreachable!("validated MergeBlocks decimal types differ"),
        },
        (Column::Boolean(left), Column::Boolean(right)) => {
            left.get_bit(left_row).cmp(&right.get_bit(right_row))
        }
        (Column::String(left), Column::String(right)) => {
            databend_common_expression::types::StringColumn::compare(
                left, left_row, right, right_row,
            )
        }
        (Column::Timestamp(left), Column::Timestamp(right)) => {
            compare_buffers!(left, left_row, right, right_row)
        }
        (Column::Date(left), Column::Date(right)) => {
            compare_buffers!(left, left_row, right, right_row)
        }
        (Column::Nullable(left), Column::Nullable(right)) => match (
            left.validity.get_bit(left_row),
            right.validity.get_bit(right_row),
        ) {
            (true, true) => {
                compare_key_column_rows(&left.column, left_row, &right.column, right_row)
            }
            (true, false) => Ordering::Less,
            (false, true) => Ordering::Greater,
            (false, false) => Ordering::Equal,
        },
        // Reuse the existing borrowed scalar ordering for less common scalar
        // cluster-key types. This keeps the merge allocation-free without
        // changing SortCompare or introducing another public comparator API.
        _ => unsafe {
            left.index_unchecked(left_row)
                .cmp(&right.index_unchecked(right_row))
        },
    }
}

#[cfg(test)]
mod tests {
    use databend_common_expression::ColumnRef;
    use databend_common_expression::FromData;
    use databend_common_expression::TableDataType;
    use databend_common_expression::TableField;
    use databend_common_expression::TableSchema;
    use databend_common_expression::types::BinaryType;
    use databend_common_expression::types::NumberDataType;
    use databend_common_expression::types::nullable::NullableColumn;
    use databend_common_expression::types::number::Float64Type;
    use databend_common_expression::types::number::Int32Type;
    use databend_common_expression::types::string::StringType;
    use databend_storages_common_blocks::build_parquet_writer_properties;
    use databend_storages_common_table_meta::meta::StatisticsOfColumns;
    use databend_storages_common_table_meta::table::TableCompression;
    use opendal::Operator;
    use opendal::services::Memory;
    use rand::Rng;
    use rand::SeedableRng;
    use rand::rngs::StdRng;

    use super::*;
    use crate::io::FuseLowLevelBlockWriteOptions;
    use crate::io::WriteSettings;

    #[test]
    fn test_output_rows_by_size_respects_row_and_byte_limits() {
        assert_eq!(output_rows_by_size(1_000, 1_000_000, 800, 2_000_000), 800);
        assert_eq!(output_rows_by_size(1_000, 4_000_000, 2_000, 1_000_000), 250);
        assert_eq!(output_rows_by_size(1_000, 500_000, 2_000, 1_000_000), 1_000);
    }

    #[test]
    fn test_output_rows_by_size_handles_metadata_boundaries() {
        assert_eq!(output_rows_by_size(100, 0, 80, 1_000), 80);
        assert_eq!(output_rows_by_size(100, 10_000, 100, 1), 1);
        assert_eq!(output_rows_by_size(100, 10_000, 0, 0), 1);
    }

    #[test]
    fn test_output_rows_by_size_uses_overflow_safe_arithmetic() {
        assert_eq!(
            output_rows_by_size(usize::MAX, usize::MAX, usize::MAX, usize::MAX),
            usize::MAX
        );
    }

    #[test]
    fn test_merge_output_ranges_follow_estimated_row_size() {
        let output_rows = output_rows_by_size(1_000, 4_000_000, 1_000, 1_000_000);
        assert_eq!(output_rows, 250);
        assert_eq!(ranges(1_000, output_rows), vec![
            0..250,
            250..500,
            500..750,
            750..1_000
        ]);
    }

    #[test]
    fn test_row_source_mapping_bytes_checks_budget_math() {
        assert_eq!(
            row_source_mapping_bytes(10).unwrap(),
            MAPPING_SAFETY_BYTES + 10 * std::mem::size_of::<u32>()
        );
        assert!(row_source_mapping_bytes(usize::MAX).is_err());
    }

    #[test]
    fn test_retained_memory_admission_math() {
        assert_eq!(
            proportional_working_bytes("test", 1_000, 100, 3).unwrap(),
            30
        );
        assert_eq!(sort_batch_working_bytes(1_000, 100, 10).unwrap(), 300);
        assert_eq!(checked_retained_sum("test", &[10, 20, 30]).unwrap(), 60);
        assert!(checked_retained_sum("test", &[usize::MAX, 1]).is_err());
        assert!(sort_batch_working_bytes(1, 1, usize::MAX).is_err());

        ensure_memory_budget("test", 60, 0).unwrap();
        ensure_memory_budget("test", 60, 60).unwrap();
        assert!(ensure_memory_budget("test", 60, 59).is_err());
    }

    fn write_key_source(
        operator: Operator,
        schema: TableSchemaRef,
        path: &str,
        column: &Column,
    ) -> Arc<BlockMeta> {
        let compression = TableCompression::Zstd;
        let properties = Arc::new(build_parquet_writer_properties(
            compression,
            true,
            None::<&StatisticsOfColumns>,
            None,
            column.len(),
            schema.as_ref(),
            Some(1024),
            Some(64 * 1024),
        ));
        let mut options = FuseLowLevelBlockWriteOptions::new(
            FunctionContext::default(),
            operator,
            schema.clone(),
            WriteSettings {
                table_compression: compression,
                ..Default::default()
            },
            properties,
            (path.to_string(), DataBlock::VERSION),
        );
        options.set_statistics(
            schema
                .leaf_fields()
                .iter()
                .map(|field| (field.column_id(), DataType::from(field.data_type())))
                .collect(),
            Vec::new(),
            false,
        );

        let writer = FuseLowLevelBlockWriter::create(options).unwrap();
        let mut data = writer.write_data().unwrap();
        let mut output = data.next_column().unwrap();
        output.write(column).unwrap();
        data = output.finish().unwrap();
        Arc::new(data.finish().unwrap().finish().unwrap().block_meta)
    }

    fn key_reader(
        operator: Operator,
        schema: TableSchemaRef,
        meta: Arc<BlockMeta>,
    ) -> FuseLowLevelClusterKeyReader {
        let key_expr = Expr::ColumnRef(ColumnRef {
            span: None,
            id: 0,
            data_type: DataType::Number(NumberDataType::Int32),
            display_name: "key".to_string(),
        });
        let options = FuseLowLevelBlockReadOptions::new(operator, schema, meta)
            .with_cluster_keys(vec![key_expr], FunctionContext::default());
        let block_reader = FuseLowLevelBlockReader::create(options).unwrap();
        block_reader.read_cluster_keys().unwrap()
    }

    fn merge_test_batches(keys: &[Vec<Column>]) -> Vec<u32> {
        let mut positions = vec![0usize; keys.len()];
        let mut source_rows = Vec::with_capacity(keys.len());
        let mut total_rows = 0usize;
        for columns in keys {
            let rows = columns.first().map_or(0, Column::len);
            source_rows.push(rows);
            total_rows += rows;
        }

        let mut row_sources = Vec::with_capacity(total_rows);
        while row_sources.len() < total_rows {
            let mut winner: Option<usize> = None;
            for source in 0..keys.len() {
                if positions[source] == source_rows[source] {
                    continue;
                }
                if let Some(current) = winner {
                    let order = compare_key_rows(
                        &keys[source],
                        positions[source],
                        source as u32,
                        &keys[current],
                        positions[current],
                        current as u32,
                    );
                    if order == Ordering::Less {
                        winner = Some(source);
                    }
                } else {
                    winner = Some(source);
                }
            }

            let source = winner.expect("at least one merge source has rows");
            row_sources.push(source as u32);
            positions[source] += 1;
        }
        row_sources
    }

    fn replay_columns(sources: &[Vec<Column>], row_sources: &[u32]) -> Vec<Column> {
        let mut positions = vec![0usize; sources.len()];
        (0..sources[0].len())
            .map(|field| {
                let data_type = sources[0][field].data_type();
                let mut builder = ColumnBuilder::with_capacity(&data_type, row_sources.len());
                for &source in row_sources {
                    let source = source as usize;
                    builder
                        .push(unsafe { sources[source][field].index_unchecked(positions[source]) });
                    positions[source] += 1;
                }
                positions.fill(0);
                builder.build()
            })
            .collect()
    }

    fn assert_matches_horizontal(sources: &[Vec<Column>]) {
        let row_sources = merge_test_batches(sources);
        let total_rows = row_sources.len();
        let vertical = replay_columns(sources, &row_sources);
        let all_columns = (0..sources[0].len())
            .map(|field| {
                Column::concat_columns(sources.iter().map(|columns| columns[field].clone()))
                    .unwrap()
            })
            .collect::<Vec<_>>();
        let order = compare_columns(all_columns.clone(), total_rows).unwrap();
        let horizontal = take_columns(&all_columns, &order).unwrap();
        assert_eq!(vertical, horizontal);
    }

    #[test]
    fn test_production_cursor_heap_crosses_key_batches() {
        crate::test_utils::init_test_globals().unwrap();
        let operator = Operator::new(Memory::default()).unwrap().finish();
        let schema = Arc::new(TableSchema::new(vec![TableField::new(
            "key",
            TableDataType::Number(NumberDataType::Int32),
        )]));
        let source0 = Int32Type::from_data(
            (0..MERGE_BATCH_ROWS + 37)
                .map(|value| value as i32 * 2)
                .collect::<Vec<_>>(),
        );
        let source1 = Int32Type::from_data(
            (0..MERGE_BATCH_ROWS + 41)
                .map(|value| value as i32 * 2 + 1)
                .collect::<Vec<_>>(),
        );
        let meta0 = write_key_source(
            operator.clone(),
            schema.clone(),
            "vertical-cursor-0.parquet",
            &source0,
        );
        let meta1 = write_key_source(
            operator.clone(),
            schema.clone(),
            "vertical-cursor-1.parquet",
            &source1,
        );
        let reader0 = key_reader(operator.clone(), schema.clone(), meta0.clone());
        let reader1 = key_reader(operator.clone(), schema.clone(), meta1.clone());
        let inputs = vec![(reader0, source0.len()), (reader1, source1.len())];
        let key_types = vec![DataType::Number(NumberDataType::Int32)];
        let mut stream =
            MergeKeyStream::try_create(inputs, key_types.clone(), MERGE_BATCH_ROWS).unwrap();

        let mut row_source_batches = Vec::new();
        let mut key_batches = Vec::new();
        while let Some((key_columns, row_sources)) = stream.next_batch().unwrap() {
            key_batches.push(key_columns[0].clone());
            row_source_batches.push(row_sources);
        }
        stream.finish().unwrap();

        assert_eq!(row_source_batches.len(), 3);
        assert_eq!(row_source_batches[0].len(), MERGE_BATCH_ROWS);
        assert_eq!(row_source_batches[1].len(), MERGE_BATCH_ROWS);
        assert_eq!(row_source_batches[2].len(), 78);

        let expected_sources = merge_test_batches(&[vec![source0.clone()], vec![source1.clone()]]);
        let mut expected_start = 0;
        for row_sources in &row_source_batches {
            let expected_end = expected_start + row_sources.len();
            assert_eq!(row_sources, &expected_sources[expected_start..expected_end]);
            expected_start = expected_end;
        }
        assert_eq!(expected_start, expected_sources.len());

        let merged = Column::concat_columns(key_batches.into_iter()).unwrap();

        let mut readers = vec![
            FuseLowLevelBlockReader::create(FuseLowLevelBlockReadOptions::new(
                operator.clone(),
                schema.clone(),
                meta0,
            ))
            .unwrap()
            .read_column(0)
            .unwrap(),
            FuseLowLevelBlockReader::create(FuseLowLevelBlockReadOptions::new(
                operator, schema, meta1,
            ))
            .unwrap()
            .read_column(0)
            .unwrap(),
        ];
        let output_ranges = ranges(expected_sources.len(), MERGE_BATCH_ROWS - 17);
        assert!(output_ranges.len() > 2);
        let mut replayed_outputs = Vec::with_capacity(output_ranges.len());
        for range in &output_ranges {
            let mut output_batches = Vec::new();
            for row_sources in expected_sources[range.clone()].chunks(MERGE_BATCH_ROWS) {
                output_batches
                    .push(gather_stream_batch(&mut readers, row_sources, &key_types[0]).unwrap());
            }
            replayed_outputs.push(Column::concat_columns(output_batches.into_iter()).unwrap());
        }
        for reader in readers {
            reader.finish().unwrap();
        }
        let replayed = Column::concat_columns(replayed_outputs.into_iter()).unwrap();
        assert_eq!(replayed, merged);

        let all = Column::concat_columns([source0, source1].into_iter()).unwrap();
        let order = compare_columns(vec![all.clone()], all.len()).unwrap();
        assert_eq!(merged, take_columns(&[all], &order).unwrap().remove(0));
    }

    #[test]
    fn test_queue_requires_pending_rows_to_be_committed() {
        crate::test_utils::init_test_globals().unwrap();
        let operator = Operator::new(Memory::default()).unwrap().finish();
        let schema = Arc::new(TableSchema::new(vec![TableField::new(
            "key",
            TableDataType::Number(NumberDataType::Int32),
        )]));
        let source = Int32Type::from_data(vec![1, 2, 3]);
        let meta = write_key_source(
            operator.clone(),
            schema.clone(),
            "vertical-queue-pending.parquet",
            &source,
        );
        let reader = key_reader(operator, schema, meta);
        let key_types: Arc<[DataType]> = vec![DataType::Number(NumberDataType::Int32)].into();
        let mut queue = MergeKeyQueue::try_create(
            vec![(reader, source.len())],
            key_types.clone(),
            MERGE_BATCH_ROWS,
        )
        .unwrap();

        {
            let selected = queue.next_source_rows(2).unwrap().unwrap();
            assert_eq!(selected.len(), 1);
            assert_eq!(selected[0].source_index, 0);
            assert_eq!(selected[0].row_count, 2);
        }
        assert!(queue.next_source_rows(1).is_err());

        let data_type = &key_types[0];
        let mut output = vec![ColumnBuilder::with_capacity(data_type, 3)];
        queue.commit_pending(&mut output).unwrap();
        {
            let selected = queue.next_source_rows(1).unwrap().unwrap();
            assert_eq!(selected.len(), 1);
            assert_eq!(selected[0].source_index, 0);
            assert_eq!(selected[0].row_count, 1);
        }
        queue.commit_pending(&mut output).unwrap();
        assert!(queue.next_source_rows(1).unwrap().is_none());
        queue.finish().unwrap();

        let merged = output.pop().unwrap().build();
        assert_eq!(merged, source);
    }

    #[test]
    fn test_stream_removes_finished_source_and_fills_output_batch() {
        crate::test_utils::init_test_globals().unwrap();
        let operator = Operator::new(Memory::default()).unwrap().finish();
        let schema = Arc::new(TableSchema::new(vec![TableField::new(
            "key",
            TableDataType::Number(NumberDataType::Int32),
        )]));
        let short_source = Int32Type::from_data(vec![0]);
        let long_source = Int32Type::from_data(vec![1, 2, 3, 4, 5, 6]);
        let short_meta = write_key_source(
            operator.clone(),
            schema.clone(),
            "vertical-short-source.parquet",
            &short_source,
        );
        let long_meta = write_key_source(
            operator.clone(),
            schema.clone(),
            "vertical-long-source.parquet",
            &long_source,
        );
        let short_reader = key_reader(operator.clone(), schema.clone(), short_meta);
        let long_reader = key_reader(operator, schema, long_meta);
        let inputs = vec![
            (short_reader, short_source.len()),
            (long_reader, long_source.len()),
        ];
        let key_types = vec![DataType::Number(NumberDataType::Int32)];
        let mut stream = MergeKeyStream::try_create(inputs, key_types, 4).unwrap();

        let (first_keys, first_sources) = stream.next_batch().unwrap().unwrap();
        assert_eq!(first_sources, vec![0, 1, 1, 1]);
        assert_eq!(first_keys[0], Int32Type::from_data(vec![0, 1, 2, 3]));

        let (second_keys, second_sources) = stream.next_batch().unwrap().unwrap();
        assert_eq!(second_sources, vec![1, 1, 1]);
        assert_eq!(second_keys[0], Int32Type::from_data(vec![4, 5, 6]));
        assert!(stream.next_batch().unwrap().is_none());
        stream.finish().unwrap();
    }

    #[test]
    fn test_row_sources_record_stream_only() {
        let sources = vec![
            vec![
                Int32Type::from_data(vec![1, 3, 5]),
                StringType::from_data(vec!["a", "a", "b"]),
            ],
            vec![
                Int32Type::from_data(vec![1, 2, 5]),
                StringType::from_data(vec!["b", "z", "a"]),
            ],
        ];
        assert_eq!(merge_test_batches(&sources), vec![0, 1, 1, 0, 1, 0]);
        assert_matches_horizontal(&sources);
    }

    #[test]
    fn test_equal_keys_use_source_order_and_replay_payload_losslessly() {
        let keys = vec![vec![Int32Type::from_data(vec![1, 1, 1])], vec![
            Int32Type::from_data(vec![1, 1]),
        ]];
        let row_sources = merge_test_batches(&keys);
        assert_eq!(row_sources, vec![0, 0, 0, 1, 1]);

        let payload = vec![vec![StringType::from_data(vec!["a", "b", "c"])], vec![
            StringType::from_data(vec!["d", "e"]),
        ]];
        assert_eq!(
            replay_columns(&payload, &row_sources)[0],
            StringType::from_data(vec!["a", "b", "c", "d", "e"])
        );
    }

    #[test]
    fn test_nullable_float_and_binary_order_match_horizontal() {
        let nullable0 = NullableColumn::new_column(
            Int32Type::from_data(vec![1, 0]),
            [true, false].into_iter().collect(),
        );
        let nullable1 = NullableColumn::new_column(
            Int32Type::from_data(vec![2, 0]),
            [true, false].into_iter().collect(),
        );
        assert_matches_horizontal(&[vec![nullable0], vec![nullable1]]);

        assert_matches_horizontal(&[
            vec![
                Float64Type::from_data(vec![f64::NEG_INFINITY, -0.0, 1.0, f64::NAN]),
                Int32Type::from_data(vec![0, 0, 0, 0]),
            ],
            vec![
                Float64Type::from_data(vec![-1.0, 0.0, 2.0, f64::NAN]),
                Int32Type::from_data(vec![1, 1, 1, 1]),
            ],
        ]);

        assert_matches_horizontal(&[
            vec![
                BinaryType::from_data(vec![vec![0], vec![1, 0], vec![2]]),
                Int32Type::from_data(vec![0, 0, 0]),
            ],
            vec![
                BinaryType::from_data(vec![vec![0, 1], vec![1, 1], vec![3]]),
                Int32Type::from_data(vec![1, 1, 1]),
            ],
        ]);
    }

    #[test]
    fn test_batch_merge_crosses_8192_boundary() {
        let source0 = (0..MERGE_BATCH_ROWS + 37)
            .map(|value| value as i32 * 2)
            .collect::<Vec<_>>();
        let source1 = (0..MERGE_BATCH_ROWS + 41)
            .map(|value| value as i32 * 2 + 1)
            .collect::<Vec<_>>();
        let sources = vec![vec![Int32Type::from_data(source0)], vec![
            Int32Type::from_data(source1),
        ]];
        let row_sources = merge_test_batches(&sources);
        assert_eq!(row_sources.len(), MERGE_BATCH_ROWS * 2 + 78);
        assert!(row_sources.windows(2).any(|pair| pair[0] != pair[1]));
        assert_matches_horizontal(&sources);
    }

    #[test]
    fn test_non_overlapping_batches_are_emitted_as_whole_runs() {
        let sources = vec![
            vec![Int32Type::from_data(
                (0..MERGE_BATCH_ROWS * 2 + 3)
                    .map(|value| value as i32)
                    .collect::<Vec<_>>(),
            )],
            vec![Int32Type::from_data(
                (0..MERGE_BATCH_ROWS + 5)
                    .map(|value| 100_000 + value as i32)
                    .collect::<Vec<_>>(),
            )],
        ];
        let row_sources = merge_test_batches(&sources);
        assert_eq!(&row_sources[..MERGE_BATCH_ROWS * 2 + 3], vec![
            0;
            MERGE_BATCH_ROWS
                * 2
                + 3
        ]);
        assert_eq!(&row_sources[MERGE_BATCH_ROWS * 2 + 3..], vec![
            1;
            MERGE_BATCH_ROWS
                + 5
        ]);
    }

    #[test]
    fn test_randomized_stream_replay_matches_horizontal_sort() {
        let mut rng = StdRng::seed_from_u64(0x5eed_cafe_d15c_a11e);
        for _ in 0..256 {
            let source_count = rng.gen_range(1..=8);
            let mut next_id = 0i32;
            let mut sources = Vec::with_capacity(source_count);
            for _ in 0..source_count {
                let rows = rng.gen_range(1..=24);
                let mut nullable = Vec::with_capacity(rows);
                let mut strings = Vec::with_capacity(rows);
                let mut ids = Vec::with_capacity(rows);
                for _ in 0..rows {
                    nullable.push(if rng.gen_ratio(1, 5) {
                        None
                    } else {
                        Some(rng.gen_range(-4..=4))
                    });
                    strings.push(match rng.gen_range(0..4) {
                        0 => "",
                        1 => "a",
                        2 => "aa",
                        _ => "z",
                    });
                    ids.push(next_id);
                    next_id += 1;
                }
                let columns = vec![
                    Int32Type::from_opt_data(nullable),
                    StringType::from_data(strings),
                    Int32Type::from_data(ids),
                ];
                let order = compare_columns(columns.clone(), rows).unwrap();
                sources.push(take_columns(&columns, &order).unwrap());
            }
            assert_matches_horizontal(&sources);
        }
    }
}
