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

use ahash::AHashMap;
use ahash::HashSet;
use common_arrow::arrow::bitmap::MutableBitmap;
use common_base::base::tokio::sync::OwnedSemaphorePermit;
use common_base::base::tokio::sync::Semaphore;
use common_base::base::ProgressValues;
use common_base::runtime::GlobalIORuntime;
use common_base::runtime::TrySpawn;
use common_catalog::plan::Projection;
use common_catalog::table_context::TableContext;
use common_exception::ErrorCode;
use common_exception::Result;
use common_expression::ColumnId;
use common_expression::ComputedExpr;
use common_expression::DataBlock;
use common_expression::FieldIndex;
use common_expression::Scalar;
use common_expression::TableSchema;
use common_sql::evaluator::BlockOperator;
use common_sql::executor::OnConflictField;
use log::info;
use log::warn;
use opendal::Operator;
use storages_common_cache::LoadParams;
use storages_common_index::filters::Filter;
use storages_common_index::filters::Xor8Filter;
use storages_common_index::BloomIndex;
use storages_common_table_meta::meta::BlockMeta;
use storages_common_table_meta::meta::ColumnStatistics;
use storages_common_table_meta::meta::Location;
use storages_common_table_meta::meta::SegmentInfo;

use crate::io::read::bloom::block_filter_reader::BloomBlockFilterReader;
use crate::io::write_data;
use crate::io::BlockBuilder;
use crate::io::BlockReader;
use crate::io::CompactSegmentInfoReader;
use crate::io::MetaReaders;
use crate::io::ReadSettings;
use crate::io::WriteSettings;
use crate::metrics::metrics_inc_replace_accumulated_merge_action_time_ms;
use crate::metrics::metrics_inc_replace_apply_deletion_time_ms;
use crate::metrics::metrics_inc_replace_block_number_after_pruning;
use crate::metrics::metrics_inc_replace_block_number_bloom_pruned;
use crate::metrics::metrics_inc_replace_block_number_totally_loaded;
use crate::metrics::metrics_inc_replace_block_number_write;
use crate::metrics::metrics_inc_replace_block_of_zero_row_deleted;
use crate::metrics::metrics_inc_replace_number_accumulated_merge_action;
use crate::metrics::metrics_inc_replace_number_apply_deletion;
use crate::metrics::metrics_inc_replace_row_number_after_pruning;
use crate::metrics::metrics_inc_replace_row_number_totally_loaded;
use crate::metrics::metrics_inc_replace_row_number_write;
use crate::metrics::metrics_inc_replace_whole_block_deletion;
use crate::operations::common::BlockMetaIndex;
use crate::operations::common::MutationLogEntry;
use crate::operations::common::MutationLogs;
use crate::operations::mutation::BlockIndex;
use crate::operations::mutation::SegmentIndex;
use crate::operations::replace_into::meta::merge_into_operation_meta::DeletionByColumn;
use crate::operations::replace_into::meta::merge_into_operation_meta::MergeIntoOperation;
use crate::operations::replace_into::meta::merge_into_operation_meta::UniqueKeyDigest;
use crate::operations::replace_into::mutator::column_hash::row_hash_of_columns;
use crate::operations::replace_into::mutator::deletion_accumulator::DeletionAccumulator;
struct AggregationContext {
    segment_locations: AHashMap<SegmentIndex, Location>,
    // the fields specified in ON CONFLICT clause
    on_conflict_fields: Vec<OnConflictField>,

    // table fields excludes `on_conflict_fields`
    remain_column_field_ids: Vec<FieldIndex>,
    // reader that reads the ON CONFLICT key fields
    key_column_reader: Arc<BlockReader>,
    // reader that reads the `remain_column_field_ids`
    remain_column_reader: Option<Arc<BlockReader>>,
    data_accessor: Operator,
    write_settings: WriteSettings,
    read_settings: ReadSettings,
    segment_reader: CompactSegmentInfoReader,
    block_builder: BlockBuilder,
    io_request_semaphore: Arc<Semaphore>,
}

// Apply MergeIntoOperations to segments
pub struct MergeIntoOperationAggregator {
    deletion_accumulator: DeletionAccumulator,
    aggregation_ctx: Arc<AggregationContext>,
    // the most significant field index in `on_conflict_fields`
    // which we should apply bloom filtering, if any
    most_significant_on_conflict_field_index: Option<usize>,
}

impl MergeIntoOperationAggregator {
    #[allow(clippy::too_many_arguments)] // TODO fix this
    pub fn try_create(
        ctx: Arc<dyn TableContext>,
        on_conflict_fields: Vec<OnConflictField>,
        most_significant_on_conflict_field_index: Option<usize>,
        segment_locations: Vec<(SegmentIndex, Location)>,
        data_accessor: Operator,
        table_schema: Arc<TableSchema>,
        write_settings: WriteSettings,
        read_settings: ReadSettings,
        block_builder: BlockBuilder,
        io_request_semaphore: Arc<Semaphore>,
    ) -> Result<Self> {
        let deletion_accumulator = DeletionAccumulator::default();
        let segment_reader =
            MetaReaders::segment_info_reader(data_accessor.clone(), table_schema.clone());

        // order matters, later the projection that used by block readers depends on the order
        let key_column_field_indexes: Vec<FieldIndex> =
            on_conflict_fields.iter().map(|i| i.field_index).collect();

        let remain_column_field_ids: Vec<FieldIndex> = {
            let all_field_indexes = table_schema
                .fields()
                .iter()
                .enumerate()
                .filter(|(_, f)| !matches!(f.computed_expr(), Some(ComputedExpr::Virtual(_))))
                .map(|(i, _)| i)
                .collect::<Vec<FieldIndex>>();

            all_field_indexes
                .into_iter()
                .filter(|index| !key_column_field_indexes.contains(index))
                .collect()
        };

        let key_column_reader = {
            let projection = Projection::Columns(key_column_field_indexes);
            BlockReader::create(
                data_accessor.clone(),
                table_schema.clone(),
                projection,
                ctx.clone(),
                false,
            )
        }?;

        let remain_column_reader = {
            if remain_column_field_ids.is_empty() {
                None
            } else {
                let projection = Projection::Columns(remain_column_field_ids.clone());
                let reader = BlockReader::create(
                    data_accessor.clone(),
                    table_schema,
                    projection,
                    ctx.clone(),
                    false,
                )?;
                Some(reader)
            }
        };

        Ok(Self {
            deletion_accumulator,
            aggregation_ctx: Arc::new(AggregationContext {
                segment_locations: AHashMap::from_iter(segment_locations.into_iter()),
                on_conflict_fields,
                remain_column_field_ids,
                key_column_reader,
                remain_column_reader,
                data_accessor,
                write_settings,
                read_settings,
                segment_reader,
                block_builder,
                io_request_semaphore,
            }),
            most_significant_on_conflict_field_index,
        })
    }
}

// aggregate mutations (currently, deletion only)
impl MergeIntoOperationAggregator {
    #[async_backtrace::framed]
    pub async fn accumulate(&mut self, merge_into_operation: MergeIntoOperation) -> Result<()> {
        let aggregation_ctx = &self.aggregation_ctx;
        metrics_inc_replace_number_accumulated_merge_action();

        let start = Instant::now();
        match merge_into_operation {
            MergeIntoOperation::Delete(partitions) => {
                for (segment_index, (path, ver)) in &aggregation_ctx.segment_locations {
                    // segment level
                    let load_param = LoadParams {
                        location: path.clone(),
                        len_hint: None,
                        ver: *ver,
                        put_cache: true,
                    };
                    let compact_segment_info =
                        aggregation_ctx.segment_reader.read(&load_param).await?;
                    let mut segment_info: Option<SegmentInfo> = None;

                    for DeletionByColumn {
                        columns_min_max,
                        key_hashes,
                        bloom_hashes,
                    } in &partitions
                    {
                        if aggregation_ctx
                            .overlapped(&compact_segment_info.summary.col_stats, columns_min_max)
                        {
                            let seg = match &segment_info {
                                None => {
                                    // un-compact the segment if necessary
                                    segment_info = Some(compact_segment_info.as_ref().try_into()?);
                                    segment_info.as_ref().unwrap()
                                }
                                Some(v) => v,
                            };

                            // block level pruning, using range index
                            for (block_index, block_meta) in seg.blocks.iter().enumerate() {
                                if aggregation_ctx
                                    .overlapped(&block_meta.col_stats, columns_min_max)
                                {
                                    // block level range pruning failed, try applying bloom filter pruning if possible
                                    // note, performance may heavily depend on the bloom filter cache

                                    if let (
                                        Some(bloom_hashes),
                                        Some(bloom_on_conflict_field_index),
                                    ) = (
                                        bloom_hashes,
                                        self.most_significant_on_conflict_field_index,
                                    ) {
                                        // let make it clear, that the bool returned here indicates whether the block is pruned
                                        let pruned = self
                                            .apply_bloom_pruning(
                                                block_meta,
                                                bloom_hashes,
                                                bloom_on_conflict_field_index,
                                            )
                                            .await;

                                        if pruned {
                                            // skip this block
                                            metrics_inc_replace_block_number_bloom_pruned(1);
                                            continue;
                                        }
                                    }

                                    self.deletion_accumulator.add_block_deletion(
                                        *segment_index,
                                        block_index,
                                        key_hashes,
                                    )
                                }
                            }
                        }
                    }

                    let num_blocks_mutated = self.deletion_accumulator.deletions.values().fold(
                        0,
                        |acc, blocks_may_have_row_deletion| {
                            acc + blocks_may_have_row_deletion.len()
                        },
                    );

                    metrics_inc_replace_block_number_after_pruning(num_blocks_mutated as u64);
                }
            }
            MergeIntoOperation::None => {}
        }
        metrics_inc_replace_accumulated_merge_action_time_ms(start.elapsed().as_millis() as u64);
        Ok(())
    }

    // return true if the block is pruned, otherwise false
    async fn apply_bloom_pruning(
        &self,
        block_meta: &BlockMeta,
        input_hashes: &HashSet<u64>,
        bloom_on_conflict_field_index: FieldIndex,
    ) -> bool {
        if let Some(loc) = &block_meta.bloom_filter_index_location {
            match self
                .load_bloom_filter(
                    loc,
                    block_meta.bloom_filter_index_size,
                    bloom_on_conflict_field_index,
                )
                .await
            {
                Ok(filter) => {
                    let mut pruned = true;
                    for hash in input_hashes {
                        if filter.contains_digest(*hash) {
                            // if any of the input hashes is contained in the bloom filter, do not prune
                            pruned = false;
                            break;
                        }
                    }
                    pruned
                }
                Err(e) => {
                    // broken index should not stop us:
                    warn!("failed to build bloom index column name: {}", e);
                    // failed to load bloom filter, do not prune
                    false
                }
            }
        } else {
            // no bloom filter, no pruning
            false
        }
    }

    async fn load_bloom_filter(
        &self,
        location: &Location,
        index_len: u64,
        bloom_on_conflict_field_index: FieldIndex,
    ) -> Result<Arc<Xor8Filter>> {
        // different block may have different version of bloom filter index
        let bloom_column_name = BloomIndex::build_filter_column_name(
            location.1,
            &self.aggregation_ctx.on_conflict_fields[bloom_on_conflict_field_index].table_field,
        )?;

        // using load_bloom_filter_by_columns is attractive,
        // but it do not care about the version of the bloom filter index
        let block_filter = location
            .read_block_filter(
                self.aggregation_ctx.data_accessor.clone(),
                &[bloom_column_name],
                index_len,
            )
            .await?;
        // we know that there is exactly one filter
        Ok(block_filter.filters.into_iter().next().unwrap())
    }
}

// apply the mutations and generate mutation log
impl MergeIntoOperationAggregator {
    #[async_backtrace::framed]
    pub async fn apply(&mut self) -> Result<Option<MutationLogs>> {
        metrics_inc_replace_number_apply_deletion();
        let start = Instant::now();
        let mut mutation_logs = Vec::new();
        let aggregation_ctx = &self.aggregation_ctx;
        let io_runtime = GlobalIORuntime::instance();
        let mut mutation_log_handlers = Vec::new();
        let mut num_rows_mutated = 0;
        for (segment_idx, block_deletion) in self.deletion_accumulator.deletions.drain() {
            let (path, ver) = self
                .aggregation_ctx
                .segment_locations
                .get(&segment_idx)
                .ok_or_else(|| {
                    ErrorCode::Internal(format!(
                        "unexpected, segment (idx {}) not found, during applying mutation log",
                        segment_idx
                    ))
                })?;

            let load_param = LoadParams {
                location: path.clone(),
                len_hint: None,
                ver: *ver,
                put_cache: true,
            };

            let compact_segment_info = aggregation_ctx.segment_reader.read(&load_param).await?;
            let segment_info: SegmentInfo = compact_segment_info.as_ref().try_into()?;

            for (block_index, keys) in block_deletion {
                let permit = aggregation_ctx.acquire_task_permit().await?;
                let block_meta = segment_info.blocks[block_index].clone();
                let aggregation_ctx = aggregation_ctx.clone();
                num_rows_mutated += block_meta.row_count;
                let handle = io_runtime.spawn(async_backtrace::location!().frame({
                    async move {
                        let mutation_log_entry = aggregation_ctx
                            .apply_deletion_to_data_block(
                                segment_idx,
                                block_index,
                                &block_meta,
                                &keys,
                            )
                            .await?;
                        drop(permit);
                        Ok::<_, ErrorCode>(mutation_log_entry)
                    }
                }));
                mutation_log_handlers.push(handle)
            }
        }
        if num_rows_mutated > 0 {
            metrics_inc_replace_row_number_after_pruning(num_rows_mutated);
        }

        let log_entries = futures::future::try_join_all(mutation_log_handlers)
            .await
            .map_err(|e| {
                ErrorCode::Internal("unexpected, failed to join apply-deletion tasks.")
                    .add_message_back(e.to_string())
            })?;

        for maybe_log_entry in log_entries {
            if let Some(segment_mutation_log) = maybe_log_entry? {
                mutation_logs.push(segment_mutation_log);
            }
        }

        metrics_inc_replace_apply_deletion_time_ms(start.elapsed().as_millis() as u64);

        Ok(Some(MutationLogs {
            entries: mutation_logs,
        }))
    }
}

impl AggregationContext {
    #[async_backtrace::framed]
    async fn apply_deletion_to_data_block(
        &self,
        segment_index: SegmentIndex,
        block_index: BlockIndex,
        block_meta: &BlockMeta,
        deleted_key_hashes: &ahash::HashSet<UniqueKeyDigest>,
    ) -> Result<Option<MutationLogEntry>> {
        info!(
            "apply delete to segment idx {}, block idx {}, num of deletion key hashes: {}",
            segment_index,
            block_index,
            deleted_key_hashes.len()
        );

        if block_meta.row_count == 0 {
            return Ok(None);
        }

        let key_columns_data = self.read_block(&self.key_column_reader, block_meta).await?;

        let num_rows = key_columns_data.num_rows();

        let on_conflict_fields = &self.on_conflict_fields;
        let mut columns = Vec::with_capacity(on_conflict_fields.len());
        for (field, _) in on_conflict_fields.iter().enumerate() {
            let on_conflict_field_index = field;
            columns.push(&key_columns_data
                .columns()
                .get(on_conflict_field_index)
                .ok_or_else(|| {
                    ErrorCode::Internal(format!(
                        "unexpected, block entry (index {}) not found. segment index {}, block index {}",
                        on_conflict_field_index, segment_index, block_index
                    ))
                })?
                .value);
        }

        let mut bitmap = MutableBitmap::new();
        for row in 0..num_rows {
            if let Some(hash) = row_hash_of_columns(&columns, row)? {
                // some row hash means on-conflict columns of this row contains non-null values
                // let's check it out
                bitmap.push(!deleted_key_hashes.contains(&hash));
            } else {
                // otherwise, keep this row
                bitmap.push(true);
            }
        }

        let delete_nums = bitmap.unset_bits();
        info!("number of row deleted: {}", delete_nums);

        // shortcut: nothing to be deleted
        if delete_nums == 0 {
            info!("nothing deleted");
            metrics_inc_replace_block_of_zero_row_deleted(1);
            // nothing to be deleted
            return Ok(None);
        }

        let progress_values = ProgressValues {
            rows: delete_nums,
            // ignore bytes.
            bytes: 0,
        };

        self.block_builder
            .ctx
            .get_write_progress()
            .incr(&progress_values);

        // shortcut: whole block deletion
        if delete_nums == block_meta.row_count as usize {
            info!("whole block deletion");
            metrics_inc_replace_whole_block_deletion(1);
            // whole block deletion
            // NOTE that if deletion marker is enabled, check the real meaning of `row_count`
            let mutation = MutationLogEntry::DeletedBlock {
                index: BlockMetaIndex {
                    segment_idx: segment_index,
                    block_idx: block_index,
                },
            };

            return Ok(Some(mutation));
        }

        let bitmap = bitmap.into();
        let mut key_columns_data_after_deletion = key_columns_data.filter_with_bitmap(&bitmap)?;

        let new_block = match &self.remain_column_reader {
            None => key_columns_data_after_deletion,
            Some(remain_columns_reader) => {
                metrics_inc_replace_block_number_totally_loaded(1);
                metrics_inc_replace_row_number_totally_loaded(block_meta.row_count);

                // read the remaining columns
                let remain_columns_data =
                    self.read_block(remain_columns_reader, block_meta).await?;

                // remove the deleted rows
                let remain_columns_data_after_deletion =
                    remain_columns_data.filter_with_bitmap(&bitmap)?;

                // merge the remaining columns
                for col in remain_columns_data_after_deletion.columns() {
                    key_columns_data_after_deletion.add_column(col.clone());
                }

                // resort the block
                let col_indexes = self
                    .on_conflict_fields
                    .iter()
                    .map(|f| f.field_index)
                    .chain(self.remain_column_field_ids.iter().copied())
                    .collect::<Vec<_>>();
                let mut projection = (0..col_indexes.len()).collect::<Vec<_>>();
                projection.sort_by_key(|&i| col_indexes[i]);
                let func_ctx = self.block_builder.ctx.get_function_context()?;
                BlockOperator::Project { projection }
                    .execute(&func_ctx, key_columns_data_after_deletion)?
            }
        };

        // serialization and compression is cpu intensive, send them to dedicated thread pool
        // and wait (asyncly, which will NOT block the executor thread)
        let block_builder = self.block_builder.clone();
        let origin_stats = block_meta.cluster_stats.clone();
        let serialized = GlobalIORuntime::instance()
            .spawn_blocking(move || {
                block_builder.build(new_block, |block, generator| {
                    let cluster_stats =
                        generator.gen_with_origin_stats(&block, origin_stats.clone())?;
                    Ok((cluster_stats, block))
                })
            })
            .await?;

        // persistent data
        let new_block_meta = serialized.block_meta;
        let new_block_location = new_block_meta.location.0.clone();
        let new_block_raw_data = serialized.block_raw_data;
        let data_accessor = self.data_accessor.clone();
        write_data(new_block_raw_data, &data_accessor, &new_block_location).await?;

        metrics_inc_replace_block_number_write(1);
        metrics_inc_replace_row_number_write(new_block_meta.row_count);
        if let Some(index_state) = serialized.bloom_index_state {
            write_data(index_state.data, &data_accessor, &index_state.location.0).await?;
        }

        // generate log
        let mutation = MutationLogEntry::Replaced {
            index: BlockMetaIndex {
                segment_idx: segment_index,
                block_idx: block_index,
            },
            block_meta: Arc::new(new_block_meta),
        };

        Ok(Some(mutation))
    }

    #[async_backtrace::framed]
    async fn acquire_task_permit(&self) -> Result<OwnedSemaphorePermit> {
        let permit = self
            .io_request_semaphore
            .clone()
            .acquire_owned()
            .await
            .map_err(|e| {
                ErrorCode::Internal("unexpected, io request semaphore is closed. {}")
                    .add_message_back(e.to_string())
            })?;
        Ok(permit)
    }

    fn overlapped(
        &self,
        column_stats: &HashMap<ColumnId, ColumnStatistics>,
        columns_min_max: &[(Scalar, Scalar)],
    ) -> bool {
        Self::check_overlap(&self.on_conflict_fields, column_stats, columns_min_max)
    }

    // if any item of `column_min_max` does NOT overlap with the corresponding item of `column_stats`
    // returns false, otherwise returns true.
    fn check_overlap(
        on_conflict_fields: &[OnConflictField],
        column_stats: &HashMap<ColumnId, ColumnStatistics>,
        columns_min_max: &[(Scalar, Scalar)],
    ) -> bool {
        for (idx, field) in on_conflict_fields.iter().enumerate() {
            let column_id = field.table_field.column_id();
            let (min, max) = &columns_min_max[idx];
            if !Self::check_overlapped_by_stats(column_stats.get(&column_id), min, max) {
                return false;
            }
        }
        true
    }

    fn check_overlapped_by_stats(
        column_stats: Option<&ColumnStatistics>,
        key_min: &Scalar,
        key_max: &Scalar,
    ) -> bool {
        if let Some(stats) = column_stats {
            let max = stats.max();
            let min = stats.min();
            std::cmp::min(key_max, max) >= std::cmp::max(key_min, min)
                || // coincide overlap
                (max == key_max && min == key_min)
        } else {
            false
        }
    }

    async fn read_block(&self, reader: &BlockReader, block_meta: &BlockMeta) -> Result<DataBlock> {
        let merged_io_read_result = reader
            .read_columns_data_by_merge_io(
                &self.read_settings,
                &block_meta.location.0,
                &block_meta.col_metas,
            )
            .await?;

        // deserialize block data
        // cpu intensive task, send them to dedicated thread pool
        let storage_format = self.write_settings.storage_format;
        let block_meta_ptr = block_meta.clone();
        let reader = reader.clone();
        GlobalIORuntime::instance()
            .spawn_blocking(move || {
                let column_chunks = merged_io_read_result.columns_chunks()?;
                reader.deserialize_chunks(
                    block_meta_ptr.location.0.as_str(),
                    block_meta_ptr.row_count as usize,
                    &block_meta_ptr.compression,
                    &block_meta_ptr.col_metas,
                    column_chunks,
                    &storage_format,
                )
            })
            .await
    }
}

#[cfg(test)]
mod tests {
    use common_expression::types::NumberDataType;
    use common_expression::types::NumberScalar;
    use common_expression::TableDataType;
    use common_expression::TableField;

    use super::*;

    #[test]
    fn test_check_overlap() -> Result<()> {
        // setup :
        //
        // - on conflict('xx_id', 'xx_type', 'xx_time');
        //
        // - range index of columns
        //   'xx_id' : [1, 10]
        //   'xx_type' : ["a", "z"]
        //   'xx_time' : [100, 200]

        // setup schema
        let field_type_id = TableDataType::Number(NumberDataType::UInt64);
        let field_type_string = TableDataType::String;
        let field_type_time = TableDataType::Number(NumberDataType::UInt32);

        let xx_id = TableField::new("xx_id", field_type_id);
        let xx_type = TableField::new("xx_type", field_type_string);
        let xx_time = TableField::new("xx_time", field_type_time);

        let schema = TableSchema::new(vec![xx_id, xx_type, xx_time]);

        let fields = schema.fields();

        // setup the ON CONFLICT fields
        let on_conflict_fields = fields
            .iter()
            .enumerate()
            .map(|(id, field)| OnConflictField {
                table_field: field.clone(),
                field_index: id,
            })
            .collect::<Vec<_>>();

        // set up range index of columns
        // the null_count/in_memory_size/distinct_of_values do not matter in this case
        let range = |min: Scalar, max: Scalar| ColumnStatistics::new(min, max, 0, 0, None);

        let column_range_indexes = HashMap::from_iter([
            // range of xx_id [1, 10]
            (
                0,
                range(
                    Scalar::Number(NumberScalar::UInt64(1)),
                    Scalar::Number(NumberScalar::UInt64(10)),
                ),
            ),
            // range of xx_type [a, z]
            (
                1,
                range(
                    Scalar::String("a".to_string().into_bytes()),
                    Scalar::String("z".to_string().into_bytes()),
                ),
            ),
            // range of xx_time [100, 200]
            (
                2,
                range(
                    Scalar::Number(NumberScalar::UInt32(100)),
                    Scalar::Number(NumberScalar::UInt32(200)),
                ),
            ),
        ]);

        // case 1:
        //
        // - min/max of input block
        //
        //  'xx_id' : [1, 9]
        //  'xx_type' : ["b", "y"]
        //  'xx_time' : [101, 200]
        //
        // - recall that the range index of columns are:
        //
        //   'xx_id' : [1, 10]
        //   'xx_type' : ["a", "z"]
        //   'xx_time' : [100, 200]
        //
        // - expected : overlap == true
        //   since value of all the ON CONFLICT columns of input block overlap with range index

        let input_column_min_max = [
            // for xx_id column, overlaps
            (
                Scalar::Number(NumberScalar::UInt64(1)),
                Scalar::Number(NumberScalar::UInt64(9)),
            ),
            // for xx_type column, overlaps
            (
                Scalar::String("b".to_string().into_bytes()),
                Scalar::String("y".to_string().into_bytes()),
            ),
            // for xx_time column, overlaps
            (
                Scalar::Number(NumberScalar::UInt32(101)),
                Scalar::Number(NumberScalar::UInt32(200)),
            ),
        ];

        let overlap = super::AggregationContext::check_overlap(
            &on_conflict_fields,
            &column_range_indexes,
            &input_column_min_max,
        );

        assert!(overlap);

        // case 2:
        //
        // - min/max of input block
        //
        //  'xx_id' : [11, 12]
        //  'xx_type' : ["b", "b"]
        //  'xx_time' : [100, 100]
        //
        // - recall that the range index of columns are:
        //
        //   'xx_id' : [1, 10]
        //   'xx_type' : ["a", "z"]
        //   'xx_time' : [100, 200]
        //
        // - expected : overlap == false
        //
        //   although columns 'xx_type' and 'xx_time' do overlap, but 'xx_id' does not overlap,
        //   so the result is NOT overlap

        let input_column_min_max = [
            // for xx_id column, NOT overlaps
            (
                Scalar::Number(NumberScalar::UInt64(11)),
                Scalar::Number(NumberScalar::UInt64(12)),
            ),
            // for xx_type column, overlaps
            (
                Scalar::String("b".to_string().into_bytes()),
                Scalar::String("b".to_string().into_bytes()),
            ),
            // for xx_time column, overlaps
            (
                Scalar::Number(NumberScalar::UInt32(100)),
                Scalar::Number(NumberScalar::UInt32(100)),
            ),
        ];

        let overlap = super::AggregationContext::check_overlap(
            &on_conflict_fields,
            &column_range_indexes,
            &input_column_min_max,
        );

        assert!(!overlap);

        Ok(())
    }
}
