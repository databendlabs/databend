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
use std::hash::Hasher;
use std::sync::Arc;

use common_arrow::arrow::bitmap::MutableBitmap;
use common_catalog::plan::Projection;
use common_catalog::table_context::TableContext;
use common_exception::ErrorCode;
use common_exception::Result;
use common_expression::ColumnId;
use common_expression::Scalar;
use common_expression::TableSchema;
use opendal::Operator;
use siphasher::sip128;
use siphasher::sip128::Hasher128;
use storages_common_cache::LoadParams;
use storages_common_table_meta::meta::BlockMeta;
use storages_common_table_meta::meta::ColumnStatistics;
use storages_common_table_meta::meta::Location;

use crate::io::write_data;
use crate::io::BlockBuilder;
use crate::io::BlockReader;
use crate::io::MetaReaders;
use crate::io::ReadSettings;
use crate::io::SegmentInfoReader;
use crate::io::WriteSettings;
use crate::operations::merge_into::mutation_meta::merge_into_operation_meta::DeletionByColumn;
use crate::operations::merge_into::mutation_meta::merge_into_operation_meta::MergeIntoOperation;
use crate::operations::merge_into::mutation_meta::merge_into_operation_meta::UniqueKeyDigest;
use crate::operations::merge_into::mutation_meta::mutation_log::BlockMetaIndex;
use crate::operations::merge_into::mutation_meta::mutation_log::Mutation;
use crate::operations::merge_into::mutation_meta::mutation_log::MutationLog;
use crate::operations::merge_into::mutation_meta::mutation_log::MutationLogEntry;
use crate::operations::merge_into::mutation_meta::mutation_log::MutationLogs;
use crate::operations::merge_into::mutator::deletion_accumulator::DeletionAccumulator;
use crate::operations::mutation::base_mutator::BlockIndex;
use crate::operations::mutation::base_mutator::SegmentIndex;

// Apply MergeIntoOperations to segments
pub struct MergeIntoOperationAggregator {
    segment_locations: HashMap<SegmentIndex, Location>,
    deletion_accumulator: DeletionAccumulator,
    // TODO encapsulate these (support multi columns)
    on_conflict_field_index: usize,
    on_conflict_field_id: ColumnId,
    block_reader: Arc<BlockReader>,
    data_accessor: Operator,
    write_settings: WriteSettings,
    read_settings: ReadSettings,
    segment_reader: SegmentInfoReader,
    block_builder: BlockBuilder,
}

impl MergeIntoOperationAggregator {
    #[allow(clippy::too_many_arguments)] // TODO fix this
    pub fn try_create(
        ctx: Arc<dyn TableContext>,
        on_conflict_field_index: usize,
        on_conflict_field_id: ColumnId,
        segment_locations: Vec<(SegmentIndex, Location)>,
        data_accessor: Operator,
        table_schema: Arc<TableSchema>,
        write_settings: WriteSettings,
        read_settings: ReadSettings,
        block_builder: BlockBuilder,
    ) -> Result<Self> {
        let deletion_accumulator = DeletionAccumulator::default();
        let segment_reader =
            MetaReaders::segment_info_reader(data_accessor.clone(), table_schema.clone());
        let indices = (0..table_schema.fields().len())
            .into_iter()
            .collect::<Vec<usize>>();
        let projection = Projection::Columns(indices);
        let block_reader =
            BlockReader::create(data_accessor.clone(), table_schema, projection, ctx.clone())?;

        Ok(Self {
            segment_locations: HashMap::from_iter(segment_locations.into_iter()),
            deletion_accumulator,
            on_conflict_field_index,
            on_conflict_field_id,
            block_reader,
            data_accessor,
            write_settings,
            read_settings,
            segment_reader,
            block_builder,
        })
    }
}

// aggregate mutations (currently, deletion only)
impl MergeIntoOperationAggregator {
    pub async fn accumulate(&mut self, merge_action: &MergeIntoOperation) -> Result<()> {
        match &merge_action {
            MergeIntoOperation::Delete(DeletionByColumn {
                key_min,
                key_max,
                key_hashes,
            }) => {
                for (segment_index, (path, ver)) in &self.segment_locations {
                    let load_param = LoadParams {
                        location: path.clone(),
                        len_hint: None,
                        ver: *ver,
                        put_cache: true,
                    };
                    // for typical configuration, segment cache is enabled, thus after the first loop, we are reading from cache
                    let segment_info = self.segment_reader.read(&load_param).await?;
                    // segment level
                    if self.overlapped(&segment_info.summary.col_stats, key_min, key_max) {
                        // block level
                        for (block_index, block_meta) in segment_info.blocks.iter().enumerate() {
                            if self.overlapped(&block_meta.col_stats, key_min, key_max) {
                                self.deletion_accumulator.add_block_deletion(
                                    *segment_index,
                                    block_index,
                                    key_hashes,
                                )
                            }
                        }
                    }
                }
            }
            MergeIntoOperation::None => {}
        }
        Ok(())
    }
}

// apply the mutations and generate mutation log
impl MergeIntoOperationAggregator {
    pub async fn apply(&mut self) -> Result<Option<MutationLogs>> {
        let mut mutation_logs = Vec::new();
        for (segment_idx, block_deletion) in &self.deletion_accumulator.deletions {
            // do we need a local cache?
            let (path, ver) = self.segment_locations.get(segment_idx).ok_or_else(|| {
                ErrorCode::Internal(format!(
                    "unexpected, segment (idx {}) not found, during appply",
                    segment_idx
                ))
            })?;

            let load_param = LoadParams {
                location: path.clone(),
                len_hint: None,
                ver: *ver,
                put_cache: true,
            };

            let segment_info = self.segment_reader.read(&load_param).await?;

            for (block_index, keys) in block_deletion {
                let block_meta = &segment_info.blocks[*block_index];
                if let Some(segment_mutation_log) = self
                    .apply_deletion_to_data_block(*segment_idx, *block_index, block_meta, keys)
                    .await?
                {
                    mutation_logs.push(MutationLogEntry::Mutation(segment_mutation_log));
                }
            }
        }
        Ok(Some(MutationLogs {
            entries: mutation_logs,
        }))
    }

    async fn apply_deletion_to_data_block(
        &self,
        segment_index: SegmentIndex,
        block_index: BlockIndex,
        block_meta: &BlockMeta,
        deleted_key_hashes: &HashSet<UniqueKeyDigest>,
    ) -> Result<Option<MutationLog>> {
        if block_meta.row_count == 0 {
            return Ok(None);
        }

        let reader = &self.block_reader;
        let on_conflict_field_index = self.on_conflict_field_index;
        // TODO optimization "prewhere"?
        let data_block = reader
            .read_by_meta(
                &self.read_settings,
                block_meta,
                &self.write_settings.storage_format,
            )
            .await?;
        let num_rows = data_block.num_rows();

        // TODO report an error
        let key_column = data_block.columns()[on_conflict_field_index]
            .value
            .as_column()
            .unwrap();

        let mut bitmap = MutableBitmap::new();
        for i in 0..num_rows {
            let value = key_column.index(i).unwrap();
            let string = value.to_string();
            let mut sip = sip128::SipHasher24::new();
            sip.write(string.as_bytes());
            let hash = sip.finish128().as_u128();
            bitmap.push(!deleted_key_hashes.contains(&hash));
        }

        // shortcuts
        if bitmap.unset_bits() == 0 {
            // nothing to be deleted
            return Ok(None);
        }

        if bitmap.unset_bits() == block_meta.row_count as usize {
            // whole block deletion
            // NOTE that if deletion marker is enabled, check the real meaning of `row_count`
            let mutation = MutationLog {
                index: BlockMetaIndex {
                    segment_idx: segment_index,
                    block_idx: block_index,
                    range: None,
                },
                op: Mutation::Deleted,
            };

            return Ok(Some(mutation));
        }

        let bitmap = bitmap.into();
        let new_block = data_block.filter_with_bitmap(&bitmap)?;

        // serialization and compression is cpu intensive, send them to dedicated thread pool
        // and wait (asyncly, which will NOT block the executor thread)
        let block_builder = self.block_builder.clone();
        let serialized = tokio_rayon::spawn(move || block_builder.build(new_block)).await?;

        // persistent data
        let new_block_meta = serialized.block_meta;
        let new_block_location = new_block_meta.location.0.clone();
        let new_block_raw_data = serialized.block_raw_data;
        let data_accessor = self.data_accessor.clone();
        write_data(new_block_raw_data, &data_accessor, &new_block_location).await?;
        if let Some(index_state) = serialized.bloom_index_state {
            write_data(index_state.data, &data_accessor, &index_state.location.0).await?;
        }

        // generate log
        let mutation = MutationLog {
            index: BlockMetaIndex {
                segment_idx: segment_index,
                block_idx: block_index,
                range: None,
            },
            op: Mutation::Replaced(Arc::new(new_block_meta)),
        };

        Ok(Some(mutation))
    }

    fn overlapped(
        &self,
        column_stats: &HashMap<ColumnId, ColumnStatistics>,
        key_min: &Scalar,
        key_max: &Scalar,
    ) -> bool {
        if let Some(stats) = column_stats.get(&self.on_conflict_field_id) {
            return if &stats.max <= key_min || key_max <= &stats.min {
                // non-coincide overlap
                true
            } else {
                // coincide overlap
                &stats.max == key_max && &stats.min == key_min
            };
        }
        false
    }
}
