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
use common_catalog::table_context::TableContext;
use common_exception::Result;
use common_expression::ColumnId;
use common_expression::Scalar;
use common_expression::TableSchemaRef;
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
use crate::io::ReadSettings;
use crate::io::SegmentInfoReader;
use crate::io::TableMetaLocationGenerator;
use crate::io::WriteSettings;
use crate::operations::merge_into::mutation_meta::merge_into_operation_meta::DeletionByColumn;
use crate::operations::merge_into::mutation_meta::merge_into_operation_meta::MergeIntoOperation;
use crate::operations::merge_into::mutation_meta::merge_into_operation_meta::UniqueKeyDigest;
use crate::operations::merge_into::mutation_meta::mutation_meta::BlockMetaIndex;
use crate::operations::merge_into::mutation_meta::mutation_meta::Mutation;
use crate::operations::merge_into::mutation_meta::mutation_meta::MutationLog;
use crate::operations::merge_into::mutation_meta::mutation_meta::MutationLogEntry;
use crate::operations::merge_into::mutation_meta::mutation_meta::MutationLogs;
use crate::operations::merge_into::mutator::deletion_accumulator::DeletionAccumulator;
use crate::operations::mutation::base_mutator::BlockIndex;
use crate::operations::mutation::base_mutator::SegmentIndex;

// Apply MergeIntoOperations to segments
pub struct MergeIntoOperationAggregator {
    ctx: Arc<dyn TableContext>,
    // segments that this mutator is in charge of
    segment_locations: Vec<(Location, SegmentIndex)>,
    column_id: ColumnId,
    deletion_accumulator: DeletionAccumulator,
    data_accessor: Operator,
    // schema of table we are working on (no projection)
    schema: TableSchemaRef,
    block_reader: BlockReader,
    location_gen: TableMetaLocationGenerator,
    write_settings: WriteSettings,
    segment_reader: SegmentInfoReader,
    block_builder: BlockBuilder,
}

impl MergeIntoOperationAggregator {
    pub fn try_create(segment_location: &[Location]) -> Result<Self> {
        todo!()
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
                for ((path, ver), segment_index) in &self.segment_locations {
                    let load_param = LoadParams {
                        location: path.clone(),
                        len_hint: None,
                        ver: *ver,
                        put_cache: true,
                    };
                    // for typical configuration, segment cache is enabled, thus after the first loop, we are reading from cache
                    let segment_info = self.segment_reader.read(&load_param).await?;
                    // segment level prune
                    if !self.overlapped(&segment_info.summary.col_stats, &key_min, &key_max) {
                        // block level prune
                        for (block_index, block_meta) in segment_info.blocks.iter().enumerate() {
                            if !self.overlapped(&block_meta.col_stats, &key_min, &&key_max) {
                                self.deletion_accumulator.add_block_deletion(
                                    *segment_index,
                                    block_index,
                                    &key_hashes,
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
            let ((path, ver), segment_index) = &self.segment_locations[*segment_idx as usize];
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
                    .apply_deletion_to_data_block(*segment_index, *block_index, block_meta, keys)
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
        let settings = ReadSettings::default(); // TODO
        let field_index = 0; // TODO the index of column (not the ColumnId)
        // TODO optimize, "prewhere"
        // if delete all or delete nothing is common
        // NOTE: here both segment and block meta pruning have been performed
        let data_block = reader
            //.read_by_meta(&settings, block_meta, &FuseStorageFormat::Parquet) // use generic reader, read native as well
            .read_by_meta(&settings, block_meta, &self.write_settings.storage_format) // use generic reader, read native as well
            .await?;
        let num_rows = data_block.num_rows();
        let key_column = data_block.columns()[field_index].value.as_column().unwrap();
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
        // and wait (async)
        let block_builder = self.block_builder.clone();
        let serialized = tokio_rayon::spawn(move || block_builder.build(new_block)).await?;

        let new_block_meta = serialized.block_meta;
        let location = block_meta.location.0.clone();
        let raw_data = serialized.block_raw_data;
        let data_accessor = self.data_accessor.clone();
        // async processor is driven by global runtime io
        write_data(raw_data, &data_accessor, &location).await?;

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
        if let Some(stats) = column_stats.get(&self.column_id) {
            if &stats.max <= key_min || key_max <= &stats.min {
                return true;
            }
        }
        false
    }
}
