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

use std::collections::BTreeMap;
use std::sync::Arc;

use databend_common_exception::ErrorCode;
use databend_common_exception::Result;
use databend_common_expression::BlockMetaInfoDowncast;
use databend_common_expression::DataBlock;
use databend_common_metrics::storage::metrics_inc_recluster_write_block_nums;
use databend_common_pipeline_transforms::AccumulatingTransform;
use databend_storages_common_table_meta::meta::ExtendedBlockMeta;

use super::slice::IndexPartitionSlices;
use super::state::ReclusterIndexMergeState;
use crate::io::block_index::BlockIndexMergeWriter;
use crate::io::block_index::BlockIndexSlice;
use crate::operations::common::MutationLogEntry;
use crate::operations::common::MutationLogs;

/// One output while its slices arrive: the open writer of every index, and the block metadata
/// the finished writers record themselves in.
struct OutputWriters {
    writers: BTreeMap<u16, Box<dyn BlockIndexMergeWriter>>,
    meta: Option<ExtendedBlockMeta>,
}

/// Replays the partitions, which arrive in order, into the writers of the outputs this
/// processor owns (`output % writers == writer`). When the input ends, finishes them and emits
/// the final block metadata as `MutationLogs`.
pub struct TransformReclusterIndexWriter {
    state: Arc<ReclusterIndexMergeState>,
    writer: usize,
    writers: usize,
    outputs: BTreeMap<usize, OutputWriters>,
    next_partition: u32,
}

impl TransformReclusterIndexWriter {
    pub fn new(state: Arc<ReclusterIndexMergeState>, writer: usize, writers: usize) -> Self {
        Self {
            state,
            writer,
            writers: writers.max(1),
            outputs: BTreeMap::new(),
            next_partition: 0,
        }
    }

    fn append(&mut self, index: u16, slice: BlockIndexSlice) -> Result<()> {
        let output = slice.output as usize;
        if output % self.writers != self.writer {
            return Err(ErrorCode::Internal(
                "recluster index slice routed to the wrong writer",
            ));
        }
        let state = self.state.clone();
        let writers = self.outputs.entry(output).or_insert_with(|| OutputWriters {
            writers: BTreeMap::new(),
            meta: None,
        });
        // Indexes arrive one after another: a partition of a later index means the earlier
        // index received all of its partitions.
        while let Some((&open, _)) = writers.writers.first_key_value() {
            if open >= index {
                break;
            }
            Self::finish_index(&state, output, writers, open)?;
        }
        if !writers.writers.contains_key(&index) {
            let merger = state.merger(index as usize)?;
            let writer = merger.merge_writer(state.mapping()?, output)?;
            writers.writers.insert(index, writer);
        }
        let writer = writers.writers.get_mut(&index).expect("inserted above");
        writer.append(slice)
    }

    /// Finishes the writer of `index` for `output`; it records itself in the block metadata.
    fn finish_index(
        state: &ReclusterIndexMergeState,
        output: usize,
        writers: &mut OutputWriters,
        index: u16,
    ) -> Result<()> {
        let Some(writer) = writers.writers.remove(&index) else {
            return Ok(());
        };
        let meta = match writers.meta.as_mut() {
            Some(meta) => meta,
            None => writers.meta.insert(state.take_output_meta(output)?),
        };
        writer.finish(&mut meta.block_meta)
    }

    /// Every partition arrived: finish the remaining writers and emit one block per output.
    fn finish(&mut self) -> Result<Vec<DataBlock>> {
        let state = self.state.clone();
        let Some(total) = state.partitions_total() else {
            return Err(ErrorCode::Internal(
                "recluster index partitions were never listed",
            ));
        };
        if self.next_partition != total {
            return Err(ErrorCode::Internal(format!(
                "recluster index writer saw {} of {total} partitions",
                self.next_partition
            )));
        }
        let mut owned = 0usize;
        for output in 0..state.num_outputs()? {
            if output % self.writers == self.writer {
                owned += 1;
            }
        }
        if owned != self.outputs.len() {
            return Err(ErrorCode::Internal(format!(
                "recluster index writer owns {owned} outputs but saw slices for {}",
                self.outputs.len()
            )));
        }
        let mut blocks = Vec::with_capacity(self.outputs.len());
        for (output, mut writers) in std::mem::take(&mut self.outputs) {
            while let Some((&open, _)) = writers.writers.first_key_value() {
                Self::finish_index(&state, output, &mut writers, open)?;
            }
            let Some(meta) = writers.meta else {
                return Err(ErrorCode::Internal("recluster index output meta missing"));
            };
            metrics_inc_recluster_write_block_nums();
            blocks.push(DataBlock::empty_with_meta(Box::new(MutationLogs {
                entries: vec![MutationLogEntry::AppendBlock {
                    block_meta: Arc::new(meta),
                    merge_hll: false,
                }],
                ..Default::default()
            })));
        }
        Ok(blocks)
    }
}

impl AccumulatingTransform for TransformReclusterIndexWriter {
    const NAME: &'static str = "ReclusterIndexWriter";

    fn transform(&mut self, mut block: DataBlock) -> Result<Vec<DataBlock>> {
        let Some(meta) = block.take_meta() else {
            return Err(ErrorCode::Internal(
                "missing recluster index partition slices",
            ));
        };
        let Some(slices) = IndexPartitionSlices::downcast_from(meta) else {
            return Err(ErrorCode::Internal(
                "unexpected recluster index partition slices",
            ));
        };
        if slices.id != self.next_partition {
            return Err(ErrorCode::Internal(format!(
                "recluster index partition {} arrived, expected {}",
                slices.id, self.next_partition
            )));
        }
        self.next_partition += 1;
        for slice in slices.slices {
            self.append(slices.index, slice)?;
        }
        Ok(vec![])
    }

    fn on_finish(&mut self, output: bool) -> Result<Vec<DataBlock>> {
        if !output {
            return Ok(vec![]);
        }
        self.finish()
    }
}
