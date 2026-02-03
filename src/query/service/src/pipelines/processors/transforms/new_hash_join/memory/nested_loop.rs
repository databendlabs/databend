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

use std::sync::Arc;
use std::sync::PoisonError;

use databend_common_base::base::ProgressValues;
use databend_common_exception::Result;
use databend_common_expression::BlockEntry;
use databend_common_expression::Column;
use databend_common_expression::DataBlock;
use databend_common_expression::SELECTIVITY_THRESHOLD;

use crate::pipelines::processors::transforms::BasicHashJoinState;
use crate::pipelines::processors::transforms::GraceMemoryJoin;
use crate::pipelines::processors::transforms::HashJoinHashTable;
use crate::pipelines::processors::transforms::Join;
use crate::pipelines::processors::transforms::JoinRuntimeFilterPacket;
use crate::pipelines::processors::transforms::JoinStream;
use crate::pipelines::processors::transforms::NestedLoopDesc;
use crate::pipelines::processors::transforms::hash_join_table::RowPtr;
use crate::pipelines::processors::transforms::new_hash_join::join::EmptyJoinStream;

pub struct NestedLoopJoin<T> {
    inner: T,
    state: Arc<BasicHashJoinState>,
    desc: NestedLoopDesc,
}

impl<T> NestedLoopJoin<T> {
    pub fn new(inner: T, state: Arc<BasicHashJoinState>, desc: NestedLoopDesc) -> Self {
        Self { inner, state, desc }
    }

    fn finalize_chunks(&self) {
        if !self.state.columns.is_empty() {
            return;
        }

        let locked = self.state.mutex.lock();
        let _locked = locked.unwrap_or_else(PoisonError::into_inner);

        if !self.state.columns.is_empty() {
            return;
        }
        let num_columns = if let Some(block) = self.state.chunks.first() {
            *self.state.column_types.as_mut() = block
                .columns()
                .iter()
                .map(|entry| entry.data_type())
                .collect();
            block.num_columns()
        } else {
            return;
        };

        *self.state.columns.as_mut() = (0..num_columns)
            .map(|offset| {
                let full_columns = self
                    .state
                    .chunks
                    .iter()
                    .map(|block| block.get_by_offset(offset).to_column())
                    .collect::<Vec<_>>();

                Column::take_downcast_column_vec(&full_columns)
            })
            .collect();
    }
}

impl<T: Join> Join for NestedLoopJoin<T> {
    fn add_block(&mut self, data: Option<DataBlock>) -> Result<()> {
        self.inner.add_block(data)
    }

    fn final_build(&mut self) -> Result<Option<ProgressValues>> {
        self.inner.final_build()
    }

    fn add_runtime_filter_packet(&self, packet: JoinRuntimeFilterPacket) {
        self.inner.add_runtime_filter_packet(packet);
    }

    fn build_runtime_filter(&self) -> Result<JoinRuntimeFilterPacket> {
        self.inner.build_runtime_filter()
    }

    fn probe_block(&mut self, data: DataBlock) -> Result<Box<dyn JoinStream + '_>> {
        if data.is_empty() || *self.state.build_rows == 0 {
            return Ok(Box::new(EmptyJoinStream));
        }

        let HashJoinHashTable::NestedLoop(build_blocks) = &*self.state.hash_table else {
            return self.inner.probe_block(data);
        };
        self.finalize_chunks();

        let max_block_size = self.desc.filter.max_block_size();
        Ok(Box::new(NestedLoopJoinStream {
            probe_block: data,
            build_blocks,
            state: &self.state,
            max_block_size,
            desc: &mut self.desc,
            matches: Vec::with_capacity(max_block_size),
            build_block_index: 0,
            build_row_index: 0,
            use_range: false,
        }))
    }
}

impl<T> GraceMemoryJoin for NestedLoopJoin<T>
where T: GraceMemoryJoin
{
    fn reset_memory(&mut self) {
        self.inner.reset_memory()
    }
}

struct NestedLoopJoinStream<'a> {
    probe_block: DataBlock,
    build_blocks: &'a [DataBlock],
    state: &'a BasicHashJoinState,
    desc: &'a mut NestedLoopDesc,
    max_block_size: usize,
    build_block_index: usize,
    build_row_index: usize,
    matches: Vec<(u32, RowPtr)>,
    use_range: bool,
}

impl<'a> NestedLoopJoinStream<'a> {
    fn process_next_row(&mut self) -> Result<()> {
        let build_block = &self.build_blocks[self.build_block_index];

        let probe_rows = self.probe_block.num_rows();
        let entries = self
            .probe_block
            .columns()
            .iter()
            .cloned()
            .chain(build_block.columns().iter().map(|entry| {
                BlockEntry::Const(
                    entry.index(self.build_row_index).unwrap().to_owned(),
                    entry.data_type(),
                    probe_rows,
                )
            }))
            .collect();
        let merged = DataBlock::new(entries, probe_rows);
        let row_ptr = RowPtr {
            chunk_index: self.build_block_index as u32,
            row_index: self.build_row_index as u32,
        };

        let max_block_size = self.desc.filter.max_block_size();
        if merged.num_rows() <= max_block_size {
            let result_count = self.desc.filter.select(&merged)?;
            self.matches.extend(
                self.desc.filter.true_selection()[..result_count]
                    .iter()
                    .map(|probe| (*probe, row_ptr)),
            );
        } else {
            for (i, block) in merged
                .split_by_rows_no_tail(max_block_size)
                .into_iter()
                .enumerate()
            {
                let offset = (i * max_block_size) as u32;
                let result_count = self.desc.filter.select(&block)?;
                self.matches.extend(
                    self.desc.filter.true_selection()[..result_count]
                        .iter()
                        .map(|probe| (*probe + offset, row_ptr)),
                );
            }
        }

        self.build_row_index += 1;
        if self.build_row_index >= build_block.num_rows() {
            self.build_row_index = 0;
            self.build_block_index += 1;
        }

        Ok(())
    }

    fn emit_block(&mut self, count: usize) -> Result<DataBlock> {
        if !self.use_range
            && self.matches.len() as f64
                > SELECTIVITY_THRESHOLD * self.probe_block.num_rows() as f64
        {
            // Need to test the scenario where a probe matches multiple builds
            self.use_range = true;
        }

        let block = {
            if self.use_range {
                self.matches.sort_unstable_by_key(|(probe, _)| *probe);
            }
            let (probe_indices, build_indices): (Vec<_>, Vec<_>) =
                self.matches.drain(..count).unzip();

            let probe = self.probe_block.clone().project(&self.desc.projections);
            let probe = if self.use_range {
                let ranges = DataBlock::merge_indices_to_ranges(&probe_indices);
                probe.take_ranges(&ranges, count)?
            } else {
                probe.take_with_optimize_size(probe_indices.as_slice())?
            };

            let build_entries = self
                .state
                .columns
                .iter()
                .zip(self.state.column_types.as_slice())
                .enumerate()
                .filter_map(|(i, x)| {
                    let i = self.probe_block.num_columns() + i;
                    self.desc.projections.contains(&i).then_some(x)
                })
                .map(|(columns, data_type)| {
                    Column::take_column_vec_indices(columns, data_type.clone(), &build_indices)
                        .into()
                });

            DataBlock::from_iter(probe.take_columns().into_iter().chain(build_entries), count)
        };

        if let Some(field_reorder) = &self.desc.field_reorder {
            Ok(DataBlock::from_iter(
                field_reorder
                    .iter()
                    .map(|offset| block.get_by_offset(*offset).clone()),
                block.num_rows(),
            ))
        } else {
            Ok(block)
        }
    }
}

impl<'a> JoinStream for NestedLoopJoinStream<'a> {
    fn next(&mut self) -> Result<Option<DataBlock>> {
        loop {
            if self.matches.len() >= self.max_block_size {
                return Ok(Some(self.emit_block(self.max_block_size)?));
            }

            if self.build_block_index >= self.build_blocks.len() {
                return if self.matches.is_empty() {
                    Ok(None)
                } else {
                    Ok(Some(self.emit_block(self.matches.len())?))
                };
            }

            self.process_next_row()?;
        }
    }
}
