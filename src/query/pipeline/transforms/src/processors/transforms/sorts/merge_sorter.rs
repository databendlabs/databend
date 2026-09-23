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

use databend_common_exception::Result;
use databend_common_expression::DataBlock;

use super::Base;
use super::MemoryMerger;
use super::SortSpill;
use super::SortSpillParams;
use super::core::algorithm::SortAlgorithm;
use super::create_memory_merger;
use crate::traits::SortSpiller;

/// Work required after consuming any pending output. Only Collecting accepts input.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum MergeSortStatus {
    Collecting,
    Merge,
    Finished,
}

#[allow(clippy::large_enum_variant)]
enum State<A: SortAlgorithm> {
    Collecting,
    Spill { finish_input: bool, write: bool },
    PrepareMerge,
    Memory(MemoryMerger<A>),
    Restore,
    Finished,
}

/// Merges individually sorted blocks, using external runs when requested.
///
/// Every input block must contain the comparison column at `base.sort_row_offset`.
/// The caller owns key encoding, spill thresholds and input/output scheduling. This
/// sorter owns buffered runs and the transition from ingestion to merged output;
/// it neither modifies the block schema nor interprets SQL partition semantics.
/// `base.limit` prunes run candidates; callers still enforce the final output limit.
pub struct MergeSorter<A: SortAlgorithm, S: SortSpiller> {
    base: Base<S>,
    output_rows: usize,
    state: State<A>,
    blocks: Vec<DataBlock>,
    pub buffered_rows: usize,
    pub buffered_bytes: usize,
    spill: Option<SortSpill<A, S>>,
    output: Option<DataBlock>,
}

impl<A: SortAlgorithm, S: SortSpiller> MergeSorter<A, S> {
    pub fn new(base: Base<S>, output_rows: usize) -> Self {
        assert!(output_rows > 0);
        Self {
            base,
            output_rows,
            state: State::Collecting,
            blocks: Vec::new(),
            buffered_rows: 0,
            buffered_bytes: 0,
            spill: None,
            output: None,
        }
    }

    pub fn status(&self) -> MergeSortStatus {
        match self.state {
            State::Collecting => MergeSortStatus::Collecting,
            State::Spill { .. } | State::Restore | State::PrepareMerge | State::Memory(_) => {
                MergeSortStatus::Merge
            }
            State::Finished => MergeSortStatus::Finished,
        }
    }

    pub fn add_block(&mut self, block: DataBlock) {
        assert_eq!(self.status(), MergeSortStatus::Collecting);
        if !block.is_empty() {
            self.buffered_rows += block.num_rows();
            self.buffered_bytes += block.memory_size();
            self.blocks.push(block);
        }
    }

    /// Flush buffered runs to storage, then resume accepting input. Parameters
    /// are fixed by the first spill and reused for subsequent runs.
    pub fn start_spill(&mut self, params: SortSpillParams) {
        assert_eq!(self.status(), MergeSortStatus::Collecting);
        assert!(!self.blocks.is_empty());
        self.spill
            .get_or_insert_with(|| SortSpill::new(self.base.clone(), params));
        self.state = State::Spill {
            finish_input: false,
            write: true,
        };
    }

    /// End ingestion. If external runs exist, `spill_remaining` determines whether
    /// the final buffered run is written to storage or retained for the merge.
    pub fn finish_input(&mut self, spill_remaining: bool) {
        assert_eq!(self.status(), MergeSortStatus::Collecting);
        if self.spill.is_some() {
            self.state = if self.blocks.is_empty() {
                State::Restore
            } else {
                State::Spill {
                    finish_input: true,
                    write: spill_remaining,
                }
            };
        } else if self.blocks.len() > 1 {
            self.state = State::PrepareMerge;
        } else {
            let mut blocks = self.take_blocks();
            self.state = match blocks.len() {
                0 => State::Finished,
                1 => {
                    // The merge kernel requires at least two streams.
                    let block = blocks.pop().unwrap();
                    self.output = Some(match self.base.limit {
                        Some(limit) if limit < block.num_rows() => block.slice(0..limit),
                        _ => block,
                    });
                    State::Finished
                }
                _ => unreachable!(),
            };
        }
    }

    /// Take output before requesting more work or observing Finished.
    pub fn take_output(&mut self) -> Option<DataBlock> {
        self.output.take()
    }

    pub fn process(&mut self) -> Result<()> {
        assert!(self.output.is_none());
        if matches!(self.state, State::PrepareMerge) {
            // Allocate per-run merge state in synchronous work, not in event().
            let blocks = self.take_blocks();
            self.state = State::Memory(create_memory_merger::<A>(
                blocks,
                self.base.sort_row_offset,
                self.base.limit,
                self.output_rows,
            ));
        }
        match self.state {
            State::Spill {
                finish_input,
                write,
            } => {
                let blocks = self.take_blocks();
                log::debug!(
                    incoming_block = blocks.len(),
                    incoming_rows = blocks.iter().map(DataBlock::num_rows).sum::<usize>(),
                    total_rows = self.spill.as_ref().unwrap().collect_total_rows(),
                    finished = finish_input;
                    "sort_input_data"
                );
                self.spill
                    .as_mut()
                    .unwrap()
                    .sort_input_data(blocks, write)?;
                self.state = if finish_input {
                    State::Restore
                } else {
                    State::Collecting
                };
            }
            State::Restore => {
                let output = self
                    .spill
                    .as_mut()
                    .unwrap()
                    .on_restore(self.base.spiller.memory_settings())?;
                self.output = output.block;
                if output.finish {
                    self.spill = None;
                    self.state = State::Finished;
                }
            }
            State::Memory(ref mut merger) => {
                self.output = merger.next_block()?;
                if self.output.is_none() {
                    self.state = State::Finished;
                }
            }
            _ => unreachable!(),
        }
        Ok(())
    }

    /// Release retained input, output and merge state on downstream cancellation.
    pub fn clear(&mut self) {
        self.state = State::Finished;
        self.blocks.clear();
        self.buffered_rows = 0;
        self.buffered_bytes = 0;
        self.spill = None;
        self.output = None;
    }

    fn take_blocks(&mut self) -> Vec<DataBlock> {
        self.buffered_rows = 0;
        self.buffered_bytes = 0;
        std::mem::take(&mut self.blocks)
    }
}
