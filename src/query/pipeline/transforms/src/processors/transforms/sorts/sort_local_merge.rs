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

use std::any::Any;
use std::collections::VecDeque;
use std::hint::unlikely;
use std::sync::Arc;
use std::sync::atomic;
use std::sync::atomic::AtomicBool;

use bytesize::ByteSize;
use databend_common_exception::Result;
use databend_common_expression::DataBlock;
use databend_common_pipeline::core::Event;
use databend_common_pipeline::core::InputPort;
use databend_common_pipeline::core::OutputPort;
use databend_common_pipeline::core::Processor;

use super::Base;
use super::MemoryMerger;
use super::MergeSort;
use super::OutputData;
use super::RowsStat;
use super::SortSpill;
use super::SortSpillParams;
use super::TransformSortMergeLimit;
use super::core::RowConverter;
use super::core::Rows;
use super::core::algorithm::SortAlgorithm;
use super::create_memory_merger;
use crate::traits::SortSpiller;

#[derive(Debug)]
enum State {
    /// This state means the processor will collect incoming blocks.
    Collect,
    /// This state means the processor is sorting collected blocks.
    Sort,
    /// Finish the process.
    Finish,
}

#[allow(clippy::large_enum_variant)]
enum Inner<A: SortAlgorithm, S: SortSpiller> {
    Collect(Vec<DataBlock>),
    Limit(TransformSortMergeLimit<A::Rows>),
    Memory(MemoryMerger<A>),
    Spill(Vec<DataBlock>, SortSpill<A, S>),
}

pub struct TransformSort<A: SortAlgorithm, S: SortSpiller> {
    name: &'static str,
    input: Arc<InputPort>,
    output: Arc<OutputPort>,
    output_data: VecDeque<DataBlock>,
    state: State,

    row_converter: <A::Rows as Rows>::Converter,
    /// If the next transform of current transform is [`super::transform_multi_sort_merge::MultiSortMergeProcessor`],
    /// we can generate and output the order column to avoid the extra converting in the next transform.
    remove_order_col: bool,
    /// If this transform is after an Exchange transform,
    /// it means it will compact the data from cluster nodes.
    /// And the order column is already generated in each cluster node,
    /// so we don't need to generate the order column again.
    input_has_order_col: bool,

    base: Base<S>,
    inner: Inner<A, S>,

    aborting: AtomicBool,

    max_block_size: usize,
    enable_restore_prefetch: bool,
    enable_sort_spill_stream_regroup: bool,
}

impl<A, S> TransformSort<A, S>
where
    A: SortAlgorithm,
    S: SortSpiller,
{
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        input: Arc<InputPort>,
        output: Arc<OutputPort>,
        sort_row_offset: usize,
        row_converter: <A::Rows as Rows>::Converter,
        max_block_size: usize,
        limit: Option<(usize, bool)>,
        spiller: S,
        remove_order_col: bool,
        input_has_order_col: bool,
        enable_restore_prefetch: bool,
        enable_sort_spill_stream_regroup: bool,
    ) -> Result<Self> {
        assert!(max_block_size > 0);
        let (name, inner, limit) = match limit {
            Some((limit, true)) => (
                "TransformSortMergeLimit",
                Inner::Limit(TransformSortMergeLimit::create(max_block_size, limit)),
                Some(limit),
            ),
            Some((limit, false)) => ("TransformSortMerge", Inner::Collect(vec![]), Some(limit)),
            None => ("TransformSortMerge", Inner::Collect(vec![]), None),
        };
        Ok(Self {
            input,
            output,
            name,
            state: State::Collect,
            row_converter,
            output_data: VecDeque::new(),
            remove_order_col,
            input_has_order_col,
            base: Base {
                spiller,
                sort_row_offset,
                limit,
            },
            inner,
            max_block_size,
            aborting: AtomicBool::new(false),
            enable_restore_prefetch,
            enable_sort_spill_stream_regroup,
        })
    }

    fn limit_trans_to_spill(&mut self) -> Result<()> {
        let Inner::Limit(merger) = &self.inner else {
            unreachable!()
        };
        let params = self.determine_params(merger.num_bytes(), merger.num_rows());
        let Inner::Limit(merger) = &mut self.inner else {
            unreachable!()
        };
        let blocks = merger.prepare_spill(params.batch_rows)?;
        let spill_sort = SortSpill::new(self.base.clone(), params);
        self.inner = Inner::Spill(blocks, spill_sort);
        Ok(())
    }

    fn collect_trans_to_spill(&mut self, input_data: Vec<DataBlock>) {
        let (num_rows, num_bytes) = input_data
            .iter()
            .map(|block| (block.num_rows(), ByteSize(block.memory_size() as _)))
            .fold((0, ByteSize(0)), |(acc_rows, acc_bytes), (rows, bytes)| {
                (acc_rows + rows, acc_bytes + bytes)
            });
        let params = self.determine_params(num_bytes, num_rows);
        let spill_sort = SortSpill::new(self.base.clone(), params);
        self.inner = Inner::Spill(input_data, spill_sort);
    }

    fn trans_to_spill(&mut self) -> Result<()> {
        match &mut self.inner {
            Inner::Limit(_) => self.limit_trans_to_spill(),
            Inner::Collect(input_data) => {
                let input_data = std::mem::take(input_data);
                self.collect_trans_to_spill(input_data);
                Ok(())
            }
            Inner::Spill(_, _) => Ok(()),
            Inner::Memory(_) => unreachable!(),
        }
    }

    fn determine_params(&self, bytes: ByteSize, rows: usize) -> SortSpillParams {
        SortSpillParams::determine(
            bytes,
            rows,
            ByteSize(self.base.spiller.memory_settings().spill_unit_size as _),
            self.max_block_size,
            self.enable_restore_prefetch,
            self.enable_sort_spill_stream_regroup,
        )
    }

    fn collect_block(&mut self, block: DataBlock) -> Result<()> {
        if self.input_has_order_col {
            match &mut self.inner {
                Inner::Limit(limit_sort) => {
                    let rows = A::Rows::from_column(
                        &block.get_by_offset(self.base.sort_row_offset).to_column(),
                    )?;
                    limit_sort.add_block(block, rows)
                }
                Inner::Collect(input_data) | Inner::Spill(input_data, _) => {
                    input_data.push(block);
                    Ok(())
                }
                _ => unreachable!(),
            }
        } else {
            let rows = self.row_converter.convert(&block)?;
            let mut block = block;
            block.add_column(rows.to_column());
            match &mut self.inner {
                Inner::Limit(limit_sort) => limit_sort.add_block(block, rows),
                Inner::Collect(input_data) | Inner::Spill(input_data, _) => {
                    input_data.push(block);
                    Ok(())
                }
                _ => unreachable!(),
            }
        }
    }

    fn sort_block_sync(&mut self) -> Result<()> {
        match &mut self.inner {
            Inner::Limit(limit_sort) => {
                self.output_data.extend(limit_sort.on_finish(false)?);
                self.state = State::Finish;
            }
            Inner::Collect(input_data) => {
                let input_data = std::mem::take(input_data);
                if input_data.len() == 1 {
                    self.output_data.extend(input_data);
                    self.state = State::Finish;
                    return Ok(());
                }
                let mut merger = create_memory_merger::<A>(
                    input_data,
                    self.base.sort_row_offset,
                    self.base.limit,
                    self.max_block_size,
                );

                if let Some(block) = merger.next_block()? {
                    self.output_data.push_back(block);
                } else {
                    self.state = State::Finish
                }
                self.inner = Inner::Memory(merger)
            }
            Inner::Memory(merger) => {
                if let Some(block) = merger.next_block()? {
                    self.output_data.push_back(block);
                } else {
                    self.state = State::Finish
                }
            }
            _ => unreachable!(),
        }
        Ok(())
    }

    fn output_block(&self, mut block: DataBlock) {
        if self.remove_order_col {
            block.pop_columns(1);
        }
        self.output.push_data(Ok(block));
    }

    fn check_spill(&self) -> bool {
        let memory_settings = self.base.spiller.memory_settings();
        match &self.inner {
            Inner::Limit(limit_sort) => {
                memory_settings.check_spill()
                    && limit_sort.num_bytes()
                        >= ByteSize(memory_settings.spill_unit_size as _) * 2_u64
            }
            Inner::Collect(input_data) => {
                memory_settings.check_spill()
                    && input_data.iter().map(|b| b.memory_size()).sum::<usize>()
                        >= memory_settings.spill_unit_size * 2
            }
            Inner::Spill(input_data, sort_spill) => {
                let params = sort_spill.params();
                input_data.in_memory_rows() >= params.batch_rows * 2 && {
                    let remain = memory_settings.check_spill_remain().unwrap();
                    remain < memory_settings.spill_unit_size as isize * 2
                }
            }
            _ => unreachable!(),
        }
    }
}

#[async_trait::async_trait]
impl<A, S> Processor for TransformSort<A, S>
where
    A: SortAlgorithm + 'static,
    A::Rows: 'static,
    <A::Rows as Rows>::Converter: Send + 'static,
    S: SortSpiller,
{
    fn name(&self) -> String {
        self.name.to_string()
    }

    fn as_any(&mut self) -> &mut dyn Any {
        self
    }

    fn event(&mut self) -> Result<Event> {
        if self.output.is_finished() {
            self.input.finish();
            return Ok(Event::Finished);
        }

        if !self.output.can_push() {
            self.input.set_not_need_data();
            return Ok(Event::NeedConsume);
        }

        if let Some(block) = self.output_data.pop_front() {
            match self.state {
                State::Sort | State::Finish => {
                    self.output_block(block);
                    return Ok(Event::NeedConsume);
                }
                _ => unreachable!(),
            }
        }

        if matches!(self.state, State::Finish) {
            assert!(self.input.is_finished());
            self.output.finish();
            return Ok(Event::Finished);
        }

        if self.input.has_data() {
            return match self.state {
                State::Collect => {
                    if self.check_spill() {
                        // delay the handle of input until the next call.
                        Ok(Event::Async)
                    } else {
                        Ok(Event::Sync)
                    }
                }
                _ => unreachable!(),
            };
        }

        if self.input.is_finished() {
            return match &self.state {
                State::Finish => {
                    self.output.finish();
                    Ok(Event::Finished)
                }
                State::Collect => match &self.inner {
                    Inner::Limit(_) => {
                        self.state = State::Sort;
                        Ok(Event::Sync)
                    }
                    Inner::Collect(input_data) => {
                        if input_data.is_empty() {
                            self.state = State::Finish;
                            self.output.finish();
                            Ok(Event::Finished)
                        } else {
                            self.state = State::Sort;
                            Ok(Event::Sync)
                        }
                    }
                    Inner::Spill(input_data, _) => {
                        if input_data.is_empty() {
                            self.state = State::Sort;
                        }
                        Ok(Event::Async)
                    }
                    Inner::Memory(_) => unreachable!(),
                },
                State::Sort => match &self.inner {
                    Inner::Limit(_) | Inner::Memory(_) => Ok(Event::Sync),
                    Inner::Spill(_, _) => Ok(Event::Async),
                    _ => unreachable!(),
                },
            };
        }

        self.input.set_need_data();
        Ok(Event::NeedData)
    }

    fn process(&mut self) -> Result<()> {
        match self.state {
            State::Collect => {
                let block = self.input.pull_data().unwrap()?;
                self.input.set_need_data();
                if unlikely(block.is_empty()) {
                    return Ok(());
                }
                self.collect_block(block)
            }
            State::Sort => self.sort_block_sync(),
            State::Finish => unreachable!(),
        }
    }

    #[async_backtrace::framed]
    async fn async_process(&mut self) -> Result<()> {
        match &self.state {
            State::Collect => {
                let finished = self.input.is_finished();
                self.trans_to_spill()?;

                let Inner::Spill(input_data, spill_sort) = &mut self.inner else {
                    unreachable!()
                };
                let incoming = input_data.in_memory_rows();
                if incoming > 0 {
                    let incoming_block = input_data.len();
                    let total_rows = spill_sort.collect_total_rows();
                    log::debug!(incoming_block, incoming_rows = incoming, total_rows, finished; "sort_input_data");
                    spill_sort
                        .sort_input_data(std::mem::take(input_data), !finished, &self.aborting)
                        .await?;
                }
                if finished {
                    self.state = State::Sort;
                }
                Ok(())
            }
            State::Sort => {
                let Inner::Spill(input_data, spill_sort) = &mut self.inner else {
                    unreachable!()
                };
                assert!(input_data.is_empty());
                let OutputData { block, finish, .. } = spill_sort
                    .on_restore(self.base.spiller.memory_settings())
                    .await?;
                self.output_data.extend(block);
                if finish {
                    self.state = State::Finish
                }
                Ok(())
            }
            _ => unreachable!(),
        }
    }

    fn interrupt(&self) {
        self.aborting.store(true, atomic::Ordering::Release);
    }
}
