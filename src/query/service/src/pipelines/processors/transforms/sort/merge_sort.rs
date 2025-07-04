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
use std::intrinsics::unlikely;
use std::sync::atomic;
use std::sync::atomic::AtomicBool;
use std::sync::Arc;

use databend_common_exception::Result;
use databend_common_expression::DataBlock;
use databend_common_expression::DataSchemaRef;
use databend_common_expression::SortColumnDescription;
use databend_common_pipeline_core::processors::Event;
use databend_common_pipeline_core::processors::InputPort;
use databend_common_pipeline_core::processors::OutputPort;
use databend_common_pipeline_core::processors::Processor;
use databend_common_pipeline_transforms::processors::sort::algorithm::SortAlgorithm;
use databend_common_pipeline_transforms::sort::RowConverter;
use databend_common_pipeline_transforms::sort::Rows;
use databend_common_pipeline_transforms::MemorySettings;
use databend_common_pipeline_transforms::MergeSort;
use databend_common_pipeline_transforms::SortSpillParams;
use databend_common_pipeline_transforms::TransformSortMergeLimit;

use super::sort_spill::create_memory_merger;
use super::sort_spill::MemoryMerger;
use super::sort_spill::SortSpill;
use super::Base;
use super::MemoryRows;
use crate::pipelines::processors::transforms::sort::sort_spill::OutputData;
use crate::spillers::Spiller;

#[derive(Debug)]
enum State {
    /// This state means the processor will collect incoming blocks.
    Collect,
    /// This state means the processor is sorting collected blocks.
    Sort,
    /// Finish the process.
    Finish,
}

enum Inner<A: SortAlgorithm> {
    Collect(Vec<DataBlock>),
    Limit(TransformSortMergeLimit<A::Rows>),
    Memory(MemoryMerger<A>),
    Spill(Vec<DataBlock>, SortSpill<A>),
}

pub struct TransformSort<A: SortAlgorithm, C> {
    name: &'static str,
    input: Arc<InputPort>,
    output: Arc<OutputPort>,
    output_data: VecDeque<DataBlock>,
    state: State,

    row_converter: C,
    sort_desc: Arc<[SortColumnDescription]>,
    /// If the next transform of current transform is [`super::transform_multi_sort_merge::MultiSortMergeProcessor`],
    /// we can generate and output the order column to avoid the extra converting in the next transform.
    remove_order_col: bool,
    /// If this transform is after an Exchange transform,
    /// it means it will compact the data from cluster nodes.
    /// And the order column is already generated in each cluster node,
    /// so we don't need to generate the order column again.
    order_col_generated: bool,

    base: Base,
    inner: Inner<A>,

    aborting: AtomicBool,

    max_block_size: usize,
    memory_settings: MemorySettings,
}

impl<A, C> TransformSort<A, C>
where
    A: SortAlgorithm,
    C: RowConverter<A::Rows>,
{
    pub(super) fn new(
        input: Arc<InputPort>,
        output: Arc<OutputPort>,
        schema: DataSchemaRef,
        sort_desc: Arc<[SortColumnDescription]>,
        max_block_size: usize,
        limit: Option<(usize, bool)>,
        spiller: Arc<Spiller>,
        output_order_col: bool,
        order_col_generated: bool,
        memory_settings: MemorySettings,
    ) -> Result<Self> {
        let sort_row_offset = schema.fields().len() - 1;
        let row_converter = C::create(&sort_desc, schema.clone())?;
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
            sort_desc,
            remove_order_col: !output_order_col,
            order_col_generated,
            base: Base {
                schema,
                spiller,
                sort_row_offset,
                limit,
            },
            inner,
            max_block_size,
            aborting: AtomicBool::new(false),
            memory_settings,
        })
    }

    fn generate_order_column(&self, mut block: DataBlock) -> Result<(A::Rows, DataBlock)> {
        let rows = self
            .row_converter
            .convert_data_block(&self.sort_desc, &block)?;
        let order_col = rows.to_column();
        block.add_column(order_col);
        Ok((rows, block))
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
            .map(|block| (block.num_rows(), block.memory_size()))
            .fold((0, 0), |(acc_rows, acc_bytes), (rows, bytes)| {
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

    fn determine_params(&self, bytes: usize, rows: usize) -> SortSpillParams {
        // We use the first memory calculation to estimate the batch size and the number of merge.
        let unit_size = self.memory_settings.spill_unit_size;
        let num_merge = bytes.div_ceil(unit_size).max(2);
        let batch_rows = rows.div_ceil(num_merge);

        /// The memory will be doubled during merging.
        const MERGE_RATIO: usize = 2;
        let num_merge = num_merge.div_ceil(MERGE_RATIO).max(2);
        log::info!("determine sort spill params, buffer_bytes: {bytes}, buffer_rows: {rows}, spill_unit_size: {unit_size}, batch_rows: {batch_rows}, batch_num_merge {num_merge}");
        SortSpillParams {
            batch_rows,
            num_merge,
        }
    }

    fn collect_block(&mut self, block: DataBlock) -> Result<()> {
        if self.order_col_generated {
            return match &mut self.inner {
                Inner::Limit(limit_sort) => {
                    let rows = A::Rows::from_column(block.get_last_column())?;
                    limit_sort.add_block(block, rows)
                }
                Inner::Collect(input_data) | Inner::Spill(input_data, _) => {
                    input_data.push(block);
                    Ok(())
                }
                _ => unreachable!(),
            };
        }

        let (rows, block) = self.generate_order_column(block)?;
        match &mut self.inner {
            Inner::Limit(limit_sort) => limit_sort.add_block(block, rows),
            Inner::Collect(input_data) | Inner::Spill(input_data, _) => {
                input_data.push(block);
                Ok(())
            }
            _ => unreachable!(),
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
                    self.base.schema.clone(),
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

    fn input_rows(&self) -> usize {
        match &self.inner {
            Inner::Collect(input_data) | Inner::Spill(input_data, _) => input_data.in_memory_rows(),
            _ => 0,
        }
    }

    fn check_spill(&self) -> bool {
        if !self.memory_settings.check_spill() {
            return false;
        }

        match &self.inner {
            Inner::Limit(limit_sort) => {
                limit_sort.num_bytes() > self.memory_settings.spill_unit_size * 2
            }
            Inner::Collect(input_data) => {
                input_data.iter().map(|b| b.memory_size()).sum::<usize>()
                    > self.memory_settings.spill_unit_size * 2
            }
            Inner::Spill(input_data, sort_spill) => {
                input_data.in_memory_rows() > sort_spill.max_rows()
            }
            _ => unreachable!(),
        }
    }
}

#[async_trait::async_trait]
impl<A, C> Processor for TransformSort<A, C>
where
    A: SortAlgorithm + 'static,
    A::Rows: 'static,
    C: RowConverter<A::Rows> + Send + 'static,
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

                let input = self.input_rows();
                let Inner::Spill(input_data, spill_sort) = &mut self.inner else {
                    unreachable!()
                };
                let memory_rows = spill_sort.collect_memory_rows();
                let max = spill_sort.max_rows();

                if memory_rows > 0 && memory_rows + input > max {
                    spill_sort
                        .collect_spill_last(memory_rows + input - max)
                        .await?;
                }
                if input > max || finished && input > 0 {
                    spill_sort
                        .sort_input_data(std::mem::take(input_data), &self.aborting)
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
                let OutputData { block, finish, .. } = spill_sort.on_restore().await?;
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
