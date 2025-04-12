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
use std::sync::Arc;

use databend_common_exception::Result;
use databend_common_expression::BlockEntry;
use databend_common_expression::DataBlock;
use databend_common_expression::DataSchemaRef;
use databend_common_expression::SortColumnDescription;
use databend_common_expression::Value;
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
use sort_spill::TransformStreamSortSpill;

use crate::spillers::Spiller;

mod sort_spill;

mod builder;
pub use builder::TransformSortBuilder;

#[derive(Debug)]
enum State {
    /// This state means the processor will collect incoming blocks.
    Collect,
    /// This state means the processor is sorting collected blocks.
    Sort,
    /// Finish the process.
    Finish,
}

pub struct TransformSort<A: SortAlgorithm, C> {
    input: Arc<InputPort>,
    output: Arc<OutputPort>,

    state: State,

    row_converter: C,

    input_data: Vec<DataBlock>,
    output_data: VecDeque<DataBlock>,

    sort_desc: Arc<Vec<SortColumnDescription>>,

    /// If the next transform of current transform is [`super::transform_multi_sort_merge::MultiSortMergeProcessor`],
    /// we can generate and output the order column to avoid the extra converting in the next transform.
    output_order_col: bool,

    /// If this transform is after an Exchange transform,
    /// it means it will compact the data from cluster nodes.
    /// And the order column is already generated in each cluster node,
    /// so we don't need to generate the order column again.
    order_col_generated: bool,

    /// The index for the next input block.
    next_index: usize,

    spill_sort: Option<TransformStreamSortSpill<A>>,
    limit_sort: Option<TransformSortMergeLimit<A::Rows>>,

    // The spill_params will be passed to the spill processor.
    // If spill_params is Some, it means we need to spill.
    spill_params: Option<SortSpillParams>,

    memory_settings: MemorySettings,
}

impl<A, C> TransformSort<A, C>
where
    A: SortAlgorithm,
    C: RowConverter<A::Rows>,
{
    #[allow(clippy::too_many_arguments)]
    fn new(
        input: Arc<InputPort>,
        output: Arc<OutputPort>,
        schema: DataSchemaRef,
        sort_desc: Arc<Vec<SortColumnDescription>>,
        limit: Option<usize>,
        spiller: Arc<Spiller>,
        output_order_col: bool,
        limit_sort: Option<TransformSortMergeLimit<A::Rows>>,
        order_col_generated: bool,
        memory_settings: MemorySettings,
    ) -> Result<Self> {
        let sort_row_offset = schema.fields().len() - 1;
        let row_converter = C::create(&sort_desc, schema.clone())?;
        Ok(Self {
            input,
            output,
            state: State::Collect,
            row_converter,
            input_data: vec![],
            output_data: VecDeque::new(),
            sort_desc,
            output_order_col,
            order_col_generated,
            next_index: 0,
            spill_sort: Some(TransformStreamSortSpill::new(
                schema,
                limit,
                spiller,
                sort_row_offset,
            )),
            limit_sort,
            spill_params: None,
            memory_settings,
        })
    }

    fn generate_order_column(&self, mut block: DataBlock) -> Result<(A::Rows, DataBlock)> {
        let order_by_cols = self
            .sort_desc
            .iter()
            .map(|desc| block.get_by_offset(desc.offset).clone())
            .collect::<Vec<_>>();
        let rows = self
            .row_converter
            .convert(&order_by_cols, block.num_rows())?;
        let order_col = rows.to_column();
        block.add_column(BlockEntry {
            data_type: order_col.data_type(),
            value: Value::Column(order_col),
        });
        Ok((rows, block))
    }

    fn prepare_spill_limit(&mut self) -> Result<()> {
        let mut limit_merge = self.limit_sort.take().unwrap();
        let params = self.determine_params(limit_merge.num_bytes(), limit_merge.num_rows());
        self.spill_params = Some(params);
        let blocks = limit_merge.prepare_spill(params.batch_rows)?;
        self.input_data = blocks;
        Ok(())
    }

    fn prepare_spill(&mut self) {
        let (num_rows, num_bytes) = self
            .input_data
            .iter()
            .map(|block| (block.num_rows(), block.memory_size()))
            .fold((0, 0), |(acc_rows, acc_bytes), (rows, bytes)| {
                (acc_rows + rows, acc_bytes + bytes)
            });
        self.spill_params = Some(self.determine_params(num_bytes, num_rows));
    }

    fn determine_params(&self, bytes: usize, rows: usize) -> SortSpillParams {
        // We use the first memory calculation to estimate the batch size and the number of merge.
        let unit_size = self.memory_settings.spill_unit_size;
        let num_merge = bytes.div_ceil(unit_size).max(2);
        let batch_rows = rows.div_ceil(num_merge);
        log::info!("determine sort spill params, buffer_bytes: {bytes}, buffer_rows: {rows}, spill_unit_size: {unit_size}, batch_rows: {batch_rows}, batch_num_merge {num_merge}");
        SortSpillParams {
            batch_rows,
            num_merge,
        }
    }

    fn collect_block(&mut self, block: DataBlock) -> Result<()> {
        if self.order_col_generated {
            if let Some(limit_sort) = &mut self.limit_sort {
                let rows = A::Rows::from_column(block.get_last_column())?;
                limit_sort.add_block(block, rows, self.next_index)?;
                self.next_index += 1;
                return Ok(());
            }

            self.input_data.push(block);
            return Ok(());
        }

        let (rows, block) = self.generate_order_column(block)?;
        if let Some(limit_sort) = &mut self.limit_sort {
            limit_sort.add_block(block, rows, self.next_index)?;
            self.next_index += 1;
        } else {
            self.input_data.push(block);
        }
        Ok(())
    }

    fn output_block(&self, mut block: DataBlock) {
        if !self.output_order_col {
            block.pop_columns(1);
        }
        self.output.push_data(Ok(block));
    }

    fn input_rows(&self) -> usize {
        self.input_data.iter().map(|b| b.num_rows()).sum::<usize>()
    }

    fn check_spill(&self) -> bool {
        if !self.memory_settings.check_spill() {
            return false;
        }

        if self.spill_params.is_none()
            && let Some(limit_sort) = &self.limit_sort
        {
            return limit_sort.num_bytes() > self.memory_settings.spill_unit_size * 2;
        }

        match &self.spill_params {
            Some(params) => {
                self.input_data.iter().map(|b| b.num_rows()).sum::<usize>()
                    > params.batch_rows * params.num_merge
            }
            None => {
                self.input_data
                    .iter()
                    .map(|b| b.memory_size())
                    .sum::<usize>()
                    > self.memory_settings.spill_unit_size * 2
            }
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
        String::from("TransformSortMerge")
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
            match self.state {
                State::Finish => {
                    self.input.set_not_need_data();
                    return Ok(Event::NeedConsume);
                }
                State::Collect | State::Sort => {
                    // may should pull upstream?
                    self.input.set_not_need_data();
                    return Ok(Event::NeedConsume);
                }
            }
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
                State::Collect => {
                    if self.limit_sort.is_some() {
                        self.state = State::Sort;
                        Ok(Event::Sync)
                    } else {
                        if self.input_data.is_empty() {
                            self.state = State::Sort;
                        }
                        Ok(Event::Async)
                    }
                }
                State::Sort => Ok(Event::Async),
            };
        }

        self.input.set_need_data();
        Ok(Event::NeedData)
    }

    #[async_backtrace::framed]
    async fn async_process(&mut self) -> Result<()> {
        match &self.state {
            State::Collect => {
                if self.spill_params.is_none() {
                    if self.limit_sort.is_some() {
                        self.prepare_spill_limit()?;
                    } else {
                        self.prepare_spill();
                    }
                    let spill_sort = self.spill_sort.as_mut().unwrap();
                    spill_sort.init_spill(self.spill_params.unwrap());
                }
                debug_assert!(self.limit_sort.is_none());

                let input = self.input_rows();
                let spill_sort = self.spill_sort.as_mut().unwrap();
                let subsequent = spill_sort.subsequent_memory_rows();
                let max = spill_sort.max_rows();

                if subsequent > 0 && subsequent + input > max {
                    spill_sort
                        .subsequent_spill_last(subsequent + input - max)
                        .await?;
                }
                let finished = self.input.is_finished();
                if input > max || finished && input > 0 {
                    let input_data = std::mem::take(&mut self.input_data);
                    spill_sort.sort_input_data(input_data)?;
                }
                if finished {
                    self.state = State::Sort;
                }
                Ok(())
            }
            State::Sort => {
                debug_assert!(self.input_data.is_empty());
                let spill_sort = self.spill_sort.as_mut().unwrap();
                let (block, finish) = spill_sort.on_restore().await?;
                self.output_data.extend(block);
                if finish {
                    self.state = State::Finish
                }
                Ok(())
            }
            _ => unreachable!(),
        }
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
            State::Sort => {
                let limit_sort = self.limit_sort.as_mut().unwrap();
                self.output_data.extend(limit_sort.on_finish(false)?);
                self.state = State::Finish;
                Ok(())
            }
            State::Finish => unreachable!(),
        }
    }
}
