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
use std::sync::atomic;
use std::sync::atomic::AtomicBool;
use std::sync::Arc;

use bytesize::ByteSize;
use databend_common_exception::Result;
use databend_common_expression::DataBlock;
use databend_common_expression::SortColumnDescription;
use databend_common_pipeline_core::processors::Event;
use databend_common_pipeline_core::processors::InputPort;
use databend_common_pipeline_core::processors::OutputPort;
use databend_common_pipeline_core::processors::Processor;

use super::core::algorithm::SortAlgorithm;
use super::core::RowConverter;
use super::core::Rows;
use super::Base;
use super::MergeSort;
use super::RowsStat;
use super::SortSpill;
use super::SortSpillParams;
use super::TransformSortMergeLimit;
use crate::traits::DataBlockSpill;
use crate::MemorySettings;

#[allow(clippy::large_enum_variant)]
enum Inner<A: SortAlgorithm, S: DataBlockSpill> {
    Collect(Vec<DataBlock>),
    Limit(TransformSortMergeLimit<A::Rows>),
    Spill(Vec<DataBlock>, SortSpill<A, S>),
    None,
}

pub struct TransformSortCollect<A: SortAlgorithm, C, S: DataBlockSpill> {
    name: &'static str,
    input: Arc<InputPort>,
    output: Arc<OutputPort>,
    output_data: Option<DataBlock>,

    max_block_size: usize,
    default_num_merge: usize,
    row_converter: C,
    sort_desc: Arc<[SortColumnDescription]>,
    /// If this transform is after an Exchange transform,
    /// it means it will compact the data from cluster nodes.
    /// And the order column is already generated in each cluster node,
    /// so we don't need to generate the order column again.
    order_col_generated: bool,

    base: Base<S>,
    inner: Inner<A, S>,

    aborting: AtomicBool,

    memory_settings: MemorySettings,
}

impl<A, C, S> TransformSortCollect<A, C, S>
where
    A: SortAlgorithm,
    C: RowConverter<A::Rows>,
    S: DataBlockSpill,
{
    pub fn new(
        input: Arc<InputPort>,
        output: Arc<OutputPort>,
        base: Base<S>,
        sort_desc: Arc<[SortColumnDescription]>,
        max_block_size: usize,
        default_num_merge: usize,
        sort_limit: bool,
        order_col_generated: bool,
        memory_settings: MemorySettings,
    ) -> Result<Self> {
        let row_converter = C::create(&sort_desc, base.schema.clone())?;
        let (name, inner) = match base.limit {
            Some(limit) if sort_limit => (
                "TransformSortMergeCollectLimit",
                Inner::Limit(TransformSortMergeLimit::create(max_block_size, limit)),
            ),
            _ => ("TransformSortMergeCollect", Inner::Collect(vec![])),
        };
        Ok(Self {
            input,
            output,
            name,
            row_converter,
            output_data: None,
            sort_desc,
            order_col_generated,
            base,
            inner,
            aborting: AtomicBool::new(false),
            memory_settings,
            max_block_size,
            default_num_merge,
        })
    }

    fn generate_order_column(&self, mut block: DataBlock) -> Result<(A::Rows, DataBlock)> {
        let rows = self
            .row_converter
            .convert_data_block(&self.sort_desc, &block)?;
        block.add_column(rows.to_column());
        Ok((rows, block))
    }

    fn limit_trans_to_spill(&mut self, no_spill: bool) -> Result<()> {
        let Inner::Limit(merger) = &self.inner else {
            unreachable!()
        };
        assert!(merger.num_rows() > 0);
        let params = if no_spill {
            SortSpillParams {
                batch_rows: self.max_block_size,
                num_merge: self.default_num_merge,
            }
        } else {
            self.determine_params(merger.num_bytes(), merger.num_rows())
        };
        let Inner::Limit(merger) = &mut self.inner else {
            unreachable!()
        };
        let blocks = merger.prepare_spill(params.batch_rows)?;
        let spill_sort = SortSpill::new(self.base.clone(), params);
        self.inner = Inner::Spill(blocks, spill_sort);
        Ok(())
    }

    fn collect_trans_to_spill(&mut self, input_data: Vec<DataBlock>, no_spill: bool) {
        let (num_rows, num_bytes) = input_data
            .iter()
            .map(|block| (block.num_rows(), ByteSize(block.memory_size() as _)))
            .fold((0, ByteSize(0)), |(acc_rows, acc_bytes), (rows, bytes)| {
                (acc_rows + rows, acc_bytes + bytes)
            });
        assert!(num_rows > 0);
        let params = if no_spill {
            SortSpillParams {
                batch_rows: self.max_block_size,
                num_merge: self.default_num_merge,
            }
        } else {
            self.determine_params(num_bytes, num_rows)
        };
        let spill_sort = SortSpill::new(self.base.clone(), params);
        self.inner = Inner::Spill(input_data, spill_sort);
    }

    fn trans_to_spill(&mut self, no_spill: bool) -> Result<()> {
        match &mut self.inner {
            Inner::Limit(_) => self.limit_trans_to_spill(no_spill),
            Inner::Collect(input_data) => {
                let input_data = std::mem::take(input_data);
                self.collect_trans_to_spill(input_data, no_spill);
                Ok(())
            }
            Inner::Spill(_, _) => Ok(()),
            Inner::None => unreachable!(),
        }
    }

    fn determine_params(&self, bytes: ByteSize, rows: usize) -> SortSpillParams {
        SortSpillParams::determine(
            bytes,
            rows,
            ByteSize(self.memory_settings.spill_unit_size as _),
            self.max_block_size,
        )
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

    fn check_spill(&self) -> bool {
        match &self.inner {
            Inner::Limit(limit_sort) => {
                self.memory_settings.check_spill()
                    && limit_sort.num_bytes()
                        >= ByteSize(self.memory_settings.spill_unit_size as _) * 2_u64
            }
            Inner::Collect(input_data) => {
                self.memory_settings.check_spill()
                    && input_data.iter().map(|b| b.memory_size()).sum::<usize>()
                        >= self.memory_settings.spill_unit_size * 2
            }
            Inner::Spill(input_data, sort_spill) => {
                let rows = input_data.in_memory_rows();
                let params = sort_spill.params();
                self.memory_settings.check_spill() && rows >= params.batch_rows * 2
                    || input_data.in_memory_rows() >= params.max_rows()
            }
            _ => unreachable!(),
        }
    }

    fn create_output(&mut self) -> Result<()> {
        let Inner::Spill(input_data, spill_sort) = std::mem::replace(&mut self.inner, Inner::None)
        else {
            unreachable!()
        };
        assert!(input_data.is_empty());

        let meta = spill_sort.dump_collect()?;
        self.output_data = Some(DataBlock::empty_with_meta(Box::new(meta)));
        Ok(())
    }
}

#[async_trait::async_trait]
impl<A, C, S> Processor for TransformSortCollect<A, C, S>
where
    A: SortAlgorithm + 'static,
    A::Rows: 'static,
    C: RowConverter<A::Rows> + Send + 'static,
    S: DataBlockSpill,
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

        if let Some(block) = self.output_data.take() {
            assert!(self.input.is_finished());
            self.output.push_data(Ok(block));
            self.output.finish();
            return Ok(Event::Finished);
        }

        if self.input.has_data() {
            return if self.check_spill() {
                // delay the handle of input until the next call.
                Ok(Event::Async)
            } else {
                Ok(Event::Sync)
            };
        }

        if self.input.is_finished() {
            return match &self.inner {
                Inner::Limit(merger) => {
                    if merger.num_rows() == 0 {
                        self.output.finish();
                        Ok(Event::Finished)
                    } else {
                        Ok(Event::Async)
                    }
                }
                Inner::Collect(input_data) => {
                    if input_data.is_empty() {
                        self.output.finish();
                        Ok(Event::Finished)
                    } else {
                        Ok(Event::Async)
                    }
                }
                Inner::Spill(_, _) => Ok(Event::Async),
                Inner::None => unreachable!(),
            };
        }

        self.input.set_need_data();
        Ok(Event::NeedData)
    }

    fn process(&mut self) -> Result<()> {
        if let Some(block) = self.input.pull_data().transpose()? {
            self.input.set_need_data();
            if !block.is_empty() {
                self.collect_block(block)?;
            }
        }
        Ok(())
    }

    #[async_backtrace::framed]
    async fn async_process(&mut self) -> Result<()> {
        let finished = self.input.is_finished();
        self.trans_to_spill(finished)?;

        let Inner::Spill(input_data, spill_sort) = &mut self.inner else {
            unreachable!()
        };

        let incoming = input_data.in_memory_rows();
        let incoming_block = input_data.len();
        if incoming > 0 {
            let total_rows = spill_sort.collect_total_rows();
            log::debug!(incoming_block, incoming_rows = incoming, total_rows, finished; "sort_input_data");
            spill_sort
                .sort_input_data(std::mem::take(input_data), !finished, &self.aborting)
                .await?;
        }
        if finished {
            self.create_output()
        } else {
            Ok(())
        }
    }

    fn interrupt(&self) {
        self.aborting.store(true, atomic::Ordering::Release);
    }
}
