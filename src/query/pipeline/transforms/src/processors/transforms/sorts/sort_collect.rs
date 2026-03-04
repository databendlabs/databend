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
use super::MergeSort;
use super::RowsStat;
use super::SortSpill;
use super::SortSpillParams;
use super::TransformSortMergeLimit;
use super::core::RowConverter;
use super::core::Rows;
use super::core::algorithm::SortAlgorithm;
use crate::traits::SortSpiller;

#[allow(clippy::large_enum_variant)]
enum Inner<A: SortAlgorithm, S: SortSpiller> {
    Collect(Vec<DataBlock>),
    Limit(TransformSortMergeLimit<A::Rows>),
    Spill(Vec<DataBlock>, SortSpill<A, S>),
    None,
}

pub struct TransformSortCollect<A: SortAlgorithm, S: SortSpiller> {
    name: &'static str,
    input: Arc<InputPort>,
    output: Arc<OutputPort>,
    output_data: Option<DataBlock>,

    max_block_size: usize,
    default_num_merge: usize,
    order_col_converter: Option<<A::Rows as Rows>::Converter>,

    base: Base<S>,
    inner: Inner<A, S>,

    aborting: AtomicBool,

    enable_restore_prefetch: bool,
}

impl<A, S> TransformSortCollect<A, S>
where
    A: SortAlgorithm,
    S: SortSpiller,
{
    pub fn new(
        input: Arc<InputPort>,
        output: Arc<OutputPort>,
        base: Base<S>,
        max_block_size: usize,
        default_num_merge: usize,
        sort_limit: bool,
        order_col_converter: Option<<A::Rows as Rows>::Converter>,
        enable_restore_prefetch: bool,
    ) -> Result<Self> {
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
            output_data: None,
            order_col_converter,
            base,
            inner,
            aborting: AtomicBool::new(false),
            max_block_size,
            default_num_merge,
            enable_restore_prefetch,
        })
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
                prefetch: false,
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
                prefetch: false,
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
            ByteSize(self.base.spiller.memory_settings().spill_unit_size as _),
            self.max_block_size,
            self.enable_restore_prefetch,
        )
    }

    fn collect_block(&mut self, mut block: DataBlock) -> Result<()> {
        match &self.order_col_converter {
            Some(converter) => {
                let rows = converter.convert(&block)?;
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
            None => match &mut self.inner {
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
            },
        }
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
                input_data.in_memory_rows() >= sort_spill.params().batch_rows * 2 && {
                    let remain = memory_settings.check_spill_remain().unwrap();
                    remain < memory_settings.spill_unit_size as isize * 2
                }
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
impl<A, S> Processor for TransformSortCollect<A, S>
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
