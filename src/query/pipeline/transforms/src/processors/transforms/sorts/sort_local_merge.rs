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

use bytesize::ByteSize;
use databend_common_exception::Result;
use databend_common_expression::DataBlock;
use databend_common_pipeline::core::Event;
use databend_common_pipeline::core::InputPort;
use databend_common_pipeline::core::OutputPort;
use databend_common_pipeline::core::Processor;

use super::Base;
use super::MergeSort;
use super::MergeSortStatus;
use super::MergeSorter;
use super::SortSpillParams;
use super::TransformSortMergeLimit;
use super::core::RowConverter;
use super::core::Rows;
use super::core::algorithm::SortAlgorithm;
use crate::traits::SortSpiller;

#[allow(clippy::large_enum_variant)]
enum Inner<A: SortAlgorithm, S: SortSpiller> {
    Limit(TransformSortMergeLimit<A::Rows>),
    Merge(MergeSorter<A, S>),
    Finished,
}

pub struct TransformSort<A: SortAlgorithm, S: SortSpiller> {
    name: &'static str,
    input: Arc<InputPort>,
    output: Arc<OutputPort>,
    output_data: VecDeque<DataBlock>,

    row_converter: <A::Rows as Rows>::Converter,
    // Preserve the comparison column when another merge stage consumes it.
    remove_order_col: bool,
    // Exchange inputs may already carry the encoded comparison column.
    input_has_order_col: bool,

    base: Base<S>,
    inner: Inner<A, S>,
    spill_params: Option<SortSpillParams>,
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
        let base = Base {
            spiller,
            sort_row_offset,
            limit: limit.map(|(limit, _)| limit),
        };
        let (name, inner) = match limit {
            Some((limit, true)) => (
                "TransformSortMergeLimit",
                Inner::Limit(TransformSortMergeLimit::create(max_block_size, limit)),
            ),
            _ => (
                "TransformSortMerge",
                Inner::Merge(MergeSorter::new(base.clone(), max_block_size)),
            ),
        };
        Ok(Self {
            input,
            output,
            name,
            row_converter,
            output_data: VecDeque::new(),
            remove_order_col,
            input_has_order_col,
            base,
            inner,
            spill_params: None,
            max_block_size,
            enable_restore_prefetch,
            enable_sort_spill_stream_regroup,
        })
    }

    fn determine_params(&self) -> SortSpillParams {
        let (bytes, rows) = match &self.inner {
            Inner::Limit(sort) => (sort.num_bytes(), sort.num_rows()),
            Inner::Merge(sorter) => (ByteSize(sorter.buffered_bytes as u64), sorter.buffered_rows),
            Inner::Finished => unreachable!(),
        };
        SortSpillParams::determine(
            bytes,
            rows,
            ByteSize(self.base.spiller.memory_settings().spill_unit_size as u64),
            self.enable_restore_prefetch,
            self.enable_sort_spill_stream_regroup,
        )
    }

    fn collect_block(&mut self, mut block: DataBlock) -> Result<()> {
        let rows = if self.input_has_order_col {
            if matches!(self.inner, Inner::Limit(_)) {
                Some(A::Rows::from_column(
                    &block.get_by_offset(self.base.sort_row_offset).to_column(),
                )?)
            } else {
                None
            }
        } else {
            let rows = self.row_converter.convert(&block)?;
            block.add_column(rows.to_column());
            Some(rows)
        };
        match &mut self.inner {
            Inner::Limit(limit_sort) => limit_sort.add_block(block, rows.unwrap()),
            Inner::Merge(sorter) => {
                sorter.add_block(block);
                Ok(())
            }
            Inner::Finished => unreachable!(),
        }
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
                        >= ByteSize(memory_settings.spill_unit_size as u64) * 2_u64
            }
            Inner::Merge(sorter) => {
                if let Some(params) = self.spill_params {
                    sorter.buffered_rows >= params.batch_rows * 2 && {
                        let remain = memory_settings.check_spill_remain().unwrap();
                        remain < memory_settings.spill_unit_size as isize * 2
                    }
                } else {
                    memory_settings.check_spill()
                        && sorter.buffered_bytes >= memory_settings.spill_unit_size * 2
                }
            }
            Inner::Finished => unreachable!(),
        }
    }
}

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
            self.output_data.clear();
            self.inner = Inner::Finished;
            return Ok(Event::Finished);
        }

        if !self.output.can_push() {
            self.input.set_not_need_data();
            return Ok(Event::NeedConsume);
        }

        if let Some(block) = self
            .output_data
            .pop_front()
            .or_else(|| match &mut self.inner {
                Inner::Merge(sorter) => sorter.take_output(),
                _ => None,
            })
        {
            self.output_block(block);
            return Ok(Event::NeedConsume);
        }

        if let Inner::Merge(sorter) = &self.inner {
            match sorter.status() {
                MergeSortStatus::Collecting => (),
                MergeSortStatus::Merge => return Ok(Event::Sync),
                MergeSortStatus::Finished => self.inner = Inner::Finished,
            }
        }

        match self.inner {
            Inner::Finished => {
                self.output.finish();
                Ok(Event::Finished)
            }
            _ if self.input.has_data() || self.input.is_finished() => Ok(Event::Sync),
            _ => {
                self.input.set_need_data();
                Ok(Event::NeedData)
            }
        }
    }

    fn process(&mut self) -> Result<()> {
        match &mut self.inner {
            Inner::Merge(sorter) => match sorter.status() {
                MergeSortStatus::Collecting => (),
                MergeSortStatus::Merge => return sorter.process(),
                MergeSortStatus::Finished => unreachable!(),
            },
            Inner::Limit(_) => (),
            Inner::Finished => unreachable!(),
        }

        if self.input.has_data() && self.check_spill() {
            let params = self.spill_params.unwrap_or_else(|| self.determine_params());
            if let Inner::Limit(limit_sort) = &mut self.inner {
                let blocks = limit_sort.prepare_spill(params.batch_rows)?;
                let mut sorter = MergeSorter::new(self.base.clone(), self.max_block_size);
                for block in blocks {
                    sorter.add_block(block);
                }
                self.inner = Inner::Merge(sorter);
            }
            self.spill_params = Some(params);
            let Inner::Merge(sorter) = &mut self.inner else {
                unreachable!()
            };
            sorter.start_spill(params);
            return sorter.process();
        }

        if self.input.has_data() {
            let block = self.input.pull_data().unwrap()?;
            self.input.set_need_data();
            if unlikely(block.is_empty()) {
                return Ok(());
            }
            return self.collect_block(block);
        }

        match &mut self.inner {
            Inner::Limit(limit_sort) => {
                self.output_data.extend(limit_sort.on_finish(false)?);
                self.inner = Inner::Finished;
            }
            Inner::Merge(sorter) => sorter.finish_input(false),
            Inner::Finished => unreachable!(),
        }
        Ok(())
    }
}
