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
use std::sync::Arc;

use databend_common_exception::Result;
use databend_common_expression::Column;
use databend_common_expression::DataBlock;
use databend_common_expression::DataSchemaRef;
use databend_common_expression::SortColumnDescription;
use databend_common_pipeline::core::Event;
use databend_common_pipeline::core::InputPort;
use databend_common_pipeline::core::OutputPort;
use databend_common_pipeline::core::Pipe;
use databend_common_pipeline::core::PipeItem;
use databend_common_pipeline::core::Pipeline;
use databend_common_pipeline::core::Processor;
use databend_common_pipeline::core::ProcessorPtr;

use super::core::Merger;
use super::core::RowConverter;
use super::core::Rows;
use super::core::RowsTypeVisitor;
use super::core::SortedStream;
use super::core::algorithm::HeapSort;
use super::core::algorithm::LoserTreeSort;
use super::core::algorithm::SortAlgorithm;
use super::core::select_row_type;
use super::utils::ORDER_COL_NAME;

pub fn try_add_multi_sort_merge(
    pipeline: &mut Pipeline,
    schema: DataSchemaRef,
    block_size: usize,
    limit: Option<usize>,
    sort_desc: &[SortColumnDescription],
    remove_order_col: bool,
    enable_loser_tree: bool,
    enable_fixed_rows_sort: bool,
) -> Result<()> {
    debug_assert!(remove_order_col != schema.has_field(ORDER_COL_NAME));

    match pipeline.output_len() {
        0 => panic!("Cannot resize empty pipe."),
        1 => Ok(()),
        last_pipe_size => {
            let mut inputs_port = Vec::with_capacity(last_pipe_size);
            for _ in 0..last_pipe_size {
                inputs_port.push(InputPort::create());
            }
            let output_port = OutputPort::create();

            let mut builder = MultiSortMergeBuilder {
                inputs: inputs_port.clone(),
                output: output_port.clone(),
                schema,
                block_size,
                limit,
                sort_desc,
                remove_order_col,
                enable_loser_tree,
            };
            pipeline.add_pipe(Pipe::create(inputs_port.len(), 1, vec![PipeItem::create(
                ProcessorPtr::create(select_row_type(&mut builder, enable_fixed_rows_sort)?),
                inputs_port,
                vec![output_port],
            )]));
            Ok(())
        }
    }
}

struct MultiSortMergeBuilder<'a> {
    inputs: Vec<Arc<InputPort>>,
    output: Arc<OutputPort>,
    schema: DataSchemaRef,
    block_size: usize,
    limit: Option<usize>,
    sort_desc: &'a [SortColumnDescription],
    remove_order_col: bool,
    enable_loser_tree: bool,
}

impl RowsTypeVisitor for MultiSortMergeBuilder<'_> {
    type Result = Result<Box<dyn Processor>>;

    fn schema(&self) -> DataSchemaRef {
        self.schema.clone()
    }

    fn sort_desc(&self) -> &[SortColumnDescription] {
        self.sort_desc
    }

    fn visit_type<R, C>(&mut self) -> Self::Result
    where
        R: Rows + 'static,
        C: RowConverter<R> + Send + 'static,
    {
        if self.enable_loser_tree {
            self.create_processor::<LoserTreeSort<R>>()
        } else {
            self.create_processor::<HeapSort<R>>()
        }
    }
}

impl MultiSortMergeBuilder<'_> {
    fn create_processor<A>(&self) -> Result<Box<dyn Processor>>
    where A: SortAlgorithm + 'static {
        let streams = self
            .inputs
            .iter()
            .map(|i| InputBlockStream::new(i.clone(), self.remove_order_col))
            .collect::<Vec<_>>();
        let merger =
            Merger::<A, _>::create(self.schema.clone(), streams, self.block_size, self.limit);

        Ok(Box::new(MultiSortMergeProcessor {
            merger,
            inputs: self.inputs.clone(),
            output: self.output.clone(),
            output_data: VecDeque::new(),
        }))
    }
}

pub struct InputBlockStream {
    input: Arc<InputPort>,
    remove_order_col: bool,
}

impl InputBlockStream {
    pub fn new(input: Arc<InputPort>, remove_order_col: bool) -> Self {
        Self {
            input,
            remove_order_col,
        }
    }
}

impl SortedStream for InputBlockStream {
    fn next(&mut self) -> Result<(Option<(DataBlock, Column)>, bool)> {
        if self.input.has_data() {
            let mut block = self.input.pull_data().unwrap()?;
            let col = block.get_last_column().clone();
            if self.remove_order_col {
                block.pop_columns(1);
            }
            self.input.set_need_data();
            Ok((Some((block, col)), false))
        } else if self.input.is_finished() {
            Ok((None, false))
        } else {
            self.input.set_need_data();
            Ok((None, true))
        }
    }
}

/// TransformMultiSortMerge is a processor with multiple input ports;
pub struct MultiSortMergeProcessor<A>
where A: SortAlgorithm
{
    merger: Merger<A, InputBlockStream>,

    /// This field is used to drive the processor's state.
    ///
    /// There is a copy of this fields in `self.merger` and it will pull data from it.
    inputs: Vec<Arc<InputPort>>,
    output: Arc<OutputPort>,

    output_data: VecDeque<DataBlock>,
}

impl<A> Processor for MultiSortMergeProcessor<A>
where A: SortAlgorithm + 'static
{
    fn name(&self) -> String {
        "MultiSortMerge".to_string()
    }

    fn as_any(&mut self) -> &mut dyn Any {
        self
    }

    fn event(&mut self) -> Result<Event> {
        if self.output.is_finished() {
            for input in self.inputs.iter() {
                input.finish();
            }
            return Ok(Event::Finished);
        }

        if !self.output.can_push() {
            return Ok(Event::NeedConsume);
        }

        if let Some(block) = self.output_data.pop_front() {
            self.output.push_data(Ok(block));
            return Ok(Event::NeedConsume);
        }

        if self.merger.is_finished() {
            self.output.finish();
            for input in self.inputs.iter() {
                input.finish();
            }
            return Ok(Event::Finished);
        }

        self.merger.poll_pending_stream()?;

        if self.merger.has_pending_stream() {
            Ok(Event::NeedData)
        } else {
            Ok(Event::Sync)
        }
    }

    fn process(&mut self) -> Result<()> {
        while let Some(block) = self.merger.next_block()? {
            self.output_data.push_back(block);
        }
        Ok(())
    }
}
