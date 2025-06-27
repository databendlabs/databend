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

use databend_common_exception::ErrorCode;
use databend_common_exception::Result;
use databend_common_expression::types::binary::BinaryColumn;
use databend_common_expression::types::DataType;
use databend_common_expression::types::DateType;
use databend_common_expression::types::NumberDataType;
use databend_common_expression::types::NumberType;
use databend_common_expression::types::StringType;
use databend_common_expression::types::TimestampType;
use databend_common_expression::with_number_mapped_type;
use databend_common_expression::Column;
use databend_common_expression::DataBlock;
use databend_common_expression::DataSchemaRef;
use databend_common_expression::SortColumnDescription;
use databend_common_pipeline_core::processors::Event;
use databend_common_pipeline_core::processors::InputPort;
use databend_common_pipeline_core::processors::OutputPort;
use databend_common_pipeline_core::processors::Processor;
use databend_common_pipeline_core::processors::ProcessorPtr;
use databend_common_pipeline_core::Pipe;
use databend_common_pipeline_core::PipeItem;
use databend_common_pipeline_core::Pipeline;

use super::sort::algorithm::HeapSort;
use super::sort::algorithm::LoserTreeSort;
use super::sort::algorithm::SortAlgorithm;
use super::sort::utils::ORDER_COL_NAME;
use super::sort::Merger;
use super::sort::SimpleRowsAsc;
use super::sort::SimpleRowsDesc;
use super::sort::SortedStream;

pub fn try_add_multi_sort_merge(
    pipeline: &mut Pipeline,
    schema: DataSchemaRef,
    block_size: usize,
    limit: Option<usize>,
    sort_columns_descriptions: Arc<[SortColumnDescription]>,
    remove_order_col: bool,
    enable_loser_tree: bool,
) -> Result<()> {
    debug_assert!(if !remove_order_col {
        schema.has_field(ORDER_COL_NAME)
    } else {
        !schema.has_field(ORDER_COL_NAME)
    });

    if pipeline.is_empty() {
        return Err(ErrorCode::Internal("Cannot resize empty pipe."));
    }

    match pipeline.output_len() {
        0 => Err(ErrorCode::Internal("Cannot resize empty pipe.")),
        1 => Ok(()),
        last_pipe_size => {
            let mut inputs_port = Vec::with_capacity(last_pipe_size);
            for _ in 0..last_pipe_size {
                inputs_port.push(InputPort::create());
            }
            let output_port = OutputPort::create();

            let processor = ProcessorPtr::create(create_processor(
                inputs_port.clone(),
                output_port.clone(),
                schema,
                block_size,
                limit,
                sort_columns_descriptions,
                remove_order_col,
                enable_loser_tree,
            )?);

            pipeline.add_pipe(Pipe::create(inputs_port.len(), 1, vec![PipeItem::create(
                processor,
                inputs_port,
                vec![output_port],
            )]));
            Ok(())
        }
    }
}

fn create_processor(
    inputs: Vec<Arc<InputPort>>,
    output: Arc<OutputPort>,
    schema: DataSchemaRef,
    block_size: usize,
    limit: Option<usize>,
    sort_desc: Arc<[SortColumnDescription]>,
    remove_order_col: bool,
    enable_loser_tree: bool,
) -> Result<Box<dyn Processor>> {
    macro_rules! create {
        ($algo:ident, $rows:ty) => {
            MultiSortMergeProcessor::<$algo<$rows>>::create(
                inputs,
                output,
                schema,
                block_size,
                limit,
                remove_order_col,
            )?
        };
    }

    macro_rules! create_algo {
        ($rows:ty) => {
            if enable_loser_tree {
                Box::new(create!(LoserTreeSort, $rows))
            } else {
                Box::new(create!(HeapSort, $rows))
            }
        };
    }

    Ok(if sort_desc.len() == 1 {
        let sort_type = schema.field(sort_desc[0].offset).data_type();
        let asc = sort_desc[0].asc;
        match (sort_type, asc) {
            (DataType::Date, true) => create_algo!(SimpleRowsAsc<DateType>),
            (DataType::Timestamp, true) => create_algo!(SimpleRowsAsc<TimestampType>),
            (DataType::String, true) => create_algo!(SimpleRowsAsc<StringType>),
            (DataType::Number(num_ty), true) => {
                with_number_mapped_type!(|NUM_TYPE| match num_ty {
                    NumberDataType::NUM_TYPE => create_algo!(SimpleRowsAsc<NumberType<NUM_TYPE>>),
                })
            }

            (DataType::Date, false) => create_algo!(SimpleRowsDesc<DateType>),
            (DataType::Timestamp, false) => create_algo!(SimpleRowsDesc<TimestampType>),
            (DataType::String, false) => create_algo!(SimpleRowsDesc<StringType>),
            (DataType::Number(num_ty), false) => {
                with_number_mapped_type!(|NUM_TYPE| match num_ty {
                    NumberDataType::NUM_TYPE => create_algo!(SimpleRowsDesc<NumberType<NUM_TYPE>>),
                })
            }

            _ => create_algo!(BinaryColumn),
        }
    } else {
        create_algo!(BinaryColumn)
    })
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

impl<A> MultiSortMergeProcessor<A>
where A: SortAlgorithm
{
    pub fn create(
        inputs: Vec<Arc<InputPort>>,
        output: Arc<OutputPort>,
        schema: DataSchemaRef,
        block_size: usize,
        limit: Option<usize>,
        remove_order_col: bool,
    ) -> Result<Self> {
        let streams = inputs
            .iter()
            .map(|i| InputBlockStream::new(i.clone(), remove_order_col))
            .collect::<Vec<_>>();
        let merger = Merger::<A, _>::create(schema, streams, block_size, limit);
        Ok(Self {
            merger,
            inputs,
            output,
            output_data: VecDeque::new(),
        })
    }
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
