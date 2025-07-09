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
use std::marker::PhantomData;
use std::sync::Arc;

use databend_common_exception::Result;
use databend_common_expression::BlockMetaInfoDowncast;
use databend_common_expression::Column;
use databend_common_expression::DataBlock;
use databend_common_expression::DataSchemaRef;
use databend_common_pipeline_core::processors::Event;
use databend_common_pipeline_core::processors::InputPort;
use databend_common_pipeline_core::processors::OutputPort;
use databend_common_pipeline_core::processors::Processor;
use databend_common_pipeline_transforms::sort::algorithm::SortAlgorithm;
use databend_common_pipeline_transforms::sort::Merger;
use databend_common_pipeline_transforms::sort::Rows;
use databend_common_pipeline_transforms::sort::SortedStream;

use crate::pipelines::processors::transforms::SortBound;

type Stream<A> = BoundedInputStream<<A as SortAlgorithm>::Rows>;

pub struct BoundedMultiSortMergeProcessor<A>
where A: SortAlgorithm
{
    inputs: Vec<Arc<InputPort>>,
    output: Arc<OutputPort>,
    schema: DataSchemaRef,
    block_size: usize,
    limit: Option<usize>,

    output_data: Option<DataBlock>,
    bound_index: u32,
    inner: std::result::Result<Merger<A, Stream<A>>, Vec<Stream<A>>>,
}

impl<A> BoundedMultiSortMergeProcessor<A>
where A: SortAlgorithm
{
    pub fn new(
        inputs: Vec<Arc<InputPort>>,
        output: Arc<OutputPort>,
        schema: DataSchemaRef,
        block_size: usize,
        limit: Option<usize>,
        remove_order_col: bool,
    ) -> Result<Self> {
        let streams = inputs
            .iter()
            .map(|input| BoundedInputStream {
                data: None,
                input: input.clone(),
                remove_order_col,
                bound_index: None,
                sort_row_offset: schema.fields().len() - 1,
                _r: PhantomData,
            })
            .collect();

        Ok(Self {
            inputs,
            output,
            schema,
            block_size,
            limit,
            output_data: None,
            bound_index: u32::MAX,
            inner: Err(streams),
        })
    }
}

impl<A> Processor for BoundedMultiSortMergeProcessor<A>
where A: SortAlgorithm + 'static
{
    fn name(&self) -> String {
        "BoundedMultiSortMerge".to_string()
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

        if let Some(block) = self.output_data.take() {
            self.output.push_data(Ok(block));
            return Ok(Event::NeedConsume);
        }

        self.next_event()
    }

    fn process(&mut self) -> Result<()> {
        if let Some(block) = self.inner.as_mut().ok().unwrap().next_block()? {
            self.output_data = Some(block.add_meta(Some(SortBound::create(self.bound_index)))?);
        };
        Ok(())
    }
}

impl<A> BoundedMultiSortMergeProcessor<A>
where A: SortAlgorithm + 'static
{
    fn next_event(&mut self) -> Result<Event> {
        let streams = match &mut self.inner {
            inner @ Ok(_) => {
                let merger = inner.as_ref().ok().unwrap();
                if !merger.is_finished() {
                    return Ok(Event::Sync);
                }
                let merger = std::mem::replace(inner, Err(vec![])).ok().unwrap();
                self.inner = Err(merger.streams());
                self.inner.as_mut().err().unwrap()
            }
            Err(streams) => streams,
        };

        let mut bounds = Vec::with_capacity(streams.len());
        for stream in streams.iter_mut() {
            if stream.pull()? {
                return Ok(Event::NeedData);
            }
            let Some(data) = &stream.data else {
                continue;
            };
            let meta = data
                .get_meta()
                .and_then(SortBound::downcast_ref_from)
                .expect("require a SortBound");
            bounds.push(meta.bound_index)
        }

        let bound_index = match bounds.iter().min() {
            Some(index) => *index,
            None => return Ok(Event::Finished),
        };
        assert!(self.bound_index != u32::MAX || bound_index > self.bound_index);
        self.bound_index = bound_index;
        for stream in streams.iter_mut() {
            stream.bound_index = Some(self.bound_index);
        }

        let streams = std::mem::take(streams);
        self.inner = Ok(Merger::create(
            self.schema.clone(),
            streams,
            self.block_size,
            self.limit,
        ));
        Ok(Event::Sync)
    }
}

struct BoundedInputStream<R: Rows> {
    data: Option<DataBlock>,
    input: Arc<InputPort>,
    remove_order_col: bool,
    bound_index: Option<u32>,
    sort_row_offset: usize,
    _r: PhantomData<R>,
}

impl<R: Rows> SortedStream for BoundedInputStream<R> {
    fn next(&mut self) -> Result<(Option<(DataBlock, Column)>, bool)> {
        if self.pull()? {
            return Ok((None, true));
        }

        match self.take_next_bounded_block() {
            None => Ok((None, false)),
            Some(mut block) => {
                let col = sort_column(&block, self.sort_row_offset).clone();
                if self.remove_order_col {
                    block.remove_column(self.sort_row_offset);
                }
                Ok((Some((block, col)), false))
            }
        }
    }
}

fn sort_column(data: &DataBlock, sort_row_offset: usize) -> &Column {
    data.get_by_offset(sort_row_offset).as_column().unwrap()
}

impl<R: Rows> BoundedInputStream<R> {
    fn pull(&mut self) -> Result<bool> {
        if self.data.is_some() {
            return Ok(false);
        }

        if self.input.has_data() {
            let block = self.input.pull_data().unwrap()?;
            self.input.set_need_data();
            self.data = Some(block);
            Ok(false)
        } else if self.input.is_finished() {
            Ok(false)
        } else {
            self.input.set_need_data();
            Ok(true)
        }
    }

    fn take_next_bounded_block(&mut self) -> Option<DataBlock> {
        let meta = self
            .data
            .as_ref()?
            .get_meta()
            .and_then(SortBound::downcast_ref_from)
            .expect("require a SortBound");

        if meta.bound_index == self.bound_index.unwrap() {
            self.data.take().map(|mut data| {
                data.take_meta().unwrap();
                data
            })
        } else {
            None
        }
    }
}
