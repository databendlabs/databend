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
                bound: None,
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
            bound_index: 0,
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
        let merger = self.inner.as_mut().ok().unwrap();
        if let Some(block) = merger.next_block()? {
            self.output_data = Some(block.add_meta(Some(SortBound::create(
                self.bound_index,
                (!merger.is_finished()).then_some(self.bound_index),
            )))?);
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
                self.bound_index += 1;
                let merger = std::mem::replace(inner, Err(vec![])).ok().unwrap();
                self.inner = Err(merger.streams());
                self.inner.as_mut().err().unwrap()
            }
            Err(streams) => streams,
        };

        if streams.iter().all(|stream| stream.input.is_finished()) {
            return Ok(Event::Finished);
        }

        // {
        //     for stream in streams.iter_mut() {
        //         stream.pull()?;
        //     }

        //     if streams
        //         .iter_mut()
        //         .map(|stream| stream.pull())
        //         .try_fold(false, |acc, pending| acc || pending?)?
        //     {
        //         return Ok(Event::NeedData);
        //     }

        //     if bounds.is_empty() {
        //         return Ok(Event::Finished);
        //     }

        //     if bounds.iter().all(|meta| meta.index != self.bound_index) {
        //         let meta = match bounds.iter().min_by_key(|meta| meta.index) {
        //             Some(index) => *index,
        //             None => return Ok(Event::Finished),
        //         };
        //         assert!(meta.index > self.bound_index);
        //         self.bound_index = meta.index;
        //     }
        // }

        for stream in streams.iter_mut() {
            stream.update_bound_index(self.bound_index);
        }

        self.inner = Ok(Merger::create(
            self.schema.clone(),
            std::mem::take(streams),
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
    sort_row_offset: usize,
    bound: Option<Bound>,
    _r: PhantomData<R>,
}

#[derive(Debug, Clone, Copy)]
struct Bound {
    bound_index: u32,
    more: bool,
}

impl<R: Rows> SortedStream for BoundedInputStream<R> {
    fn next(&mut self) -> Result<(Option<(DataBlock, Column)>, bool)> {
        if self.bound.unwrap().more && self.pull()? {
            return Ok((None, true));
        }

        match self.take_next_bounded_block() {
            None => Ok((None, false)),
            Some(mut block) => {
                if block.is_empty() {
                    return Ok((None, true));
                }
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

        let bound = self.bound.as_mut().unwrap();
        if meta.index == bound.bound_index {
            bound.more = meta.next.is_some_and(|next| next == meta.index);
            self.data.take().map(|mut data| {
                data.take_meta().unwrap();
                data
            })
        } else {
            assert!(meta.index > bound.bound_index);
            None
        }
    }

    fn update_bound_index(&mut self, bound_index: u32) {
        self.bound = Some(Bound {
            bound_index,
            more: true,
        });
    }
}
