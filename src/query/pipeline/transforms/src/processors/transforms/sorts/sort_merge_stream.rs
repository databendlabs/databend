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
use std::marker::PhantomData;
use std::sync::Arc;

use databend_common_exception::Result;
use databend_common_expression::BlockMetaInfoDowncast;
use databend_common_expression::Column;
use databend_common_expression::DataBlock;
use databend_common_expression::DataSchemaRef;
use databend_common_pipeline::core::Event;
use databend_common_pipeline::core::InputPort;
use databend_common_pipeline::core::OutputPort;
use databend_common_pipeline::core::Processor;

use super::core::algorithm::SortAlgorithm;
use super::core::Merger;
use super::core::Rows;
use super::core::SortedStream;
use super::SortBound;
use super::SortBoundNext;

type Stream<A> = BoundedInputStream<<A as SortAlgorithm>::Rows>;

pub struct BoundedMultiSortMergeProcessor<A>
where A: SortAlgorithm
{
    inputs: Vec<Arc<InputPort>>,
    output: Arc<OutputPort>,
    schema: DataSchemaRef,
    block_size: usize,

    output_data: VecDeque<DataBlock>,
    cur_index: u32,
    cur_finished: bool,
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
    ) -> Result<Self> {
        assert!(inputs.len() != 1);

        let streams = inputs
            .iter()
            .map(|input| BoundedInputStream {
                data: None,
                input: input.clone(),
                remove_order_col: true,
                bound: None,
                sort_row_offset: schema.fields().len(),
                _r: PhantomData,
            })
            .collect();

        Ok(Self {
            inputs,
            output,
            schema,
            block_size,
            output_data: VecDeque::new(),
            cur_index: 0,
            cur_finished: false,
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

        if let Some(block) = self.output_data.pop_front() {
            self.output.push_data(Ok(block));
            return Ok(Event::NeedConsume);
        }

        self.next_event()
    }

    fn process(&mut self) -> Result<()> {
        let merger = self.inner.as_mut().ok().unwrap();
        if let Some(block) = merger.next_block()? {
            let finished = merger.is_finished();
            self.cur_finished = finished;
            self.output_data
                .push_back(block.add_meta(Some(SortBound::create(
                    self.cur_index,
                    if finished {
                        SortBoundNext::Last
                    } else {
                        SortBoundNext::More
                    },
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
            Ok(merger) => {
                merger.poll_pending_stream()?;
                if !merger.is_finished() {
                    return Ok(Event::Sync);
                }

                if !self.cur_finished {
                    let empty = DataBlock::empty_with_schema(self.schema.clone());
                    self.output_data.push_back(
                        empty.add_meta(Some(SortBound::create(
                            self.cur_index,
                            SortBoundNext::Skip,
                        )))?,
                    );
                } else {
                    self.cur_finished = false;
                }

                self.cur_index += 1;
                let merger = std::mem::replace(&mut self.inner, Err(vec![]))
                    .ok()
                    .unwrap();
                self.inner = Err(merger.streams());
                self.inner.as_mut().err().unwrap()
            }
            Err(streams) => streams,
        };

        if streams.iter().all(|stream| stream.is_finished()) {
            log::debug!("output finished, cur_index {}", self.cur_index);
            self.output.finish();
            return Ok(Event::Finished);
        }

        log::debug!("create merger cur_index {}", self.cur_index);
        for stream in streams.iter_mut() {
            stream.update_bound_index(self.cur_index);
        }

        self.inner = Ok(Merger::create(
            self.schema.clone(),
            std::mem::take(streams),
            self.block_size,
            None,
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

        if let Some(block) = self.input.pull_data().transpose()? {
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
        assert!(
            meta.index >= bound.bound_index,
            "meta: {meta:?}, bound: {bound:?}",
        );
        if meta.index == bound.bound_index {
            bound.more = match meta.next {
                SortBoundNext::Next(next) => meta.index == next,
                SortBoundNext::Last => false,
                _ => unreachable!(),
            };
            self.data.take().map(|mut data| {
                data.take_meta().unwrap();
                data
            })
        } else {
            None
        }
    }

    fn update_bound_index(&mut self, bound_index: u32) {
        self.bound = Some(Bound {
            bound_index,
            more: true,
        });
    }

    fn is_finished(&self) -> bool {
        self.input.is_finished() && self.data.is_none()
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use databend_common_expression::types::Int32Type;
    use databend_common_expression::FromData;
    use databend_common_pipeline::core::port::connect;

    use super::*;
    use crate::sorts::core::SimpleRowsAsc;

    fn create_block(empty: bool, index: u32, next: u32) -> DataBlock {
        let block = DataBlock::new_from_columns(vec![Int32Type::from_data(vec![1, 2, 3])]);
        let block = if empty { block.slice(0..0) } else { block };
        block
            .add_meta(Some(SortBound::create(index, SortBoundNext::Next(next))))
            .unwrap()
    }

    fn create_stream() -> (
        BoundedInputStream<SimpleRowsAsc<Int32Type>>,
        Arc<OutputPort>,
    ) {
        let output = OutputPort::create();
        let input = InputPort::create();
        unsafe {
            connect(&input, &output);
        }

        let stream = BoundedInputStream {
            data: None,
            input,
            remove_order_col: false,
            sort_row_offset: 0,
            bound: None,
            _r: PhantomData,
        };
        (stream, output)
    }

    #[test]
    fn test_bounded_input_stream() {
        let (mut stream, output) = create_stream();

        stream.update_bound_index(0);

        {
            let (_, pending) = stream.next().unwrap();

            assert!(stream.bound.unwrap().more);
            assert!(pending);
        }

        {
            let block = create_block(true, 0, 0);
            output.push_data(Ok(block));

            let (_, pending) = stream.next().unwrap();

            assert!(stream.bound.unwrap().more);
            assert!(pending);
        }

        {
            let block = create_block(false, 0, 0);
            output.push_data(Ok(block));

            let (data, pending) = stream.next().unwrap();
            assert!(!pending);
            let data = data.unwrap();
            assert!(data.0.get_meta().is_none());
            assert_eq!(data.1.len(), 3);
        }

        {
            let block = create_block(true, 0, 1);
            output.push_data(Ok(block));

            let (data, pending) = stream.next().unwrap();

            assert!(data.is_none());
            assert!(!stream.bound.unwrap().more);
            assert!(pending);

            let block = create_block(false, 1, 1);
            output.push_data(Ok(block));

            let (data, pending) = stream.next().unwrap();
            assert!(data.is_none());
            assert!(!stream.bound.unwrap().more);
            assert!(!pending);

            let (data, pending) = stream.next().unwrap();
            assert!(data.is_none());
            assert!(!stream.bound.unwrap().more);
            assert!(!pending);
        }
    }
}
