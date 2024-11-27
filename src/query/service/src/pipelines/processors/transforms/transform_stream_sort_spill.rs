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
use databend_common_expression::types::DataType;
use databend_common_expression::types::DateType;
use databend_common_expression::types::NumberDataType;
use databend_common_expression::types::NumberType;
use databend_common_expression::types::StringType;
use databend_common_expression::types::TimestampType;
use databend_common_expression::with_number_mapped_type;
use databend_common_expression::BlockMetaInfoDowncast;
use databend_common_expression::Column;
use databend_common_expression::DataBlock;
use databend_common_expression::DataSchemaRef;
use databend_common_expression::Domain;
use databend_common_expression::SortColumnDescription;
use databend_common_pipeline_core::processors::Event;
use databend_common_pipeline_core::processors::InputPort;
use databend_common_pipeline_core::processors::OutputPort;
use databend_common_pipeline_core::processors::Processor;
use databend_common_pipeline_transforms::processors::sort::algorithm::HeapSort;
use databend_common_pipeline_transforms::processors::sort::algorithm::LoserTreeSort;
use databend_common_pipeline_transforms::processors::sort::algorithm::SortAlgorithm;
use databend_common_pipeline_transforms::processors::sort::CommonRows;
use databend_common_pipeline_transforms::processors::sort::Merger;
use databend_common_pipeline_transforms::processors::sort::Rows;
use databend_common_pipeline_transforms::processors::sort::SimpleRowsAsc;
use databend_common_pipeline_transforms::processors::sort::SimpleRowsDesc;
use databend_common_pipeline_transforms::processors::sort::SortSpillMeta;
use databend_common_pipeline_transforms::processors::sort::SortSpillMetaWithParams;
use databend_common_pipeline_transforms::processors::sort::SortedStream;
use databend_common_pipeline_transforms::processors::SortSpillParams;

use crate::spillers::Location;
use crate::spillers::Spiller;

enum State {
    /// The initial state of the processor.
    Init,
    /// This state means the processor will never spill incoming blocks.
    Pass,
    /// This state means the processor will spill incoming blocks except the last block.
    Spill,

    Restore,
    /// Finish the process.
    Finish,
}

pub struct TransformStreamSortSpill<A: SortAlgorithm> {
    input: Arc<InputPort>,
    output: Arc<OutputPort>,
    schema: DataSchemaRef,
    output_order_col: bool,
    limit: Option<usize>,

    input_data: Vec<DataBlock>,
    output_data: VecDeque<DataBlock>,

    state: State,
    spiller: Spiller,

    batch_rows: usize,
    /// Blocks to merge one time.
    num_merge: usize,

    blocks: Vec<VecDeque<Block>>,

    merger: Option<Merger<A, BlockStream>>,

    sort_desc: Arc<Vec<SortColumnDescription>>,
}

#[inline(always)]
fn take_spill_meta(block: &mut DataBlock) -> Option<Option<SortSpillParams>> {
    block.take_meta().map(|meta| {
        if SortSpillMeta::downcast_ref_from(&meta).is_some() {
            return None;
        }
        Some(
            SortSpillMetaWithParams::downcast_from(meta)
                .expect("unknown meta type")
                .0,
        )
    })
}

#[async_trait::async_trait]
impl<A> Processor for TransformStreamSortSpill<A>
where
    A: SortAlgorithm + 'static,
    A::Rows: 'static,
{
    fn name(&self) -> String {
        String::from("TransformStreamSortSpill")
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
                State::Init => {
                    self.input.set_need_data();
                    return Ok(Event::NeedData);
                }
                State::Pass | State::Finish => {
                    self.input.set_not_need_data();
                    return Ok(Event::NeedConsume);
                }
                State::Spill | State::Restore => {
                    return if self.should_process() {
                        Ok(Event::Async)
                    } else {
                        self.input.set_not_need_data();
                        return Ok(Event::NeedConsume);
                    };
                }
            }
        }

        if !self.output_data.is_empty() {
            match self.state {
                State::Pass | State::Restore | State::Finish => {
                    let block = self.output_data.pop_front().unwrap();
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
            let mut block = self.input.pull_data().unwrap()?;
            let meta = take_spill_meta(&mut block);
            return match self.state {
                State::Init => match meta {
                    Some(Some(params)) => {
                        // Need to spill this block.
                        self.batch_rows = params.batch_rows;
                        self.num_merge = params.num_merge;

                        self.input_data.push(block);
                        self.state = State::Spill;
                        if self.should_process() {
                            Ok(Event::Async)
                        } else {
                            self.input.set_need_data();
                            Ok(Event::NeedData)
                        }
                    }
                    Some(None) => unreachable!(),
                    None => {
                        // If we get a memory block at initial state, it means we will never spill data.
                        // debug_assert!(self.spiller.columns_layout.is_empty());
                        self.output_block(block);
                        self.state = State::Pass;
                        Ok(Event::NeedConsume)
                    }
                },
                State::Pass => {
                    debug_assert!(meta.is_none());
                    self.output_block(block);
                    Ok(Event::NeedConsume)
                }
                State::Spill => {
                    self.input_data.push(block);
                    if self.should_process() {
                        Ok(Event::Async)
                    } else {
                        self.input.set_need_data();
                        Ok(Event::NeedData)
                    }
                }
                _ => unreachable!(),
            };
        }

        if self.input.is_finished() {
            return match &self.state {
                State::Init | State::Pass | State::Finish => {
                    self.output.finish();
                    Ok(Event::Finished)
                }
                State::Spill => {
                    // No more input data, we can launch external merge sort now.
                    self.state = State::Restore;
                    Ok(Event::Async)
                }
                State::Restore => Ok(Event::Async),
            };
        }

        self.input.set_need_data();
        Ok(Event::NeedData)
    }

    #[async_backtrace::framed]
    async fn async_process(&mut self) -> Result<()> {
        match &self.state {
            State::Spill => {
                self.spill().await?;
            }
            State::Restore => {
                self.restore().await?;
            }
            // State::MergeFinal => {
            //     debug_assert!(self.final_merger.is_some());
            //     debug_assert!(self.unmerged_blocks.is_empty());
            //     let merger = self.final_merger.as_mut().unwrap();
            //     if let Some(block) = merger.async_next_block().await? {
            //         self.output_data = Some(block);
            //     } else {
            //         self.state = State::Finish;
            //     }
            // }
            _ => unreachable!(),
        }
        Ok(())
    }
}

impl<A> TransformStreamSortSpill<A>
where
    A: SortAlgorithm + 'static,
    A::Rows: 'static,
{
    pub fn create(
        input: Arc<InputPort>,
        output: Arc<OutputPort>,
        schema: DataSchemaRef,
        sort_desc: Arc<Vec<SortColumnDescription>>,
        limit: Option<usize>,
        spiller: Spiller,
        output_order_col: bool,
    ) -> Self {
        todo!()
        // Self {
        //     input,
        //     output,
        //     schema,
        //     limit,
        //     output_order_col,
        //     input_data: None,
        //     output_data: None,
        //     spiller,
        //     state: State::Init,
        //     num_merge: 0,
        //     unmerged_blocks: VecDeque::new(),
        //     final_merger: None,
        //     batch_rows: 0,
        //     sort_desc,
        // }
    }

    #[inline(always)]
    fn output_block(&self, mut block: DataBlock) {
        if !self.output_order_col {
            block.pop_columns(1);
        }
        self.output.push_data(Ok(block));
    }

    async fn spill(&mut self) -> Result<()> {
        let blocks = std::mem::take(&mut self.input_data);

        let mut merger = Merger::<A, BlockStream>::create(
            self.schema.clone(),
            blocks.into_iter().map(|b| BlockStream(Some(b))).collect(),
            self.sort_desc.clone(),
            self.batch_rows,
            self.limit,
        );

        let mut spilled = VecDeque::new();
        while let Some(block) = merger.next_block()? {
            spilled.push_back(self.spill_block(block).await?);
        }
        debug_assert!(merger.is_finished());

        self.blocks.push(spilled);
        Ok(())
    }

    async fn restore(&mut self) -> Result<()> {
        let bound = A::Rows::from_column(&self.bound(), &self.sort_desc)?; // todo check
        let streams = self
            .blocks
            .iter_mut()
            .map(|blocks| BoundBlockStream::<A::Rows> {
                sort_desc: &self.sort_desc,
                blocks,
                bound: bound.clone(),
                spiller: &self.spiller,
            })
            .collect();

        let mut merger = Merger::<A, BoundBlockStream<A::Rows>>::create(
            self.schema.clone(),
            streams,
            self.sort_desc.clone(),
            self.batch_rows,
            self.limit, // todo
        );

        let mut outputs = Vec::new();
        while let Some(data) = merger.async_next_block().await? {
            outputs.push(data);
        }
        debug_assert!(merger.is_finished());

        for data in outputs {
            self.output_block(data);
        }

        Ok(())
    }

    fn should_process(&self) -> bool {
        todo!()
    }

    async fn spill_block(&mut self, block: DataBlock) -> Result<Block> {
        let domain = block.get_last_column().domain();
        let location = self.spiller.spill(vec![block]).await?;
        Ok(Block {
            data: BlockData::Spilled(location),
            domain,
        })
    }

    fn bound(&self) -> Column {
        todo!()
    }
}

struct Block {
    data: BlockData,
    domain: Domain,
}

enum BlockData {
    Memory(DataBlock),
    Spilled(Location),
}

impl Block {
    // fn memory_size(&self) -> usize {
    //     match self.data {
    //         BlockData::Memory(b) => b.memory_size(),
    //         BlockData::Spilled(_) => 0,
    //     }
    // }
}

struct BoundBlockStream<'a, R: Rows> {
    sort_desc: &'a [SortColumnDescription],
    blocks: &'a mut VecDeque<Block>,
    bound: R,
    spiller: &'a Spiller,
}

unsafe impl<'a, R: Rows> Send for BoundBlockStream<'a, R> {}

#[async_trait::async_trait]
impl<'a, R: Rows + Send> SortedStream for BoundBlockStream<'a, R> {
    async fn async_next(&mut self) -> Result<(Option<(DataBlock, Column)>, bool)> {
        if self.should_include_first() {
            self.restore_first().await?;
            let block = self.block_slice();
            let col = block.get_last_column().clone();
            Ok((Some((block, col)), false))
        } else {
            Ok((None, false))
        }
    }
}

impl<'a, R: Rows> BoundBlockStream<'a, R> {
    fn should_include_first(&self) -> bool {
        todo!()
    }

    fn block_slice(&mut self) -> DataBlock {
        todo!()
    }

    async fn restore_first(&mut self) -> Result<()> {
        let block = self.blocks.front_mut().unwrap();
        match &block.data {
            BlockData::Memory(_) => Ok(()),
            BlockData::Spilled(location) => {
                let data = self.spiller.read_spilled_file(location).await?;
                block.data = BlockData::Memory(data);
                Ok(())
            }
        }
    }
}

struct BlockStream(Option<DataBlock>);

impl SortedStream for BlockStream {
    fn next(&mut self) -> Result<(Option<(DataBlock, Column)>, bool)> {
        let data = self.0.take().map(|b| {
            let col = b.get_last_column().clone();
            (b, col)
        });
        Ok((data, false))
    }
}
