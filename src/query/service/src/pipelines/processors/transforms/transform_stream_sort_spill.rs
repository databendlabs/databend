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
use std::assert_matches::debug_assert_matches;
use std::collections::VecDeque;
use std::marker::PhantomData;
use std::mem;
use std::sync::Arc;

use databend_common_column::bitmap::MutableBitmap;
use databend_common_exception::Result;
use databend_common_expression::simpler::FixedRateSimpler;
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

use crate::spillers::Layout;
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
    spiller: Arc<Spiller>,
    simpler: FixedRateSimpler,

    batch_rows: usize,
    /// Blocks to merge one time.
    num_merge: usize,

    subsequent: Vec<BoundBlockStream<A::Rows>>,
    current: Vec<BoundBlockStream<A::Rows>>,

    output_merger: Option<Merger<A, BoundBlockStream<A::Rows>>>,

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
                    // todo
                    self.input.set_not_need_data();
                    return Ok(Event::NeedConsume);
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
                        if self.input_full() {
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
                    if self.input_full() {
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
                    let n = self.input_data.len();
                    if n > 0 && n + self.subsequent.len() > self.num_merge {
                        return Ok(Event::Async);
                    }

                    self.state = State::Restore;
                    self.subsequent
                        .extend(self.input_data.drain(..).map(|data| {
                            BoundBlockStream::<A::Rows> {
                                sort_desc: self.sort_desc.clone(),
                                blocks: vec![Block {
                                    readed: 0,
                                    location: None,
                                    size: data.memory_size(),
                                    domain: get_domain(data.get_last_column()),
                                    data: Some(data),
                                }]
                                .into(),
                                bound: None,
                                spiller: self.spiller.clone(),
                            }
                        }));

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
                self.on_spill().await?;
            }
            State::Restore => {
                debug_assert!(self.input_data.is_empty());
                self.on_restore().await?;
            }
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

    async fn on_spill(&mut self) -> Result<()> {
        let blocks = mem::take(&mut self.input_data);

        for data in &blocks {
            self.simpler.add_block(data.clone())
        }
        self.simpler.compact_blocks();

        let mut merger = Merger::<A, BlockStream>::create(
            self.schema.clone(),
            blocks.into_iter().map(|b| BlockStream(Some(b))).collect(),
            self.sort_desc.clone(),
            self.batch_rows,
            self.limit,
        );

        let mut spilled = VecDeque::new();
        while let Some(block) = merger.next_block()? {
            spilled.push_back(self.new_block(block).await?);
        }
        debug_assert!(merger.is_finished());

        self.subsequent.push(BoundBlockStream::<A::Rows> {
            sort_desc: self.sort_desc.clone(),
            blocks: spilled,
            bound: None,
            spiller: self.spiller.clone(),
        });
        Ok(())
    }

    async fn on_restore(&mut self) -> Result<()> {
        while self.current.is_empty() {
            self.choice_list();
        }
        if self.subsequent.len() > self.num_merge {
            self.merge().await
        } else {
            self.restore_output().await
        }
    }

    async fn merge(&mut self) -> Result<()> {
        self.current.sort_by_key(|s| std::cmp::Reverse(s.len()));
        let streams = self
            .current
            .drain(self.current.len() - self.num_merge..)
            .collect();

        let mut merger = Merger::<A, BoundBlockStream<A::Rows>>::create(
            self.schema.clone(),
            streams,
            self.sort_desc.clone(),
            self.batch_rows,
            None,
        );

        let mut spilled = VecDeque::new();
        while let Some(data) = merger.async_next_block().await? {
            spilled.push_back(self.new_block(data).await?);
        }
        debug_assert!(merger.is_finished());

        self.current.push(BoundBlockStream::<A::Rows> {
            sort_desc: self.sort_desc.clone(),
            blocks: spilled,
            bound: None,
            spiller: self.spiller.clone(),
        });

        self.subsequent
            .extend(merger.streams().into_iter().filter(|s| !s.is_empty()));
        Ok(())
    }

    async fn restore_output(&mut self) -> Result<()> {
        let merger = match &mut self.output_merger {
            Some(merger) => merger,
            None => {
                let cur = mem::take(&mut self.current);
                let merger = Merger::<A, BoundBlockStream<A::Rows>>::create(
                    self.schema.clone(),
                    cur,
                    self.sort_desc.clone(),
                    self.batch_rows,
                    None,
                );
                self.output_merger.insert(merger)
            }
        };

        if let Some(data) = merger.async_next_block().await? {
            self.output_block(data);
            return Ok(());
        }

        debug_assert!(merger.is_finished());

        let streams = self.output_merger.take().unwrap().streams();
        self.subsequent
            .extend(streams.into_iter().filter(|s| !s.is_empty()));

        Ok(())
    }

    fn choice_list(&mut self) {
        let Some(bound) = self.next_bound() else {
            let mut sub = mem::take(&mut self.subsequent);
            for s in &mut sub {
                s.bound = None
            }
            self.current = sub;
            return;
        };

        let bound = A::Rows::from_column(&bound, &self.sort_desc).unwrap(); // todo check
        let (cur, sub): (Vec<_>, Vec<_>) = self
            .subsequent
            .drain(..)
            .map(|mut s| {
                s.bound = Some(bound.clone());
                s
            })
            .partition(|s| s.should_include_first());

        self.current = cur;
        self.subsequent = sub;
    }

    fn subsequent_memory(&self) -> usize {
        self.subsequent.iter().map(|s| s.memory_used()).sum()
    }

    fn input_full(&self) -> bool {
        let rows = self
            .input_data
            .iter()
            .map(|b| b.num_columns())
            .sum::<usize>();
        rows >= self.num_merge * self.batch_rows
    }

    async fn new_block(&mut self, block: DataBlock) -> Result<Block> {
        let size = block.memory_size();
        let domain = get_domain(block.get_last_column());
        let location = self.spiller.spill_unmanage(vec![block]).await?;
        Ok(Block {
            data: None,
            size,
            domain,
            location: Some(location),
            readed: 0,
        })
    }

    fn next_bound(&self) -> Option<Column> {
        todo!()
    }
}

struct Block {
    data: Option<DataBlock>,
    size: usize,
    location: Option<(Location, Layout)>,
    domain: Column,
    readed: usize,
}

impl Block {
    // fn memory_size(&self) -> usize {
    //     match self.data {
    //         BlockData::Memory(b) => b.memory_size(),
    //         BlockData::Spilled(_) => 0,
    //     }
    // }

    fn slice(&mut self, pos: usize) -> DataBlock {
        let data = self.data.as_ref().unwrap();

        let left = data.slice(0..pos);
        let right = data.slice(pos..data.num_rows());

        self.domain = get_domain(right.get_last_column());
        self.size = right.memory_size();
        self.data = Some(right);
        self.readed += pos;
        left
    }

    fn domain<R: Rows>(&self, sort_desc: &[SortColumnDescription]) -> R {
        R::from_column(&self.domain, sort_desc).unwrap()
    }
}

struct BoundBlockStream<R: Rows + Send> {
    sort_desc: Arc<Vec<SortColumnDescription>>,
    blocks: VecDeque<Block>,
    bound: Option<R>,
    spiller: Arc<Spiller>,
}

#[async_trait::async_trait]
impl<R: Rows + Send> SortedStream for BoundBlockStream<R> {
    async fn async_next(&mut self) -> Result<(Option<(DataBlock, Column)>, bool)> {
        if self.should_include_first() {
            self.restore_first().await?;
            let data = self.pop_front_data();
            let col = data.get_last_column().clone();
            Ok((Some((data, col)), false))
        } else {
            Ok((None, false))
        }
    }
}

impl<R: Rows + Send> BoundBlockStream<R> {
    fn should_include_first(&self) -> bool {
        let Some(block) = self.blocks.front() else {
            return false;
        };

        match &self.bound {
            Some(bound) => block.domain::<R>(&self.sort_desc).first() < bound.row(0),
            None => true,
        }
    }

    fn pop_front_data(&mut self) -> DataBlock {
        match &self.bound {
            Some(bound) => {
                let block = self.blocks.front_mut().unwrap();

                let data = block.data.as_ref().unwrap();
                let rows = R::from_column(data.get_last_column(), &self.sort_desc).unwrap();

                // todo binary_search
                let bound = self.bound.as_ref().unwrap().row(0);
                match (0..rows.len()).position(|i| rows.row(i) >= bound) {
                    Some(pos) => block.slice(pos),
                    None => {
                        let mut block = self.blocks.pop_front().unwrap();
                        block.data.take().unwrap()
                    }
                }
            }
            None => {
                let mut block = self.blocks.pop_front().unwrap();
                block.data.take().unwrap()
            }
        }
    }

    async fn restore_first(&mut self) -> Result<()> {
        let block = self.blocks.front_mut().unwrap();
        if block.data.is_none() {
            let location = block.location.as_ref().unwrap();
            let data = self
                .spiller
                .read_unmanage_spilled_file(&location.0, &location.1)
                .await?;
            block.data = Some(if block.readed != 0 {
                data.slice(block.readed..data.num_columns())
            } else {
                data
            });
        }
        Ok(())
    }

    fn need_restore(&self) -> bool {
        self.blocks.front().map_or(false, |b| b.data.is_none())
    }

    fn memory_used(&self) -> usize {
        self.blocks
            .front()
            .map_or(0, |b| if b.data.is_some() { b.size } else { 0 })
    }

    fn len(&self) -> usize {
        self.blocks.len()
    }

    fn is_empty(&self) -> bool {
        self.len() == 0
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

fn get_domain(col: &Column) -> Column {
    match col.len() {
        0 => unreachable!(),
        1 | 2 => col.clone(),
        n => {
            let mut bitmap = MutableBitmap::with_capacity(n);
            bitmap.push(true);
            bitmap.extend_constant(n - 2, false);
            bitmap.push(true);

            col.filter(&bitmap.freeze())
        }
    }
}

#[cfg(test)]
mod tests {
    use databend_common_expression::types::Int32Type;
    use databend_common_expression::types::StringType;
    use databend_common_expression::BlockEntry;
    use databend_common_expression::DataField;
    use databend_common_expression::DataSchemaRefExt;
    use databend_common_expression::FromData;
    use databend_common_expression::Value;
    use databend_common_pipeline_transforms::processors::sort::convert_rows;
    use databend_common_pipeline_transforms::processors::sort::SimpleRowsAsc;
    use databend_common_storage::DataOperator;

    use super::*;
    use crate::spillers::SpillerConfig;
    use crate::spillers::SpillerType;
    use crate::test_kits::*;

    fn test_data() -> (DataSchemaRef, DataBlock) {
        let col1 = Int32Type::from_data(vec![7, 7, 8, 11, 3, 5, 10, 11]);
        let col2 = StringType::from_data(vec!["e", "w", "d", "g", "h", "d", "e", "f"]);

        let schema = DataSchemaRefExt::create(vec![
            DataField::new("a", DataType::Number(NumberDataType::Int32)),
            DataField::new("b", DataType::String),
        ]);

        let block = DataBlock::new_from_columns(vec![col1, col2]);

        (schema, block)
    }

    async fn run_bound_block_stream<R: Rows + Send>(
        spiller: Arc<Spiller>,
        sort_desc: Arc<Vec<SortColumnDescription>>,
        bound: Column,
        block_part: usize,
        want: Option<Column>,
    ) -> Result<()> {
        let (schema, block) = test_data();
        let block = DataBlock::sort(&block, &sort_desc, None)?;
        let bound = Some(R::from_column(&bound, &sort_desc)?);

        let blocks = vec![
            block.slice(0..block_part),
            block.slice(block_part..block.num_rows()),
        ]
        .into_iter()
        .map(|mut data| {
            let col = convert_rows(schema.clone(), &sort_desc, data.clone()).unwrap();
            data.add_column(BlockEntry::new(col.data_type(), Value::Column(col)));
            let domain = get_domain(data.get_last_column());
            let size = data.memory_size();

            Block {
                data: Some(data),
                domain,
                size,
                location: None,
                readed: 0,
            }
        })
        .collect::<VecDeque<_>>();

        let mut stream = BoundBlockStream::<R> {
            sort_desc: sort_desc.clone(),
            blocks,
            bound,
            spiller: spiller.clone(),
        };

        let (got, _) = stream.async_next().await?;

        // println!("{got:?}");

        match want {
            Some(col) => assert_eq!(got.unwrap().1, col),
            None => assert!(got.is_none()),
        }

        Ok(())
    }

    #[tokio::test]
    async fn test_bound_block_stream() -> Result<()> {
        let fixture = TestFixture::setup().await?;
        let ctx = fixture.new_query_ctx().await?;

        let op = DataOperator::instance().operator();
        let spill_config = SpillerConfig {
            spiller_type: SpillerType::OrderBy,
            location_prefix: "_spill_test".to_string(),
            disk_spill: None,
            use_parquet: true,
        };
        let spiller = Arc::new(Spiller::create(ctx.clone(), op, spill_config)?);

        {
            let sort_desc = Arc::new(vec![SortColumnDescription {
                offset: 0,
                asc: true,
                nulls_first: false,
            }]);

            run_bound_block_stream::<SimpleRowsAsc<Int32Type>>(
                spiller.clone(),
                sort_desc.clone(),
                Int32Type::from_data(vec![5]),
                4,
                Some(Int32Type::from_data(vec![3])),
            )
            .await?;

            run_bound_block_stream::<SimpleRowsAsc<Int32Type>>(
                spiller.clone(),
                sort_desc.clone(),
                Int32Type::from_data(vec![3]),
                4,
                None,
            )
            .await?;
        }

        {
            let sort_desc = Arc::new(vec![SortColumnDescription {
                offset: 1,
                asc: false,
                nulls_first: false,
            }]);

            run_bound_block_stream::<SimpleRowsDesc<StringType>>(
                spiller.clone(),
                sort_desc.clone(),
                StringType::from_data(vec!["f"]),
                4,
                Some(StringType::from_data(vec!["w", "h", "g"])),
            )
            .await?;
        }

        Ok(())
    }
}
