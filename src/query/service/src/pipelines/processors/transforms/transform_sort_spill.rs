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
use std::cmp::Reverse;
use std::collections::BinaryHeap;
use std::collections::VecDeque;
use std::marker::PhantomData;
use std::sync::Arc;

use common_exception::ErrorCode;
use common_exception::Result;
use common_expression::types::DataType;
use common_expression::types::NumberDataType;
use common_expression::types::NumberType;
use common_expression::with_number_mapped_type;
use common_expression::BlockMetaInfo;
use common_expression::BlockMetaInfoDowncast;
use common_expression::DataBlock;
use common_expression::DataSchemaRef;
use common_expression::SortColumnDescription;
use common_pipeline_core::processors::Event;
use common_pipeline_core::processors::InputPort;
use common_pipeline_core::processors::OutputPort;
use common_pipeline_core::processors::Processor;
use common_pipeline_transforms::processors::sort::utils::find_bigger_child_of_root;
use common_pipeline_transforms::processors::sort::CommonRows;
use common_pipeline_transforms::processors::sort::Cursor;
use common_pipeline_transforms::processors::sort::DateRows;
use common_pipeline_transforms::processors::sort::Rows;
use common_pipeline_transforms::processors::sort::SimpleRows;
use common_pipeline_transforms::processors::sort::StringRows;
use common_pipeline_transforms::processors::sort::TimestampRows;

use crate::spillers::Spiller;

/// A spilled block file is at most 8MB.
// const SPILL_BATCH_SIZE: usize = 8 * 1024 * 1024;

enum State {
    /// The initial state of the processor.
    Init,
    /// This state means the processor will never spill incoming blocks.
    NoSpill,
    /// This state means the processor will spill incoming blocks except the last block.
    Spill,
    /// This state means the processor is doing external merge sort.
    Merging,
    /// Merge finished, we can output the sorted data now.
    MergeFinished,
    /// Finish the process.
    Finish,
}

pub struct TransformSortSpill<R> {
    input: Arc<InputPort>,
    output: Arc<OutputPort>,
    schema: DataSchemaRef,

    input_data: Option<DataBlock>,
    output_data: Option<DataBlock>,

    state: State,
    spiller: Spiller,

    batch_size: usize,
    /// Blocks to merge one time.
    num_merge: usize,
    /// Unmerged list of blocks. Each list are sorted.
    unmerged_blocks: VecDeque<VecDeque<String>>,

    sort_desc: Arc<Vec<SortColumnDescription>>,

    _r: PhantomData<R>,
}

#[inline(always)]
fn need_spill(block: &DataBlock) -> bool {
    block
        .get_meta()
        .and_then(SortSpillMeta::downcast_ref_from)
        .is_some()
}

#[async_trait::async_trait]
impl<R> Processor for TransformSortSpill<R>
where R: Rows + Send + Sync + 'static
{
    fn name(&self) -> String {
        String::from("TransformSortSpill")
    }

    fn as_any(&mut self) -> &mut dyn Any {
        self
    }

    fn event(&mut self) -> Result<Event> {
        if self.output.is_finished() {
            self.input.finish();
            return Ok(Event::Finished);
        }

        if matches!(self.state, State::Finish) {
            debug_assert!(self.input.is_finished());
            self.output.finish();
            return Ok(Event::Finished);
        }

        if !self.output.can_push() {
            self.input.set_not_need_data();
            return Ok(Event::NeedConsume);
        }

        if let Some(data_block) = self.output_data.take() {
            debug_assert!(matches!(self.state, State::MergeFinished));
            self.output.push_data(Ok(data_block));
            return Ok(Event::NeedConsume);
        }

        if self.input_data.is_some() {
            return Ok(Event::Async);
        }

        if self.input.has_data() {
            let block = self.input.pull_data().unwrap()?;
            return match &self.state {
                State::Init => {
                    if need_spill(&block) {
                        // Need to spill this block.
                        self.input_data = Some(block);
                        self.state = State::Spill;
                        Ok(Event::Async)
                    } else {
                        // If we get a memory block at initial state, it means we will never spill data.
                        debug_assert!(self.spiller.columns_layout.is_empty());
                        self.output.push_data(Ok(block));
                        self.state = State::NoSpill;
                        Ok(Event::NeedConsume)
                    }
                }
                State::NoSpill => {
                    debug_assert!(!need_spill(&block));
                    self.output.push_data(Ok(block));
                    self.state = State::NoSpill;
                    Ok(Event::NeedConsume)
                }
                State::Spill => {
                    if !need_spill(&block) {
                        // It means we get the last block.
                        // We can launch external merge sort now.
                        self.state = State::Merging;
                    }
                    self.input_data = Some(block);
                    Ok(Event::Async)
                }
                _ => unreachable!(),
            };
        }

        if self.input.is_finished() {
            return match &self.state {
                State::Init | State::NoSpill | State::Finish => {
                    self.output.finish();
                    Ok(Event::Finished)
                }
                State::Spill => {
                    // No more input data, we can launch external merge sort now.
                    self.state = State::Merging;
                    Ok(Event::Async)
                }
                State::Merging => Ok(Event::Async),
                State::MergeFinished => Ok(Event::Async),
            };
        }

        self.input.set_need_data();
        Ok(Event::NeedData)
    }

    #[async_backtrace::framed]
    async fn async_process(&mut self) -> Result<()> {
        match &self.state {
            State::Spill => {
                let block = self.input_data.take().unwrap();
                self.spill(block).await?;
            }
            State::Merging => {
                let block = self.input_data.take();
                self.merge_sort(block).await?;
            }
            State::MergeFinished => {
                debug_assert_eq!(self.unmerged_blocks.len(), 1);
                // TODO: pass the spilled locations to next processor directly.
                // The next processor will read and process the spilled files.
                if let Some(file) = self.unmerged_blocks[0].pop_front() {
                    let block = self.spiller.read_spilled(&file).await?;
                    self.output_data = Some(block);
                } else {
                    self.state = State::Finish;
                }
            }
            _ => unreachable!(),
        }
        Ok(())
    }
}

impl<R> TransformSortSpill<R>
where R: Rows + Sync + Send + 'static
{
    pub fn create(
        input: Arc<InputPort>,
        output: Arc<OutputPort>,
        schema: DataSchemaRef,
        sort_desc: Arc<Vec<SortColumnDescription>>,
        spiller: Spiller,
    ) -> Self {
        // TODO(spill): `num_merge` and `batch_size` should be determined by the memory usage.
        Self {
            input,
            output,
            schema,
            input_data: None,
            output_data: None,
            spiller,
            state: State::Init,
            num_merge: 2,
            unmerged_blocks: VecDeque::new(),
            batch_size: 65535,
            sort_desc,
            _r: PhantomData,
        }
    }

    async fn spill(&mut self, block: DataBlock) -> Result<()> {
        let location = self.spiller.spill_block(block).await?;
        self.unmerged_blocks.push_back(vec![location].into());
        Ok(())
    }

    /// Do an external merge sort until there is only one sorted stream.
    /// If `block` is not [None], we need to merge it with spilled files.
    async fn merge_sort(&mut self, mut block: Option<DataBlock>) -> Result<()> {
        while (self.unmerged_blocks.len() + block.is_some() as usize) > 1 {
            let b = block.take();
            self.merge_sort_one_round(b).await?;
        }
        self.state = State::MergeFinished;
        Ok(())
    }

    /// Merge certain number of sorted streams to one sorted stream.
    async fn merge_sort_one_round(&mut self, block: Option<DataBlock>) -> Result<()> {
        let mut num_streams = self.unmerged_blocks.len() + block.is_some() as usize;
        debug_assert!(num_streams > 1);
        num_streams = num_streams.min(self.num_merge);

        let mut streams = Vec::with_capacity(num_streams);
        if let Some(block) = block {
            streams.push(BlockStream::Block(Some(block)));
            num_streams -= 1;
        }

        let spiller_snapshot = Arc::new(self.spiller.clone());
        for _ in 0..num_streams {
            let files = self.unmerged_blocks.pop_front().unwrap();
            for file in files.iter() {
                self.spiller.columns_layout.remove(file);
            }
            let stream = BlockStream::Spilled((files, spiller_snapshot.clone()));
            streams.push(stream);
        }

        let mut merger = Merger::<R>::create(
            self.schema.clone(),
            streams,
            self.sort_desc.clone(),
            self.batch_size,
        );

        let mut spilled = VecDeque::new();
        while let Some(block) = merger.next().await? {
            let location = self.spiller.spill_block(block).await?;
            spilled.push_back(location);
        }
        self.unmerged_blocks.push_back(spilled);

        Ok(())
    }
}

/// Mark a partially sorted [`DataBlock`] as a block needs to be spilled.
#[derive(serde::Serialize, serde::Deserialize, Debug)]
pub struct SortSpillMeta {}

#[typetag::serde(name = "sort_spill")]
impl BlockMetaInfo for SortSpillMeta {
    fn equals(&self, _: &Box<dyn BlockMetaInfo>) -> bool {
        unimplemented!("Unimplemented equals SortSpillMeta")
    }

    fn clone_self(&self) -> Box<dyn BlockMetaInfo> {
        unimplemented!("Unimplemented clone SortSpillMeta")
    }
}

enum BlockStream {
    Spilled((VecDeque<String>, Arc<Spiller>)),
    Block(Option<DataBlock>),
}

impl BlockStream {
    async fn next(&mut self) -> Result<Option<DataBlock>> {
        let block = match self {
            BlockStream::Block(block) => block.take(),
            BlockStream::Spilled((files, spiller)) => {
                if let Some(file) = files.pop_front() {
                    let block = spiller.read_spilled(&file).await?;
                    Some(block)
                } else {
                    None
                }
            }
        };
        Ok(block)
    }
}

/// A merge sort operator to merge multiple sorted streams.
///
/// TODO: reuse this operator in other places such as `TransformMultiSortMerge` and `TransformSortMerge`.
struct Merger<R: Rows> {
    schema: DataSchemaRef,
    sort_desc: Arc<Vec<SortColumnDescription>>,
    unsorted_streams: Vec<BlockStream>,
    heap: BinaryHeap<Reverse<Cursor<R>>>,
    buffer: Vec<DataBlock>,
    pending_stream: VecDeque<usize>,
    batch_size: usize,
}

impl<R: Rows> Merger<R> {
    fn create(
        schema: DataSchemaRef,
        streams: Vec<BlockStream>,
        sort_desc: Arc<Vec<SortColumnDescription>>,
        batch_size: usize,
    ) -> Self {
        // We only create a merger when there are at least two streams.
        debug_assert!(streams.len() > 1);
        let heap = BinaryHeap::with_capacity(streams.len());
        let buffer = vec![DataBlock::empty_with_schema(schema.clone()); streams.len()];
        let pending_stream = (0..streams.len()).collect();

        Self {
            schema,
            unsorted_streams: streams,
            heap,
            buffer,
            batch_size,
            sort_desc,
            pending_stream,
        }
    }

    // This method can only be called when there is no data of the stream in the heap.
    async fn poll_pending_stream(&mut self) -> Result<()> {
        while let Some(i) = self.pending_stream.pop_front() {
            debug_assert!(self.buffer[i].is_empty());
            if let Some(block) = self.unsorted_streams[i].next().await? {
                let order_col = block.columns().last().unwrap().value.as_column().unwrap();
                let rows = R::from_column(order_col.clone(), &self.sort_desc)
                    .ok_or_else(|| ErrorCode::BadDataValueType("Order column type mismatched."))?;
                let cursor = Cursor::new(i, rows);
                self.heap.push(Reverse(cursor));
                self.buffer[i] = block;
            }
        }
        Ok(())
    }

    async fn next(&mut self) -> Result<Option<DataBlock>> {
        if !self.pending_stream.is_empty() {
            self.poll_pending_stream().await?;
        }

        if self.heap.is_empty() {
            return Ok(None);
        }

        let mut num_rows = 0;

        // (input_index, row_start, count)
        let mut output_indices = Vec::new();
        let mut temp_sorted_blocks = Vec::new();

        while let Some(Reverse(cursor)) = self.heap.peek() {
            let mut cursor = cursor.clone();
            if self.heap.len() == 1 {
                let start = cursor.row_index;
                let count = (cursor.num_rows() - start).min(self.batch_size - num_rows);
                num_rows += count;
                cursor.row_index += count;
                output_indices.push((cursor.input_index, start, count));
            } else {
                let next_cursor = &find_bigger_child_of_root(&self.heap).0;
                if cursor.last().le(&next_cursor.current()) {
                    // Short Path:
                    // If the last row of current block is smaller than the next cursor,
                    // we can drain the whole block.
                    let start = cursor.row_index;
                    let count = (cursor.num_rows() - start).min(self.batch_size - num_rows);
                    num_rows += count;
                    cursor.row_index += count;
                    output_indices.push((cursor.input_index, start, count));
                } else {
                    // We copy current cursor for advancing,
                    // and we will use this copied cursor to update the top of the heap at last
                    // (let heap adjust itself without popping and pushing any element).
                    let start = cursor.row_index;
                    while !cursor.is_finished()
                        && cursor.le(next_cursor)
                        && num_rows < self.batch_size
                    {
                        // If the cursor is smaller than the next cursor, don't need to push the cursor back to the heap.
                        num_rows += 1;
                        cursor.advance();
                    }
                    output_indices.push((cursor.input_index, start, cursor.row_index - start));
                }
            }

            if !cursor.is_finished() {
                // Update the top of the heap.
                // `self.heap.peek_mut` will return a `PeekMut` object which allows us to modify the top element of the heap.
                // The heap will adjust itself automatically when the `PeekMut` object is dropped (RAII).
                self.heap.peek_mut().unwrap().0 = cursor;
            } else {
                // Pop the current `cursor`.
                self.heap.pop();
                // We have read all rows of this block, need to release the old memory and read a new one.
                let temp_block = DataBlock::take_by_slices_limit_from_blocks(
                    &self.buffer,
                    &output_indices,
                    None,
                );
                self.buffer[cursor.input_index] = DataBlock::empty_with_schema(self.schema.clone());
                temp_sorted_blocks.push(temp_block);
                output_indices.clear();
                self.pending_stream.push_back(cursor.input_index);
                self.poll_pending_stream().await?;
            }

            if num_rows == self.batch_size {
                break;
            }
        }

        if !output_indices.is_empty() {
            let block =
                DataBlock::take_by_slices_limit_from_blocks(&self.buffer, &output_indices, None);
            temp_sorted_blocks.push(block);
        }

        let block = DataBlock::concat(&temp_sorted_blocks)?;
        debug_assert!(block.num_rows() <= self.batch_size);
        Ok(Some(block))
    }
}

pub fn create_transform_sort_spill(
    input: Arc<InputPort>,
    output: Arc<OutputPort>,
    schema: DataSchemaRef,
    sort_desc: Arc<Vec<SortColumnDescription>>,
    spiller: Spiller,
) -> Box<dyn Processor> {
    if sort_desc.len() == 1 {
        let sort_type = schema.field(sort_desc[0].offset).data_type();
        match sort_type {
            DataType::Number(num_ty) => with_number_mapped_type!(|NUM_TYPE| match num_ty {
                NumberDataType::NUM_TYPE => Box::new(TransformSortSpill::<
                    SimpleRows<NumberType<NUM_TYPE>>,
                >::create(
                    input, output, schema, sort_desc, spiller,
                )),
            }),
            DataType::Date => Box::new(TransformSortSpill::<DateRows>::create(
                input, output, schema, sort_desc, spiller,
            )),
            DataType::Timestamp => Box::new(TransformSortSpill::<TimestampRows>::create(
                input, output, schema, sort_desc, spiller,
            )),
            DataType::String => Box::new(TransformSortSpill::<StringRows>::create(
                input, output, schema, sort_desc, spiller,
            )),
            _ => Box::new(TransformSortSpill::<CommonRows>::create(
                input, output, schema, sort_desc, spiller,
            )),
        }
    } else {
        Box::new(TransformSortSpill::<CommonRows>::create(
            input, output, schema, sort_desc, spiller,
        ))
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use common_base::base::tokio;
    use common_exception::Result;
    use common_expression::block_debug::pretty_format_blocks;
    use common_expression::types::DataType;
    use common_expression::types::Int32Type;
    use common_expression::types::NumberDataType;
    use common_expression::DataBlock;
    use common_expression::DataField;
    use common_expression::DataSchemaRefExt;
    use common_expression::FromData;
    use common_expression::SortColumnDescription;
    use common_pipeline_core::processors::InputPort;
    use common_pipeline_core::processors::OutputPort;
    use common_pipeline_transforms::processors::sort::SimpleRows;
    use common_storage::DataOperator;
    use itertools::Itertools;
    use rand::rngs::ThreadRng;
    use rand::Rng;

    use super::TransformSortSpill;
    use crate::pipelines::processors::transforms::transform_sort_spill::BlockStream;
    use crate::sessions::QueryContext;
    use crate::spillers::Spiller;
    use crate::spillers::SpillerConfig;
    use crate::spillers::SpillerType;
    use crate::test_kits::*;

    async fn create_test_transform(
        ctx: Arc<QueryContext>,
    ) -> Result<TransformSortSpill<SimpleRows<Int32Type>>> {
        let op = DataOperator::instance().operator();
        let spiller = Spiller::create(
            ctx.clone(),
            op,
            SpillerConfig::create("_spill_test".to_string()),
            SpillerType::OrderBy,
        );

        let sort_desc = Arc::new(vec![SortColumnDescription {
            offset: 0,
            asc: true,
            nulls_first: true,
            is_nullable: false,
        }]);

        let transform = TransformSortSpill::<SimpleRows<Int32Type>>::create(
            InputPort::create(),
            OutputPort::create(),
            DataSchemaRefExt::create(vec![DataField::new(
                "a",
                DataType::Number(NumberDataType::Int32),
            )]),
            sort_desc,
            spiller,
        );

        Ok(transform)
    }

    /// Returns (input, expected)
    fn basic_test_data() -> (Vec<DataBlock>, DataBlock) {
        let data = vec![
            vec![1, 3, 5, 7],
            vec![1, 2, 3, 4],
            vec![1, 1, 1, 1],
            vec![1, 10, 100, 2000],
            vec![0, 2, 4, 5],
        ];

        let input = data
            .clone()
            .into_iter()
            .map(|v| DataBlock::new_from_columns(vec![Int32Type::from_data(v)]))
            .collect::<Vec<_>>();
        let result = data.into_iter().flatten().sorted().collect::<Vec<_>>();
        let result = DataBlock::new_from_columns(vec![Int32Type::from_data(result)]);

        (input, result)
    }

    /// Returns (input, expected, batch_size, num_merge)
    fn random_test_data(rng: &mut ThreadRng) -> (Vec<DataBlock>, DataBlock, usize, usize) {
        let random_batch_size = rng.gen_range(1..=10);
        let random_num_streams = rng.gen_range(5..=10);
        let random_num_merge = rng.gen_range(2..=10);

        let random_data = (0..random_num_streams)
            .map(|_| {
                let mut data = (0..random_batch_size)
                    .map(|_| rng.gen_range(0..=1000))
                    .collect::<Vec<_>>();
                data.sort();
                data
            })
            .collect::<Vec<_>>();

        let input = random_data
            .clone()
            .into_iter()
            .map(|v| DataBlock::new_from_columns(vec![Int32Type::from_data(v)]))
            .collect::<Vec<_>>();
        let result = random_data
            .into_iter()
            .flatten()
            .sorted()
            .collect::<Vec<_>>();
        let result = DataBlock::new_from_columns(vec![Int32Type::from_data(result)]);

        (input, result, random_batch_size, random_num_merge)
    }

    async fn test(
        ctx: Arc<QueryContext>,
        mut input: Vec<DataBlock>,
        expected: DataBlock,
        batch_size: usize,
        num_merge: usize,
        has_memory_block: bool,
    ) -> Result<()> {
        let mut transform = create_test_transform(ctx).await?;

        transform.num_merge = num_merge;
        transform.batch_size = batch_size;

        let memory_block = if has_memory_block { input.pop() } else { None };

        for data in input {
            transform.spill(data).await?;
        }
        transform.merge_sort(memory_block).await?;

        debug_assert_eq!(transform.unmerged_blocks.len(), 1);
        let mut block_stream = BlockStream::Spilled((
            transform.unmerged_blocks[0].clone(),
            Arc::new(transform.spiller.clone()),
        ));

        let mut result = Vec::new();
        while let Some(block) = block_stream.next().await? {
            result.push(block);
        }

        let result = pretty_format_blocks(&result).unwrap();
        let expected = pretty_format_blocks(&[expected]).unwrap();
        assert_eq!(
            expected, result,
            "batch_size: {}, num_merge: {}\nexpected:\n{}\nactual:\n{}",
            batch_size, num_merge, expected, result
        );

        Ok(())
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn test_two_way_merge_sort() -> Result<()> {
        let fixture = TestFixture::setup().await?;
        let ctx = fixture.new_query_ctx().await?;
        let (input, expected) = basic_test_data();

        test(ctx, input, expected, 4, 2, false).await
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn test_two_way_merge_sort_with_memory_block() -> Result<()> {
        let fixture = TestFixture::setup().await?;
        let ctx = fixture.new_query_ctx().await?;
        let (input, expected) = basic_test_data();

        test(ctx, input, expected, 4, 2, true).await
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn test_three_way_merge_sort() -> Result<()> {
        let fixture = TestFixture::setup().await?;
        let ctx = fixture.new_query_ctx().await?;
        let (input, expected) = basic_test_data();

        test(ctx.clone(), input.clone(), expected.clone(), 4, 3, false).await?;
        test(ctx, input, expected, 4, 3, true).await
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn test_large_num_merge() -> Result<()> {
        // Test if `num_merge` is bigger than the number of streams.
        let fixture = TestFixture::setup().await?;
        let ctx = fixture.new_query_ctx().await?;
        let (input, expected) = basic_test_data();

        test(ctx.clone(), input.clone(), expected.clone(), 4, 10, false).await?;
        test(ctx, input, expected, 4, 10, true).await
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn fuzz_test() -> Result<()> {
        let fixture = TestFixture::setup().await?;
        let ctx = fixture.new_query_ctx().await?;

        let mut rng = rand::thread_rng();
        for _ in 0..10 {
            let (input, expected, batch_size, num_merge) = random_test_data(&mut rng);
            test(
                ctx.clone(),
                input.clone(),
                expected.clone(),
                batch_size,
                num_merge,
                false,
            )
            .await?;
            test(
                ctx.clone(),
                input.clone(),
                expected.clone(),
                batch_size,
                num_merge,
                true,
            )
            .await?;
        }

        Ok(())
    }
}
