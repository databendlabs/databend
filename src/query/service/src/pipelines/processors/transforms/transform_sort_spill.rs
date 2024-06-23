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
use databend_common_expression::types::NumberDataType;
use databend_common_expression::types::NumberType;
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
use databend_common_pipeline_transforms::processors::sort::CommonRows;
use databend_common_pipeline_transforms::processors::sort::DateRows;
use databend_common_pipeline_transforms::processors::sort::HeapMerger;
use databend_common_pipeline_transforms::processors::sort::Rows;
use databend_common_pipeline_transforms::processors::sort::SimpleRows;
use databend_common_pipeline_transforms::processors::sort::SortSpillMeta;
use databend_common_pipeline_transforms::processors::sort::SortSpillMetaWithParams;
use databend_common_pipeline_transforms::processors::sort::SortedStream;
use databend_common_pipeline_transforms::processors::sort::StringRows;
use databend_common_pipeline_transforms::processors::sort::TimestampRows;

use crate::spillers::Spiller;

enum State {
    /// The initial state of the processor.
    Init,
    /// This state means the processor will never spill incoming blocks.
    NoSpill,
    /// This state means the processor will spill incoming blocks except the last block.
    Spill,
    /// This state means the processor is doing external merge sort.
    Merging,
    /// This state is used to merge the last few sorted streams and output directly.
    MergeFinal,
    /// Finish the process.
    Finish,
}

pub struct TransformSortSpill<R: Rows> {
    input: Arc<InputPort>,
    output: Arc<OutputPort>,
    schema: DataSchemaRef,
    output_order_col: bool,
    limit: Option<usize>,

    input_data: Option<DataBlock>,
    output_data: Option<DataBlock>,

    state: State,
    spiller: Spiller,

    batch_size: usize,
    /// Blocks to merge one time.
    num_merge: usize,
    /// Unmerged list of blocks. Each list are sorted.
    unmerged_blocks: VecDeque<VecDeque<String>>,

    /// If `ummerged_blocks.len()` < `num_merge`,
    /// we can use a final merger to merge the last few sorted streams to reduce IO.
    final_merger: Option<HeapMerger<R, BlockStream>>,

    sort_desc: Arc<Vec<SortColumnDescription>>,

    _r: PhantomData<R>,
}

#[inline(always)]
fn need_spill(block: &DataBlock) -> bool {
    block
        .get_meta()
        .and_then(SortSpillMeta::downcast_ref_from)
        .is_some()
        || block
            .get_meta()
            .and_then(SortSpillMetaWithParams::downcast_ref_from)
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

        if !self.output.can_push() {
            self.input.set_not_need_data();
            return Ok(Event::NeedConsume);
        }

        if let Some(block) = self.output_data.take() {
            debug_assert!(matches!(self.state, State::MergeFinal | State::Finish));
            self.output_block(block);
            return Ok(Event::NeedConsume);
        }

        if matches!(self.state, State::Finish) {
            debug_assert!(self.input.is_finished());
            self.output.finish();
            return Ok(Event::Finished);
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
                        let meta =
                            SortSpillMetaWithParams::downcast_ref_from(block.get_meta().unwrap())
                                .unwrap();
                        self.batch_size = meta.batch_size;
                        self.num_merge = meta.num_merge;

                        self.input_data = Some(block);
                        self.state = State::Spill;
                        Ok(Event::Async)
                    } else {
                        // If we get a memory block at initial state, it means we will never spill data.
                        debug_assert!(self.spiller.columns_layout.is_empty());
                        self.output_block(block);
                        self.state = State::NoSpill;
                        Ok(Event::NeedConsume)
                    }
                }
                State::NoSpill => {
                    debug_assert!(!need_spill(&block));
                    self.output_block(block);
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
                State::Merging | State::MergeFinal => Ok(Event::Async),
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
            State::MergeFinal => {
                debug_assert!(self.final_merger.is_some());
                debug_assert!(self.unmerged_blocks.is_empty());
                let merger = self.final_merger.as_mut().unwrap();
                if let Some(block) = merger.async_next_block().await? {
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
        limit: Option<usize>,
        spiller: Spiller,
        output_order_col: bool,
    ) -> Self {
        Self {
            input,
            output,
            schema,
            limit,
            output_order_col,
            input_data: None,
            output_data: None,
            spiller,
            state: State::Init,
            num_merge: 0,
            unmerged_blocks: VecDeque::new(),
            final_merger: None,
            batch_size: 0,
            sort_desc,
            _r: PhantomData,
        }
    }

    #[inline(always)]
    fn output_block(&self, mut block: DataBlock) {
        if !self.output_order_col {
            block.pop_columns(1);
        }
        self.output.push_data(Ok(block));
    }

    async fn spill(&mut self, block: DataBlock) -> Result<()> {
        debug_assert!(self.num_merge >= 2 && self.batch_size > 0);

        let location = self.spiller.spill_block(block).await?;

        self.unmerged_blocks.push_back(vec![location].into());
        Ok(())
    }

    fn create_merger(
        &mut self,
        memory_block: Option<DataBlock>,
        num_streams: usize,
    ) -> HeapMerger<R, BlockStream> {
        debug_assert!(num_streams <= self.unmerged_blocks.len() + memory_block.is_some() as usize);

        let mut streams = Vec::with_capacity(num_streams);
        if let Some(block) = memory_block {
            streams.push(BlockStream::Block(Some(block)));
        }

        let spiller_snapshot = Arc::new(self.spiller.clone());
        for _ in 0..num_streams - streams.len() {
            let files = self.unmerged_blocks.pop_front().unwrap();
            for file in files.iter() {
                self.spiller.columns_layout.remove(file);
            }
            let stream = BlockStream::Spilled((files, spiller_snapshot.clone()));
            streams.push(stream);
        }

        HeapMerger::<R, BlockStream>::create(
            self.schema.clone(),
            streams,
            self.sort_desc.clone(),
            self.batch_size,
            self.limit,
        )
    }

    /// Do an external merge sort until there is only one sorted stream.
    /// If `block` is not [None], we need to merge it with spilled files.
    async fn merge_sort(&mut self, mut block: Option<DataBlock>) -> Result<()> {
        while (self.unmerged_blocks.len() + block.is_some() as usize) > self.num_merge {
            let b = block.take();
            self.merge_sort_one_round(b).await?;
        }

        // Deal with a corner case:
        // If this thread only has one spilled file.
        if self.unmerged_blocks.len() == 1 {
            let files = self.unmerged_blocks.pop_front().unwrap();
            debug_assert!(files.len() == 1);

            let block = self.spiller.read_spilled_file(&files[0]).await?;

            self.output_data = Some(block);
            self.state = State::Finish;

            return Ok(());
        }

        let num_streams = self.unmerged_blocks.len() + block.is_some() as usize;
        debug_assert!(num_streams <= self.num_merge && num_streams > 1);

        self.final_merger = Some(self.create_merger(block, num_streams));
        self.state = State::MergeFinal;

        Ok(())
    }

    /// Merge certain number of sorted streams to one sorted stream.
    async fn merge_sort_one_round(&mut self, block: Option<DataBlock>) -> Result<()> {
        let num_streams =
            (self.unmerged_blocks.len() + block.is_some() as usize).min(self.num_merge);
        debug_assert!(num_streams > 1);
        let mut merger = self.create_merger(block, num_streams);

        let mut spilled = VecDeque::new();
        while let Some(block) = merger.async_next_block().await? {
            let location = self.spiller.spill_block(block).await?;

            spilled.push_back(location);
        }

        debug_assert!(merger.is_finished());
        self.unmerged_blocks.push_back(spilled);

        Ok(())
    }
}

enum BlockStream {
    Spilled((VecDeque<String>, Arc<Spiller>)),
    Block(Option<DataBlock>),
}

#[async_trait::async_trait]
impl SortedStream for BlockStream {
    async fn async_next(&mut self) -> Result<(Option<(DataBlock, Column)>, bool)> {
        let block = match self {
            BlockStream::Block(block) => block.take(),
            BlockStream::Spilled((files, spiller)) => {
                if let Some(file) = files.pop_front() {
                    let block = spiller.read_spilled_file(&file).await?;

                    Some(block)
                } else {
                    None
                }
            }
        };
        Ok((
            block.map(|b| {
                let col = b.get_last_column().clone();
                (b, col)
            }),
            false,
        ))
    }
}

pub fn create_transform_sort_spill(
    input: Arc<InputPort>,
    output: Arc<OutputPort>,
    schema: DataSchemaRef,
    sort_desc: Arc<Vec<SortColumnDescription>>,
    limit: Option<usize>,
    spiller: Spiller,
    output_order_col: bool,
) -> Box<dyn Processor> {
    if sort_desc.len() == 1 {
        let sort_type = schema.field(sort_desc[0].offset).data_type();
        match sort_type {
            DataType::Number(num_ty) => with_number_mapped_type!(|NUM_TYPE| match num_ty {
                NumberDataType::NUM_TYPE => Box::new(TransformSortSpill::<
                    SimpleRows<NumberType<NUM_TYPE>>,
                >::create(
                    input,
                    output,
                    schema,
                    sort_desc,
                    limit,
                    spiller,
                    output_order_col
                )),
            }),
            DataType::Date => Box::new(TransformSortSpill::<DateRows>::create(
                input,
                output,
                schema,
                sort_desc,
                limit,
                spiller,
                output_order_col,
            )),
            DataType::Timestamp => Box::new(TransformSortSpill::<TimestampRows>::create(
                input,
                output,
                schema,
                sort_desc,
                limit,
                spiller,
                output_order_col,
            )),
            DataType::String => Box::new(TransformSortSpill::<StringRows>::create(
                input,
                output,
                schema,
                sort_desc,
                limit,
                spiller,
                output_order_col,
            )),
            _ => Box::new(TransformSortSpill::<CommonRows>::create(
                input,
                output,
                schema,
                sort_desc,
                limit,
                spiller,
                output_order_col,
            )),
        }
    } else {
        Box::new(TransformSortSpill::<CommonRows>::create(
            input,
            output,
            schema,
            sort_desc,
            limit,
            spiller,
            output_order_col,
        ))
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use databend_common_base::base::tokio;
    use databend_common_exception::Result;
    use databend_common_expression::block_debug::pretty_format_blocks;
    use databend_common_expression::types::DataType;
    use databend_common_expression::types::Int32Type;
    use databend_common_expression::types::NumberDataType;
    use databend_common_expression::DataBlock;
    use databend_common_expression::DataField;
    use databend_common_expression::DataSchemaRefExt;
    use databend_common_expression::FromData;
    use databend_common_expression::SortColumnDescription;
    use databend_common_pipeline_core::processors::InputPort;
    use databend_common_pipeline_core::processors::OutputPort;
    use databend_common_pipeline_transforms::processors::sort::SimpleRows;
    use databend_common_storage::DataOperator;
    use itertools::Itertools;
    use rand::rngs::ThreadRng;
    use rand::Rng;

    use super::TransformSortSpill;
    use crate::sessions::QueryContext;
    use crate::spillers::Spiller;
    use crate::spillers::SpillerConfig;
    use crate::spillers::SpillerType;
    use crate::test_kits::*;

    async fn create_test_transform(
        ctx: Arc<QueryContext>,
        limit: Option<usize>,
    ) -> Result<TransformSortSpill<SimpleRows<Int32Type>>> {
        let op = DataOperator::instance().operator();
        let spiller = Spiller::create(
            ctx.clone(),
            op,
            SpillerConfig::create("_spill_test".to_string()),
            SpillerType::OrderBy,
        )?;

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
            limit,
            spiller,
            false,
        );

        Ok(transform)
    }

    /// Returns (input, expected)
    fn basic_test_data(limit: Option<usize>) -> (Vec<DataBlock>, DataBlock) {
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
        let result = if let Some(limit) = limit {
            result.into_iter().take(limit).collect::<Vec<_>>()
        } else {
            result
        };
        let result = DataBlock::new_from_columns(vec![Int32Type::from_data(result)]);

        (input, result)
    }

    /// Returns (input, expected, batch_size, num_merge)
    fn random_test_data(
        rng: &mut ThreadRng,
        limit: Option<usize>,
    ) -> (Vec<DataBlock>, DataBlock, usize, usize) {
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
        let result = if let Some(limit) = limit {
            result.into_iter().take(limit).collect::<Vec<_>>()
        } else {
            result
        };
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
        limit: Option<usize>,
    ) -> Result<()> {
        let mut transform = create_test_transform(ctx, limit).await?;

        transform.num_merge = num_merge;
        transform.batch_size = batch_size;

        let memory_block = if has_memory_block { input.pop() } else { None };

        for data in input {
            transform.spill(data).await?;
        }
        transform.merge_sort(memory_block).await?;

        let mut result = Vec::new();

        debug_assert!(transform.final_merger.is_some());
        debug_assert!(transform.unmerged_blocks.is_empty());
        let merger = transform.final_merger.as_mut().unwrap();
        while let Some(block) = merger.async_next_block().await? {
            result.push(block);
        }
        debug_assert!(merger.is_finished());

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
        let (input, expected) = basic_test_data(None);

        test(ctx, input, expected, 4, 2, false, None).await
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn test_two_way_merge_sort_with_memory_block() -> Result<()> {
        let fixture = TestFixture::setup().await?;
        let ctx = fixture.new_query_ctx().await?;
        let (input, expected) = basic_test_data(None);

        test(ctx, input, expected, 4, 2, true, None).await
    }

    async fn basic_test(
        ctx: Arc<QueryContext>,
        batch_size: usize,
        num_merge: usize,
        limit: Option<usize>,
    ) -> Result<()> {
        let (input, expected) = basic_test_data(limit);

        test(
            ctx.clone(),
            input.clone(),
            expected.clone(),
            batch_size,
            num_merge,
            false,
            limit,
        )
        .await?;
        test(ctx, input, expected, batch_size, num_merge, true, limit).await
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn test_three_way_merge_sort() -> Result<()> {
        let fixture = TestFixture::setup().await?;
        let ctx = fixture.new_query_ctx().await?;

        basic_test(ctx.clone(), 4, 3, None).await?;
        basic_test(ctx, 4, 3, Some(2)).await
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn test_large_num_merge() -> Result<()> {
        // Test if `num_merge` is bigger than the number of streams.
        let fixture = TestFixture::setup().await?;
        let ctx = fixture.new_query_ctx().await?;

        basic_test(ctx.clone(), 4, 10, None).await?;
        basic_test(ctx, 4, 10, Some(2)).await
    }

    async fn random_test(
        ctx: Arc<QueryContext>,
        rng: &mut ThreadRng,
        limit: Option<usize>,
    ) -> Result<()> {
        let (input, expected, batch_size, num_merge) = random_test_data(rng, limit);
        test(
            ctx.clone(),
            input.clone(),
            expected.clone(),
            batch_size,
            num_merge,
            false,
            limit,
        )
        .await?;
        test(
            ctx.clone(),
            input.clone(),
            expected.clone(),
            batch_size,
            num_merge,
            true,
            limit,
        )
        .await
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn fuzz_test() -> Result<()> {
        let fixture = TestFixture::setup().await?;
        let ctx = fixture.new_query_ctx().await?;

        let mut rng = rand::thread_rng();
        for _ in 0..10 {
            random_test(ctx.clone(), &mut rng, None).await?;

            let limit = rng.gen_range(1..=5);
            random_test(ctx.clone(), &mut rng, Some(limit)).await?;
        }

        Ok(())
    }
}
