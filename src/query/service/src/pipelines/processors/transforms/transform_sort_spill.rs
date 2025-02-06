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

pub struct TransformSortSpill<A: SortAlgorithm> {
    input: Arc<InputPort>,
    output: Arc<OutputPort>,
    schema: DataSchemaRef,
    output_order_col: bool,
    limit: Option<usize>,

    input_data: Option<DataBlock>,
    output_data: Option<DataBlock>,

    state: State,
    spiller: Spiller,

    batch_rows: usize,
    /// Blocks to merge one time.
    num_merge: usize,
    /// Unmerged list of blocks. Each list are sorted.
    unmerged_blocks: VecDeque<VecDeque<Location>>,

    /// If `ummerged_blocks.len()` < `num_merge`,
    /// we can use a final merger to merge the last few sorted streams to reduce IO.
    final_merger: Option<Merger<A, BlockStream>>,
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
impl<A> Processor for TransformSortSpill<A>
where
    A: SortAlgorithm + 'static,
    A::Rows: 'static,
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
            assert!(matches!(self.state, State::MergeFinal | State::Finish));
            self.output_block(block);
            return Ok(Event::NeedConsume);
        }

        if matches!(self.state, State::Finish) {
            assert!(self.input.is_finished());
            self.output.finish();
            return Ok(Event::Finished);
        }

        if self.input_data.is_some() {
            return Ok(Event::Async);
        }

        if self.input.has_data() {
            let mut block = self.input.pull_data().unwrap()?;
            let meta = take_spill_meta(&mut block);
            return match &self.state {
                State::Init => {
                    match meta {
                        Some(Some(params)) => {
                            // Need to spill this block.
                            self.batch_rows = params.batch_rows;
                            self.num_merge = params.num_merge;

                            self.input_data = Some(block);
                            self.state = State::Spill;
                            Ok(Event::Async)
                        }
                        Some(None) => unreachable!(),
                        None => {
                            self.output_block(block);
                            self.state = State::NoSpill;
                            Ok(Event::NeedConsume)
                        }
                    }
                }
                State::NoSpill => {
                    debug_assert!(meta.is_none());
                    self.output_block(block);
                    self.state = State::NoSpill;
                    Ok(Event::NeedConsume)
                }
                State::Spill => {
                    if meta.is_none() {
                        // It means we get the last block.
                        // We can launch external merge sort now.
                        self.input.finish();
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

impl<A> TransformSortSpill<A>
where
    A: SortAlgorithm + 'static,
    A::Rows: 'static,
{
    pub fn create(
        input: Arc<InputPort>,
        output: Arc<OutputPort>,
        schema: DataSchemaRef,
        _sort_desc: Arc<Vec<SortColumnDescription>>,
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
            batch_rows: 0,
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
        debug_assert!(self.num_merge >= 2 && self.batch_rows > 0);

        let location = self.spiller.spill(vec![block]).await?;

        self.unmerged_blocks.push_back(vec![location].into());
        Ok(())
    }

    fn create_merger(
        &mut self,
        memory_block: Option<DataBlock>,
        num_streams: usize,
    ) -> Merger<A, BlockStream> {
        debug_assert!(num_streams <= self.unmerged_blocks.len() + memory_block.is_some() as usize);

        let mut streams = Vec::with_capacity(num_streams);
        if let Some(block) = memory_block {
            streams.push(BlockStream::Block(Some(block)));
        }

        let spiller_snapshot = Arc::new(self.spiller.clone());
        for _ in 0..num_streams - streams.len() {
            let files = self.unmerged_blocks.pop_front().unwrap();
            let stream = BlockStream::Spilled((files, spiller_snapshot.clone()));
            streams.push(stream);
        }

        Merger::<A, BlockStream>::create(self.schema.clone(), streams, self.batch_rows, self.limit)
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
            let location = self.spiller.spill(vec![block]).await?;

            spilled.push_back(location);
        }

        debug_assert!(merger.is_finished());
        self.unmerged_blocks.push_back(spilled);

        Ok(())
    }
}

enum BlockStream {
    Spilled((VecDeque<Location>, Arc<Spiller>)),
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
    enable_loser_tree: bool,
) -> Box<dyn Processor> {
    macro_rules! create_sort {
        ($algo: ident, $row: ty) => {
            Box::new(TransformSortSpill::<$algo<$row>>::create(
                input,
                output,
                schema,
                sort_desc,
                limit,
                spiller,
                output_order_col,
            ))
        };
        ($algo: ident, $asc: ident, $data_type: ty) => {
            Box::new(TransformSortSpill::<$algo<$asc<$data_type>>>::create(
                input,
                output,
                schema,
                sort_desc,
                limit,
                spiller,
                output_order_col,
            ))
        };
    }

    if sort_desc.len() == 1 {
        let sort_type = schema.field(sort_desc[0].offset).data_type();
        let asc = sort_desc[0].asc;

        macro_rules! create_simple {
            ($data_type: ty) => {
                match (enable_loser_tree, asc) {
                    (true, true) => create_sort!(LoserTreeSort, SimpleRowsAsc, $data_type),
                    (true, false) => create_sort!(LoserTreeSort, SimpleRowsDesc, $data_type),
                    (false, true) => create_sort!(HeapSort, SimpleRowsAsc, $data_type),
                    (false, false) => create_sort!(HeapSort, SimpleRowsDesc, $data_type),
                }
            };
        }

        match sort_type {
            DataType::Number(num_ty) => {
                return with_number_mapped_type!(|NUM_TYPE| match num_ty {
                    NumberDataType::NUM_TYPE => create_simple!(NumberType<NUM_TYPE>),
                });
            }
            DataType::Date => return create_simple!(DateType),
            DataType::Timestamp => return create_simple!(TimestampType),
            DataType::String => return create_simple!(StringType),
            _ => (),
        };
    }

    if enable_loser_tree {
        create_sort!(LoserTreeSort, CommonRows)
    } else {
        create_sort!(HeapSort, CommonRows)
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use databend_common_base::base::tokio;
    use databend_common_catalog::table_context::TableContext;
    use databend_common_expression::block_debug::pretty_format_blocks;
    use databend_common_expression::types::DataType;
    use databend_common_expression::types::Int32Type;
    use databend_common_expression::DataField;
    use databend_common_expression::DataSchemaRefExt;
    use databend_common_expression::FromData;
    use databend_common_storage::DataOperator;
    use itertools::Itertools;
    use rand::rngs::ThreadRng;
    use rand::Rng;

    use super::*;
    use crate::sessions::QueryContext;
    use crate::spillers::SpillerConfig;
    use crate::spillers::SpillerType;
    use crate::test_kits::*;

    async fn create_test_transform<A>(
        ctx: Arc<QueryContext>,
        limit: Option<usize>,
    ) -> Result<TransformSortSpill<A>>
    where
        A: SortAlgorithm + 'static,
        A::Rows: 'static,
    {
        let op = DataOperator::instance().spill_operator();
        let spill_config = SpillerConfig {
            spiller_type: SpillerType::OrderBy,
            location_prefix: "_spill_test".to_string(),
            disk_spill: None,
            use_parquet: ctx.get_settings().get_spilling_file_format()?.is_parquet(),
        };

        let spiller = Spiller::create(ctx.clone(), op, spill_config)?;

        let sort_desc = Arc::new(vec![SortColumnDescription {
            offset: 0,
            asc: true,
            nulls_first: true,
        }]);

        let transform = TransformSortSpill::<A>::create(
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

    /// Returns (input, expected, batch_rows, num_merge)
    fn random_test_data(
        rng: &mut ThreadRng,
        limit: Option<usize>,
    ) -> (Vec<DataBlock>, DataBlock, usize, usize) {
        let random_batch_rows = rng.gen_range(1..=10);
        let random_num_streams = rng.gen_range(5..=10);
        let random_num_merge = rng.gen_range(2..=10);

        let random_data = (0..random_num_streams)
            .map(|_| {
                let mut data = (0..random_batch_rows)
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

        (input, result, random_batch_rows, random_num_merge)
    }

    async fn test(
        ctx: Arc<QueryContext>,
        mut input: Vec<DataBlock>,
        expected: DataBlock,
        batch_rows: usize,
        num_merge: usize,
        has_memory_block: bool,
        limit: Option<usize>,
    ) -> Result<()> {
        let mut transform =
            create_test_transform::<LoserTreeSort<SimpleRowsAsc<Int32Type>>>(ctx, limit).await?;

        transform.num_merge = num_merge;
        transform.batch_rows = batch_rows;

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
            "batch_rows: {}, num_merge: {}\nexpected:\n{}\nactual:\n{}",
            batch_rows, num_merge, expected, result
        );

        Ok(())
    }

    async fn basic_test(
        ctx: Arc<QueryContext>,
        batch_rows: usize,
        num_merge: usize,
        limit: Option<usize>,
    ) -> Result<()> {
        let (input, expected) = basic_test_data(limit);

        test(
            ctx.clone(),
            input.clone(),
            expected.clone(),
            batch_rows,
            num_merge,
            false,
            limit,
        )
        .await?;
        test(ctx, input, expected, batch_rows, num_merge, true, limit).await
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
        let (input, expected, batch_rows, num_merge) = random_test_data(rng, limit);
        test(
            ctx.clone(),
            input.clone(),
            expected.clone(),
            batch_rows,
            num_merge,
            false,
            limit,
        )
        .await?;
        test(
            ctx.clone(),
            input.clone(),
            expected.clone(),
            batch_rows,
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
