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

use std::collections::VecDeque;
use std::fmt;
use std::fmt::Debug;
use std::fmt::Formatter;
use std::mem;
use std::sync::Arc;

use databend_common_column::bitmap::MutableBitmap;
use databend_common_exception::Result;
use databend_common_expression::sampler::FixedRateSampler;
use databend_common_expression::Column;
use databend_common_expression::DataBlock;
use databend_common_expression::DataSchemaRef;
use databend_common_expression::SortColumnDescription;
use databend_common_pipeline_transforms::processors::sort::algorithm::SortAlgorithm;
use databend_common_pipeline_transforms::processors::sort::Merger;
use databend_common_pipeline_transforms::processors::sort::Rows;
use databend_common_pipeline_transforms::processors::sort::SortedStream;
use databend_common_pipeline_transforms::processors::SortSpillParams;
use rand::rngs::StdRng;
use rand::SeedableRng;

use crate::spillers::Location;
use crate::spillers::Spiller;

enum Sampler {
    Uninit,
    Collect(FixedRateSampler<StdRng>),
    /// Partition boundaries for restoring and sorting blocks, stored in reverse order of Column.
    /// Each boundary represents a cutoff point where data less than or equal to it belongs to one partition.
    Finish(Vec<Column>),
}

pub struct TransformStreamSortSpill<A: SortAlgorithm> {
    schema: DataSchemaRef,
    sort_row_offset: usize,
    limit: Option<usize>,
    spiller: Arc<Spiller>,

    pub output_data: Option<DataBlock>,

    sampler: Sampler,
    cur_bound: Option<A::Rows>,

    batch_rows: usize,
    /// Blocks to merge one time.
    num_merge: usize,

    subsequent: Vec<BoundBlockStream<A::Rows>>,
    current: Vec<BoundBlockStream<A::Rows>>,

    output_merger: Option<Merger<A, BoundBlockStream<A::Rows>>>,
}

impl<A> TransformStreamSortSpill<A>
where A: SortAlgorithm
{
    pub fn new(
        schema: DataSchemaRef,
        limit: Option<usize>,
        spiller: Arc<Spiller>,
        sort_row_offset: usize,
    ) -> Self {
        Self {
            schema,
            sort_row_offset,
            limit,
            output_data: None,
            spiller,
            sampler: Sampler::Uninit,
            cur_bound: None,
            batch_rows: 0,
            num_merge: 0,
            subsequent: Vec::new(),
            current: Vec::new(),
            output_merger: None,
        }
    }

    pub fn init_spill(&mut self, params: SortSpillParams) {
        debug_assert!(matches!(self.sampler, Sampler::Uninit));

        self.batch_rows = params.batch_rows;
        self.num_merge = params.num_merge;

        log::info!(
            "batch_rows {} num_merge {}",
            params.batch_rows,
            params.num_merge
        );

        self.sampler = Sampler::Collect(
            FixedRateSampler::new(
                vec![self.sort_row_offset],
                self.batch_rows,
                self.batch_rows * self.num_merge,
                self.batch_rows,
                StdRng::seed_from_u64(rand::random()),
            )
            .unwrap(),
        );
    }

    pub fn sort_input_data(&mut self, mut input_data: Vec<DataBlock>) -> Result<()> {
        let Sampler::Collect(sampler) = &mut self.sampler else {
            unreachable!()
        };

        for data in &input_data {
            sampler.add_block(data.clone());
        }
        sampler.compact_blocks(false);

        let sorted = if input_data.len() == 1 {
            let data = input_data.pop().unwrap();
            vec![SpillableBlock::new(data, self.sort_row_offset)].into()
        } else {
            let streams = input_data
                .drain(..)
                .map(|data| DataBlockStream::new(data, self.sort_row_offset))
                .collect();
            let mut merger =
                Merger::<A, _>::create(self.schema.clone(), streams, self.batch_rows, self.limit);

            let mut sorted = VecDeque::new();
            while let Some(data) = merger.next_block()? {
                sorted.push_back(SpillableBlock::new(data, self.sort_row_offset));
            }
            debug_assert!(merger.is_finished());
            sorted
        };

        let stream = self.new_stream(sorted, None);
        self.subsequent.push(stream);
        Ok(())
    }

    pub async fn subsequent_spill_last(&mut self, target_rows: usize) -> Result<()> {
        let Some(s) = self.subsequent.last_mut() else {
            return Ok(());
        };

        let mut released = 0;
        for b in s.blocks.iter_mut().rev() {
            if b.data.is_some() {
                b.spill(&self.spiller).await?;
                released += b.rows;
            }
            if released >= target_rows {
                break;
            }
        }

        Ok(())
    }

    pub async fn on_restore(&mut self) -> Result<bool> {
        if let Sampler::Collect(sampler) = &mut self.sampler {
            sampler.compact_blocks(true);
            let sampled_rows = std::mem::take(&mut sampler.dense_blocks);
            self.sampler = Sampler::Finish(self.determine_bounds(sampled_rows)?)
        }

        if self.output_merger.is_some() {
            return self.restore_and_output().await;
        }

        if self.subsequent.is_empty() && self.current.is_empty() {
            return Ok(true);
        }

        while self.current.is_empty() {
            self.choice_streams_by_bound();
        }

        if self.current.len() > self.num_merge {
            self.merge_current().await?;
            Ok(false)
        } else {
            self.restore_and_output().await
        }
    }

    async fn merge_current(&mut self) -> Result<()> {
        self.subsequent_spill_all().await?;
        for (i, s) in self.current.iter_mut().rev().enumerate() {
            if i < self.num_merge {
                s.spill(1).await?;
            } else {
                s.spill(0).await?;
            }
        }

        let streams = self
            .current
            .drain(self.current.len() - self.num_merge..)
            .collect();

        let mut merger =
            Merger::<A, _>::create(self.schema.clone(), streams, self.batch_rows, None);

        let mut sorted = VecDeque::new();
        while let Some(data) = merger.async_next_block().await? {
            let mut block = SpillableBlock::new(data, self.sort_row_offset);
            block.spill(&self.spiller).await?;
            sorted.push_back(block);
        }
        debug_assert!(merger.is_finished());

        let stream = self.new_stream(sorted, self.cur_bound.clone());
        self.current.insert(0, stream);
        self.subsequent
            .extend(merger.streams().into_iter().filter(|s| !s.is_empty()));
        Ok(())
    }

    async fn restore_and_output(&mut self) -> Result<bool> {
        let merger = match self.output_merger.as_mut() {
            Some(merger) => merger,
            None => {
                debug_assert!(!self.current.is_empty());
                if self.current.len() == 1 {
                    let mut s = self.current.pop().unwrap();
                    s.restore_first().await?;
                    self.output_data = Some(s.take_next_bounded_block());

                    if !s.is_empty() {
                        if s.should_include_first() {
                            self.current.push(s);
                        } else {
                            self.subsequent.push(s);
                        }
                        return Ok(false);
                    }

                    return Ok(self.subsequent.is_empty());
                }

                self.sort_spill().await?;

                let streams = mem::take(&mut self.current);
                let merger =
                    Merger::<A, _>::create(self.schema.clone(), streams, self.batch_rows, None);
                self.output_merger.insert(merger)
            }
        };

        let Some(data) = merger.async_next_block().await? else {
            debug_assert!(merger.is_finished());
            let streams = self.output_merger.take().unwrap().streams();
            self.subsequent
                .extend(streams.into_iter().filter(|s| !s.is_empty()));
            return Ok(self.subsequent.is_empty());
        };

        let mut sorted = BoundBlockStream {
            blocks: VecDeque::new(),
            bound: self.cur_bound.clone(),
            sort_row_offset: self.sort_row_offset,
            spiller: self.spiller.clone(),
        };
        sorted
            .blocks
            .push_back(SpillableBlock::new(data, self.sort_row_offset));

        if sorted.should_include_first() {
            self.output_data = Some(sorted.take_next_bounded_block());
            if sorted.is_empty() {
                return Ok(false);
            }
        }

        while let Some(data) = merger.async_next_block().await? {
            let mut block = SpillableBlock::new(data, self.sort_row_offset);
            block.spill(&self.spiller).await?;
            sorted.blocks.push_back(block);
        }
        debug_assert!(merger.is_finished());

        if !sorted.is_empty() {
            self.subsequent.push(sorted);
        }
        let streams = self.output_merger.take().unwrap().streams();
        self.subsequent
            .extend(streams.into_iter().filter(|s| !s.is_empty()));
        Ok(self.subsequent.is_empty())
    }

    async fn sort_spill(&mut self) -> Result<()> {
        let need = self
            .current
            .iter()
            .map(|s| if s.blocks[0].data.is_none() { 1 } else { 0 })
            .sum::<usize>()
            * self.batch_rows;

        if need + self.subsequent_memory_rows() + self.current_memory_rows() < self.max_rows() {
            return Ok(());
        }

        let mut unspilled = self
            .current
            .iter_mut()
            .chain(self.subsequent.iter_mut())
            .flat_map(|s| s.blocks.iter_mut())
            .filter(|s| s.data.is_some())
            .collect::<Vec<_>>();

        unspilled.sort_by(|s1, s2| {
            let r1 = s1.domain::<A::Rows>();
            let r2 = s2.domain::<A::Rows>();
            let cmp = r1.first().cmp(&r2.first());
            cmp
        });

        let mut released = 0;
        while let Some(block) = unspilled.pop() {
            if released >= need {
                break;
            }

            block.spill(&self.spiller).await?;
            released += block.rows;
        }

        Ok(())
    }

    async fn subsequent_spill_all(&mut self) -> Result<()> {
        for s in &mut self.subsequent {
            s.spill(0).await?;
        }
        Ok(())
    }

    fn choice_streams_by_bound(&mut self) {
        debug_assert!(self.current.is_empty());
        debug_assert!(!self.subsequent.is_empty());

        self.cur_bound = self.next_bound();
        if self.cur_bound.is_none() {
            mem::swap(&mut self.current, &mut self.subsequent);
            for s in &mut self.current {
                s.bound = None
            }
            return;
        }

        (self.current, self.subsequent) = self
            .subsequent
            .drain(..)
            .map(|mut s| {
                s.bound = self.cur_bound.clone();
                s
            })
            .partition(|s| s.should_include_first());

        self.current.sort_by_key(|s| s.blocks[0].data.is_some());
    }

    pub fn subsequent_memory_rows(&self) -> usize {
        self.subsequent
            .iter()
            .map(|s| s.in_memory_rows())
            .sum::<usize>()
    }

    fn current_memory_rows(&self) -> usize {
        self.current
            .iter()
            .map(|s| s.in_memory_rows())
            .sum::<usize>()
    }

    pub fn max_rows(&self) -> usize {
        debug_assert!(self.num_merge > 0);
        self.num_merge * self.batch_rows
    }

    fn new_stream(
        &mut self,
        blocks: VecDeque<SpillableBlock>,
        bound: Option<A::Rows>,
    ) -> BoundBlockStream<A::Rows> {
        BoundBlockStream::<A::Rows> {
            blocks,
            bound,
            sort_row_offset: self.sort_row_offset,
            spiller: self.spiller.clone(),
        }
    }

    fn next_bound(&mut self) -> Option<A::Rows> {
        let Sampler::Finish(bounds) = &mut self.sampler else {
            unreachable!()
        };

        let last = bounds.last_mut()?;
        let bound = match last.len() {
            0 => unreachable!(),
            1 => bounds.pop().unwrap(),
            _ => {
                let bound = last.slice(0..1).maybe_gc();
                *last = last.slice(1..last.len());
                bound
            }
        };
        Some(A::Rows::from_column(&bound).unwrap())
    }

    fn determine_bounds(&self, sampled_rows: Vec<DataBlock>) -> Result<Vec<Column>> {
        match sampled_rows.len() {
            0 => Ok(vec![]),
            1 => Ok(vec![DataBlock::sort(
                &sampled_rows[0],
                &[SortColumnDescription {
                    offset: 0,
                    asc: A::Rows::IS_ASC_COLUMN,
                    nulls_first: false,
                }],
                None,
            )?
            .get_last_column()
            .clone()]),
            _ => {
                let streams = sampled_rows
                    .into_iter()
                    .map(|data| {
                        let data = DataBlock::sort(
                            &data,
                            &[SortColumnDescription {
                                offset: 0,
                                asc: A::Rows::IS_ASC_COLUMN,
                                nulls_first: false,
                            }],
                            None,
                        )
                        .unwrap();
                        DataBlockStream::new(data, 0)
                    })
                    .collect::<Vec<_>>();

                let schema = Arc::new(self.schema.project(&[self.schema.num_fields() - 1]));
                let mut merger = Merger::<A, _>::create(schema, streams, self.batch_rows, None);

                let mut blocks = Vec::new();
                while let Some(block) = merger.next_block()? {
                    blocks.push(block)
                }
                debug_assert!(merger.is_finished());

                Ok(blocks
                    .iter()
                    .rev()
                    .map(|b| b.get_last_column().clone())
                    .collect::<Vec<_>>())
            }
        }
    }

    #[allow(unused)]
    pub fn format_memory_usage(&self, f: &mut Formatter<'_>) -> fmt::Result {
        f.debug_struct("TransformStreamSortSpill")
            .field("num_merge", &self.num_merge)
            .field("batch_rows", &self.batch_rows)
            //.field("input_rows", &self.input_rows())
            .field("current_memory_rows", &self.current_memory_rows())
            .field("current", &self.current)
            .field("subsequent_memory_rows", &self.subsequent_memory_rows())
            .field("subsequent", &self.subsequent)
            .field("has_output_merger", &self.output_merger.is_some())
            .field("cur_bound", &self.cur_bound)
            .finish()
    }
}

struct SpillableBlock {
    data: Option<DataBlock>,
    rows: usize,
    location: Option<Location>,
    domain: Column,
    processed: usize,
}

impl SpillableBlock {
    fn new(data: DataBlock, sort_row_offset: usize) -> Self {
        Self {
            location: None,
            processed: 0,
            rows: data.num_rows(),
            domain: get_domain(sort_column(&data, sort_row_offset)),
            data: Some(data),
        }
    }

    fn slice(&mut self, pos: usize, sort_row_offset: usize) -> DataBlock {
        let data = self.data.as_ref().unwrap();

        let left = data.slice(0..pos);
        let right = data.slice(pos..data.num_rows());

        self.domain = get_domain(sort_column(&right, sort_row_offset));
        self.rows = right.num_rows();
        self.data = Some(right);
        if self.location.is_some() {
            self.processed += pos;
        }
        left
    }

    fn domain<R: Rows>(&self) -> R {
        R::from_column(&self.domain).unwrap()
    }

    async fn spill(&mut self, spiller: &Spiller) -> Result<()> {
        let data = self.data.take().unwrap();
        if self.location.is_none() {
            let location = spiller.spill(vec![data]).await?;
            self.location = Some(location);
        }
        Ok(())
    }
}

impl Debug for SpillableBlock {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        f.debug_struct("SpillableBlock")
            .field("has_data", &self.data.is_some())
            .field("rows", &self.rows)
            .field("location", &self.location)
            .field("domain", &self.domain)
            .field("processed", &self.processed)
            .finish()
    }
}

fn sort_column(data: &DataBlock, sort_row_offset: usize) -> &Column {
    data.get_by_offset(sort_row_offset)
        .value
        .as_column()
        .unwrap()
}

/// BoundBlockStream is a stream of blocks that are cutoff less or equal than bound.
struct BoundBlockStream<R: Rows> {
    blocks: VecDeque<SpillableBlock>,
    bound: Option<R>,
    sort_row_offset: usize,
    spiller: Arc<Spiller>,
}

impl<R: Rows> Debug for BoundBlockStream<R> {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        f.debug_struct("BoundBlockStream")
            .field("blocks", &self.blocks)
            .field("bound", &self.bound)
            .field("sort_row_offset", &self.sort_row_offset)
            .finish()
    }
}

#[async_trait::async_trait]
impl<R: Rows> SortedStream for BoundBlockStream<R> {
    async fn async_next(&mut self) -> Result<(Option<(DataBlock, Column)>, bool)> {
        if self.should_include_first() {
            self.restore_first().await?;
            let data = self.take_next_block();
            let col = sort_column(&data, self.sort_row_offset).clone();
            Ok((Some((data, col)), false))
        } else {
            Ok((None, false))
        }
    }
}

impl<R: Rows> BoundBlockStream<R> {
    fn should_include_first(&self) -> bool {
        let Some(block) = self.blocks.front() else {
            return false;
        };

        match &self.bound {
            Some(bound) => block.domain::<R>().first() <= bound.row(0),
            None => true,
        }
    }

    fn take_next_bounded_block(&mut self) -> DataBlock {
        let Some(bound) = &self.bound else {
            return self.take_next_block();
        };

        let block = self.blocks.front_mut().unwrap();
        if let Some(pos) =
            block_split_off_position(block.data.as_ref().unwrap(), bound, self.sort_row_offset)
        {
            block.slice(pos, self.sort_row_offset)
        } else {
            self.take_next_block()
        }
    }

    fn take_next_block(&mut self) -> DataBlock {
        let mut block = self.blocks.pop_front().unwrap();
        block.data.take().unwrap()
    }

    async fn restore_first(&mut self) -> Result<()> {
        let block = self.blocks.front_mut().unwrap();
        if block.data.is_some() {
            return Ok(());
        }

        let location = block.location.as_ref().unwrap();
        let data = self.spiller.read_spilled_file(location).await?;
        block.data = Some(if block.processed != 0 {
            debug_assert_eq!(block.rows + block.processed, data.num_rows());
            data.slice(block.processed..data.num_rows())
        } else {
            data
        });
        debug_assert_eq!(
            block.domain,
            get_domain(sort_column(
                block.data.as_ref().unwrap(),
                self.sort_row_offset
            ))
        );
        Ok(())
    }

    fn len(&self) -> usize {
        self.blocks.len()
    }

    fn is_empty(&self) -> bool {
        self.len() == 0
    }

    fn in_memory_rows(&self) -> usize {
        self.blocks
            .iter()
            .map(|b| if b.data.is_some() { b.rows } else { 0 })
            .sum()
    }

    async fn spill(&mut self, skip: usize) -> Result<()> {
        for b in &mut self
            .blocks
            .iter_mut()
            .skip(skip)
            .filter(|b| b.data.is_some())
        {
            b.spill(&self.spiller).await?;
        }
        Ok(())
    }
}

fn block_split_off_position<R: Rows>(
    data: &DataBlock,
    bound: &R,
    sort_row_offset: usize,
) -> Option<usize> {
    let rows = R::from_column(sort_column(data, sort_row_offset)).unwrap();
    debug_assert!(rows.len() > 0);
    debug_assert!(bound.len() == 1);
    let bound = bound.row(0);
    partition_point(&rows, &bound)
}

/// partition_point find the first element that is greater than bound
fn partition_point<'a, R: Rows>(list: &'a R, bound: &R::Item<'a>) -> Option<usize> {
    if *bound >= list.last() {
        return None;
    }

    let mut size = list.len();
    let mut left = 0;
    let mut right = size;
    while left < right {
        let mid = left + size / 2;
        if list.row(mid) <= *bound {
            left = mid + 1;
        } else {
            right = mid;
        }
        size = right - left;
    }
    Some(left)
}

struct DataBlockStream(Option<(DataBlock, Column)>);

impl SortedStream for DataBlockStream {
    fn next(&mut self) -> Result<(Option<(DataBlock, Column)>, bool)> {
        Ok((self.0.take(), false))
    }
}

impl DataBlockStream {
    fn new(data: DataBlock, sort_row_offset: usize) -> Self {
        let col = sort_column(&data, sort_row_offset).clone();
        Self(Some((data, col)))
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
    use databend_common_expression::types::DataType;
    use databend_common_expression::types::Int32Type;
    use databend_common_expression::types::NumberDataType;
    use databend_common_expression::types::StringType;
    use databend_common_expression::BlockEntry;
    use databend_common_expression::Column;
    use databend_common_expression::DataField;
    use databend_common_expression::DataSchemaRefExt;
    use databend_common_expression::FromData;
    use databend_common_expression::Value;
    use databend_common_pipeline_transforms::processors::sort::convert_rows;
    use databend_common_pipeline_transforms::processors::sort::SimpleRowsAsc;
    use databend_common_pipeline_transforms::sort::SimpleRowsDesc;
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

    async fn run_bound_block_stream<R: Rows>(
        spiller: Arc<Spiller>,
        sort_desc: Arc<Vec<SortColumnDescription>>,
        bound: Column,
        block_part: usize,
        want: Column,
    ) -> Result<()> {
        let (schema, block) = test_data();
        let block = DataBlock::sort(&block, &sort_desc, None)?;
        let bound = Some(R::from_column(&bound)?);
        let sort_row_offset = schema.fields().len();

        let blocks = vec![
            block.slice(0..block_part),
            block.slice(block_part..block.num_rows()),
        ]
        .into_iter()
        .map(|mut data| {
            let col = convert_rows(schema.clone(), &sort_desc, data.clone()).unwrap();
            data.add_column(BlockEntry::new(col.data_type(), Value::Column(col)));
            SpillableBlock::new(data, sort_row_offset)
        })
        .collect::<VecDeque<_>>();

        let mut stream = BoundBlockStream::<R> {
            blocks,
            bound,
            sort_row_offset,
            spiller: spiller.clone(),
        };

        let data = stream.take_next_bounded_block();
        let got = sort_column(&data, stream.sort_row_offset).clone();
        assert_eq!(want, got);

        Ok(())
    }

    #[tokio::test]
    async fn test_bound_block_stream() -> Result<()> {
        let fixture = TestFixture::setup().await?;
        let ctx = fixture.new_query_ctx().await?;

        let op = DataOperator::instance().spill_operator();
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
                Int32Type::from_data(vec![3, 5]),
            )
            .await?;

            run_bound_block_stream::<SimpleRowsAsc<Int32Type>>(
                spiller.clone(),
                sort_desc.clone(),
                Int32Type::from_data(vec![8]),
                4,
                Int32Type::from_data(vec![3, 5, 7, 7]),
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
                StringType::from_data(vec!["w", "h", "g", "f"]),
            )
            .await?;
        }

        Ok(())
    }
}
