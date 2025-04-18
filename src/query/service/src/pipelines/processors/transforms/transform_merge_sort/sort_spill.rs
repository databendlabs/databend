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
use std::intrinsics::unlikely;
use std::mem;
use std::sync::atomic;
use std::sync::atomic::AtomicBool;
use std::sync::Arc;

use databend_common_column::bitmap::MutableBitmap;
use databend_common_exception::ErrorCode;
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

use super::Base;
use super::MemoryRows;
use crate::spillers::Location;
use crate::spillers::Spiller;

pub struct SortSpill<A: SortAlgorithm> {
    base: Base,
    step: Step<A>,
}

enum Step<A: SortAlgorithm> {
    Collect(StepCollect<A>),
    Sort(StepSort<A>),
}

struct StepCollect<A: SortAlgorithm> {
    params: SortSpillParams,
    sampler: FixedRateSampler<StdRng>,
    streams: Vec<BoundBlockStream<A::Rows>>,
}

struct StepSort<A: SortAlgorithm> {
    params: SortSpillParams,
    /// Partition boundaries for restoring and sorting blocks, stored in reverse order of Column.
    /// Each boundary represents a cutoff point where data less than or equal to it belongs to one partition.
    bounds: Vec<Column>,
    cur_bound: Option<A::Rows>,

    subsequent: Vec<BoundBlockStream<A::Rows>>,
    current: Vec<BoundBlockStream<A::Rows>>,

    output_merger: Option<Merger<A, BoundBlockStream<A::Rows>>>,
}

impl<A> SortSpill<A>
where A: SortAlgorithm
{
    pub fn new(base: Base, params: SortSpillParams) -> Self {
        let step = Step::Collect(StepCollect {
            sampler: FixedRateSampler::new(
                vec![base.sort_row_offset],
                params.batch_rows,
                params.batch_rows * params.num_merge,
                params.batch_rows,
                StdRng::seed_from_u64(rand::random()),
            )
            .unwrap(),
            params,
            streams: vec![],
        });

        Self { base, step }
    }

    pub fn sort_input_data(
        &mut self,
        input_data: Vec<DataBlock>,
        aborting: &AtomicBool,
    ) -> Result<()> {
        let Step::Collect(collect) = &mut self.step else {
            unreachable!()
        };
        collect.sort_input_data(&self.base, input_data, aborting)
    }

    pub async fn subsequent_spill_last(&mut self, target_rows: usize) -> Result<()> {
        let Step::Collect(collect) = &mut self.step else {
            unreachable!()
        };
        collect.spill_last(&self.base, target_rows).await
    }

    pub fn collect_memory_rows(&self) -> usize {
        match &self.step {
            Step::Collect(step_collect) => step_collect.streams.in_memory_rows(),
            _ => unreachable!(),
        }
    }

    pub async fn on_restore(&mut self) -> Result<(Option<DataBlock>, bool)> {
        match &mut self.step {
            Step::Collect(collect) => self.step = Step::Sort(collect.next_step(&self.base)?),
            Step::Sort(_) => (),
        };

        let Step::Sort(sort) = &mut self.step else {
            unreachable!()
        };

        if sort.output_merger.is_some() {
            return sort.restore_and_output(&self.base).await;
        }

        while sort.current.is_empty() {
            sort.choice_streams_by_bound();
        }

        if sort.current.len() > sort.params.num_merge {
            sort.merge_current(&self.base).await?;
            Ok((None, false))
        } else {
            sort.restore_and_output(&self.base).await
        }
    }

    pub fn max_rows(&self) -> usize {
        let params = match &self.step {
            Step::Collect(collect) => collect.params,
            Step::Sort(sort) => sort.params,
        };
        params.num_merge * params.batch_rows
    }

    #[allow(unused)]
    pub fn format_memory_usage(&self) -> FmtMemoryUsage<'_, A> {
        FmtMemoryUsage(self)
    }
}

impl<A: SortAlgorithm> StepCollect<A> {
    fn sort_input_data(
        &mut self,
        base: &Base,
        mut input_data: Vec<DataBlock>,
        aborting: &AtomicBool,
    ) -> Result<()> {
        let batch_rows = self.params.batch_rows;

        for data in &input_data {
            self.sampler.add_block(data.clone());
        }
        self.sampler.compact_blocks(false);

        let sorted = if input_data.len() == 1 {
            let data = input_data.pop().unwrap();
            vec![base.new_block(data)].into()
        } else {
            let mut merger = create_memory_merger::<A>(
                input_data,
                base.schema.clone(),
                base.sort_row_offset,
                base.limit,
                batch_rows,
            );

            let mut sorted = VecDeque::new();
            while let Some(data) = merger.next_block()? {
                if unlikely(aborting.load(atomic::Ordering::Relaxed)) {
                    return Err(ErrorCode::AbortedQuery(
                        "Aborted query, because the server is shutting down or the query was killed.",
                    ));
                }

                sorted.push_back(base.new_block(data));
            }
            debug_assert!(merger.is_finished());
            sorted
        };

        let stream = base.new_stream(sorted, None);
        self.streams.push(stream);
        Ok(())
    }

    async fn spill_last(&mut self, base: &Base, target_rows: usize) -> Result<()> {
        let Some(s) = self.streams.last_mut() else {
            return Ok(());
        };

        let mut released = 0;
        for b in s.blocks.iter_mut().rev() {
            if b.data.is_some() {
                b.spill(&base.spiller).await?;
                released += b.rows;
            }
            if released >= target_rows {
                break;
            }
        }

        Ok(())
    }

    fn next_step(&mut self, base: &Base) -> Result<StepSort<A>> {
        self.sampler.compact_blocks(true);
        let sampled_rows = std::mem::take(&mut self.sampler.dense_blocks);
        let bounds = base.determine_bounds::<A>(sampled_rows, self.params.batch_rows)?;

        Ok(StepSort {
            bounds,
            cur_bound: None,
            subsequent: std::mem::take(&mut self.streams),
            current: vec![],
            output_merger: None,
            params: self.params,
        })
    }
}

impl<A: SortAlgorithm> StepSort<A> {
    fn next_bound(&mut self) {
        let Some(last) = self.bounds.last_mut() else {
            self.cur_bound = None;
            return;
        };
        let bound = match last.len() {
            0 => unreachable!(),
            1 => self.bounds.pop().unwrap(),
            _ => {
                let bound = last.slice(0..1).maybe_gc();
                *last = last.slice(1..last.len());
                bound
            }
        };
        self.cur_bound = Some(A::Rows::from_column(&bound).unwrap());
    }

    async fn merge_current(&mut self, base: &Base) -> Result<()> {
        for s in &mut self.subsequent {
            s.spill(0).await?;
        }
        let SortSpillParams {
            batch_rows,
            num_merge,
        } = self.params;
        for (i, s) in self.current.iter_mut().rev().enumerate() {
            if i < num_merge {
                s.spill(1).await?;
            } else {
                s.spill(0).await?;
            }
        }

        let streams = self
            .current
            .drain(self.current.len() - num_merge..)
            .collect();

        let mut merger = Merger::<A, _>::create(base.schema.clone(), streams, batch_rows, None);

        let mut sorted = VecDeque::new();
        while let Some(data) = merger.async_next_block().await? {
            let mut block = base.new_block(data);
            block.spill(&base.spiller).await?;
            sorted.push_back(block);
        }
        debug_assert!(merger.is_finished());

        let stream = base.new_stream(sorted, self.cur_bound.clone());
        self.current.insert(0, stream);
        self.subsequent
            .extend(merger.streams().into_iter().filter(|s| !s.is_empty()));
        Ok(())
    }

    async fn restore_and_output(&mut self, base: &Base) -> Result<(Option<DataBlock>, bool)> {
        let merger = match self.output_merger.as_mut() {
            Some(merger) => merger,
            None => {
                debug_assert!(!self.current.is_empty());
                if self.current.len() == 1 {
                    let mut s = self.current.pop().unwrap();
                    s.restore_first().await?;
                    let block = Some(s.take_next_bounded_block());

                    if !s.is_empty() {
                        if s.should_include_first() {
                            self.current.push(s);
                        } else {
                            self.subsequent.push(s);
                        }
                        return Ok((block, false));
                    }

                    return Ok((block, self.subsequent.is_empty()));
                }

                self.sort_spill(base, self.params).await?;

                let streams = mem::take(&mut self.current);
                let merger = Merger::<A, _>::create(
                    base.schema.clone(),
                    streams,
                    self.params.batch_rows,
                    None,
                );
                self.output_merger.insert(merger)
            }
        };

        let Some(data) = merger.async_next_block().await? else {
            debug_assert!(merger.is_finished());
            let streams = self.output_merger.take().unwrap().streams();
            self.subsequent
                .extend(streams.into_iter().filter(|s| !s.is_empty()));
            return Ok((None, self.subsequent.is_empty()));
        };

        let mut sorted = base.new_stream([base.new_block(data)].into(), self.cur_bound.clone());
        let block = if sorted.should_include_first() {
            let block = Some(sorted.take_next_bounded_block());
            if sorted.is_empty() {
                return Ok((block, false));
            }
            block
        } else {
            None
        };

        while let Some(data) = merger.async_next_block().await? {
            let mut block = base.new_block(data);
            block.spill(&base.spiller).await?;
            sorted.blocks.push_back(block);
        }
        debug_assert!(merger.is_finished());

        if !sorted.is_empty() {
            self.subsequent.push(sorted);
        }
        let streams = self.output_merger.take().unwrap().streams();
        self.subsequent
            .extend(streams.into_iter().filter(|s| !s.is_empty()));
        Ok((block, self.subsequent.is_empty()))
    }

    async fn sort_spill(
        &mut self,
        base: &Base,
        SortSpillParams {
            batch_rows,
            num_merge,
        }: SortSpillParams,
    ) -> Result<()> {
        let need = self
            .current
            .iter()
            .map(|s| if s.blocks[0].data.is_none() { 1 } else { 0 })
            .sum::<usize>()
            * batch_rows;

        if need + self.subsequent.in_memory_rows() + self.current.in_memory_rows()
            < batch_rows * num_merge
        {
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

            block.spill(&base.spiller).await?;
            released += block.rows;
        }

        Ok(())
    }

    fn choice_streams_by_bound(&mut self) {
        debug_assert!(self.current.is_empty());
        debug_assert!(!self.subsequent.is_empty());

        self.next_bound();
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
}

impl Base {
    fn new_stream<R: Rows>(
        &self,
        blocks: VecDeque<SpillableBlock>,
        bound: Option<R>,
    ) -> BoundBlockStream<R> {
        BoundBlockStream::<R> {
            blocks,
            bound,
            sort_row_offset: self.sort_row_offset,
            spiller: self.spiller.clone(),
        }
    }

    fn new_block(&self, data: DataBlock) -> SpillableBlock {
        SpillableBlock::new(data, self.sort_row_offset)
    }

    fn determine_bounds<A: SortAlgorithm>(
        &self,
        sampled_rows: Vec<DataBlock>,
        batch_rows: usize,
    ) -> Result<Vec<Column>> {
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

                let schema = self.schema.project(&[self.sort_row_offset]);
                let mut merger = Merger::<A, _>::create(schema.into(), streams, batch_rows, None);

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
}

impl<R: Rows> MemoryRows for Vec<BoundBlockStream<R>> {
    fn in_memory_rows(&self) -> usize {
        self.iter().map(|s| s.in_memory_rows()).sum::<usize>()
    }
}

pub struct FmtMemoryUsage<'a, A: SortAlgorithm>(&'a SortSpill<A>);

impl<A: SortAlgorithm> fmt::Debug for FmtMemoryUsage<'_, A> {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        let debug = &mut f.debug_struct("SortSpill");
        match &self.0.step {
            Step::Collect(step_collect) => debug
                .field("num_merge", &step_collect.params.num_merge)
                .field("batch_rows", &step_collect.params.batch_rows)
                .field("subsequent", &step_collect.streams)
                .field(
                    "subsequent_memory_rows",
                    &step_collect.streams.in_memory_rows(),
                ),
            Step::Sort(step_sort) => debug
                .field("num_merge", &step_sort.params.num_merge)
                .field("batch_rows", &step_sort.params.batch_rows)
                .field("cur_bound", &step_sort.cur_bound)
                .field(
                    "subsequent_memory_rows",
                    &step_sort.subsequent.in_memory_rows(),
                )
                .field("current_memory_rows", &step_sort.current.in_memory_rows())
                .field("current", &step_sort.current)
                .field("subsequent", &step_sort.subsequent)
                .field("has_output_merger", &step_sort.output_merger.is_some()),
        }
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

pub struct DataBlockStream(Option<(DataBlock, Column)>);

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

pub type MemoryMerger<A> = Merger<A, DataBlockStream>;

pub fn create_memory_merger<A: SortAlgorithm>(
    blocks: Vec<DataBlock>,
    schema: DataSchemaRef,
    sort_row_offset: usize,
    limit: Option<usize>,
    batch_rows: usize,
) -> MemoryMerger<A> {
    let streams = blocks
        .into_iter()
        .map(|data| DataBlockStream::new(data, sort_row_offset))
        .collect();
    Merger::<A, _>::create(schema, streams, batch_rows, limit)
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
