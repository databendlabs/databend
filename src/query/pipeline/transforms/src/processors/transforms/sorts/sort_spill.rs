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
use std::hint::unlikely;
use std::marker::PhantomData;
use std::mem;
use std::sync::atomic;
use std::sync::atomic::AtomicBool;

use databend_common_column::bitmap::MutableBitmap;
use databend_common_exception::ErrorCode;
use databend_common_exception::Result;
use databend_common_expression::Column;
use databend_common_expression::DataBlock;
use databend_common_expression::DataSchemaRef;
use databend_common_expression::Scalar;
use databend_common_expression::sampler::FixedRateSampler;
use rand::SeedableRng;
use rand::rngs::StdRng;

use super::Base;
use super::RowsStat;
use super::SortCollectedMeta;
use super::SortSpillParams;
use super::core::Bounds;
use super::core::Merger;
use super::core::Rows;
use super::core::SortedStream;
use super::core::algorithm::SortAlgorithm;
use crate::MemorySettings;
use crate::traits::DataBlockSpill;
use crate::traits::Location;

pub struct SortSpill<A: SortAlgorithm, S: DataBlockSpill> {
    base: Base<S>,
    step: Step<A, S>,
}

enum Step<A: SortAlgorithm, S: DataBlockSpill> {
    Collect(StepCollect<A, S>),
    Sort(StepSort<A, S>),
}

struct StepCollect<A: SortAlgorithm, S> {
    params: SortSpillParams,
    sampler: FixedRateSampler<StdRng>,
    streams: Vec<BoundBlockStream<A::Rows, S>>,
}

struct StepSort<A: SortAlgorithm, S: DataBlockSpill> {
    params: SortSpillParams,
    /// Partition boundaries for restoring and sorting blocks.
    /// Each boundary represents a cutoff point where data less than or equal to it belongs to one partition.
    bounds: Bounds,
    cur_bound: Option<Scalar>,
    bound_index: i32,

    subsequent: Vec<BoundBlockStream<A::Rows, S>>,
    current: Vec<BoundBlockStream<A::Rows, S>>,

    output_merger: Option<Merger<A, BoundBlockStream<A::Rows, S>>>,
}

impl<A, S> SortSpill<A, S>
where
    A: SortAlgorithm,
    S: DataBlockSpill,
{
    pub fn new(base: Base<S>, params: SortSpillParams) -> Self {
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

    pub(super) fn from_meta(base: Base<S>, meta: SortCollectedMeta) -> Self {
        let SortCollectedMeta {
            params,
            bounds,
            sequences,
        } = meta;

        let subsequent = sequences
            .into_iter()
            .filter_map(|seq| {
                (!seq.is_empty()).then(|| base.new_stream(Vec::from(seq).into(), None))
            })
            .collect::<Vec<_>>();
        debug_assert!(!subsequent.is_empty());
        Self {
            base,
            step: Step::Sort(StepSort {
                params,
                bounds,
                cur_bound: None,
                bound_index: -1,
                subsequent,
                current: vec![],
                output_merger: None,
            }),
        }
    }

    pub async fn sort_input_data(
        &mut self,
        input_data: Vec<DataBlock>,
        need_spill: bool,
        aborting: &AtomicBool,
    ) -> Result<()> {
        let Step::Collect(collect) = &mut self.step else {
            unreachable!()
        };
        collect
            .sort_input_data(&self.base, input_data, need_spill, aborting)
            .await
    }

    pub fn collect_total_rows(&self) -> usize {
        match &self.step {
            Step::Collect(step_collect) => step_collect.streams.total_rows(),
            _ => unreachable!(),
        }
    }

    pub async fn on_restore(&mut self, _memory_settings: &MemorySettings) -> Result<OutputData> {
        match &mut self.step {
            Step::Collect(collect) => self.step = Step::Sort(collect.next_step(&self.base)?),
            Step::Sort(_) => (),
        };

        let Step::Sort(sort) = &mut self.step else {
            unreachable!()
        };

        if sort.output_merger.is_some() {
            return sort
                .restore_and_output(&self.base, sort.params.num_merge)
                .await;
        }

        while sort.current.is_empty() {
            sort.choice_streams_by_bound();
        }

        let num_merge = sort.recalculate_num_merge();
        assert!(num_merge >= 2);
        log::debug!(
            current_len = sort.current.len(),
            subsequent_len = sort.subsequent.len(),
            num_merge,
            batch_rows = sort.params.batch_rows;
        "restore params");
        if sort.current.len() <= num_merge {
            sort.restore_and_output(&self.base, num_merge).await
        } else {
            sort.merge_current(&self.base, num_merge).await?;
            Ok(OutputData {
                block: None,
                bound: (u32::MAX, None),
                finish: false,
            })
        }
    }

    pub fn params(&self) -> SortSpillParams {
        match &self.step {
            Step::Collect(collect) => collect.params,
            Step::Sort(sort) => sort.params,
        }
    }

    pub fn format_memory_usage(&self) -> FmtMemoryUsage<'_, A, S> {
        FmtMemoryUsage(self)
    }

    pub(super) fn dump_collect(self) -> Result<SortCollectedMeta> {
        let Self {
            base,
            step: Step::Collect(mut collect),
        } = self
        else {
            unreachable!()
        };

        let StepSort {
            params,
            bounds,
            subsequent,
            ..
        } = collect.next_step(&base)?;

        let sequences = subsequent
            .into_iter()
            .map(|stream| {
                assert!(stream.bound.is_none());
                Vec::from(stream.blocks).into_boxed_slice()
            })
            .collect();

        Ok(SortCollectedMeta {
            params,
            sequences,
            bounds,
        })
    }
}

impl<A: SortAlgorithm, S: DataBlockSpill> StepCollect<A, S> {
    #[fastrace::trace(name = "StepCollect::sort_input_data")]
    async fn sort_input_data(
        &mut self,
        base: &Base<S>,
        mut input_data: Vec<DataBlock>,
        need_spill: bool,
        aborting: &AtomicBool,
    ) -> Result<()> {
        let batch_rows = self.params.batch_rows;

        for data in &input_data {
            self.sampler.add_block(data.clone());
        }
        self.sampler.compact_blocks(false);

        let sorted = if input_data.len() == 1 {
            let data = input_data.pop().unwrap();
            let mut block = base.new_block(data);
            if need_spill {
                block.spill(&base.spiller).await?;
            }
            vec![block].into()
        } else {
            // todo: using multi-threaded cascade two-way merge sorting algorithm to obtain the best performance
            // also see https://arxiv.org/pdf/1406.2628
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
                    return Err(ErrorCode::aborting());
                }

                let mut block = base.new_block(data);
                if need_spill && !sorted.is_empty() {
                    block.spill(&base.spiller).await?;
                }
                sorted.push_back(block);
            }
            debug_assert!(merger.is_finished());
            sorted
        };

        let stream = base.new_stream(sorted, None);
        self.streams.push(stream);
        Ok(())
    }

    #[allow(dead_code)]
    #[fastrace::trace(name = "StepCollect::spill_last")]
    async fn spill_last(&mut self, base: &Base<S>, target_rows: usize) -> Result<()> {
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

    fn next_step(&mut self, base: &Base<S>) -> Result<StepSort<A, S>> {
        self.sampler.compact_blocks(true);
        let sampled_rows = std::mem::take(&mut self.sampler.dense_blocks);
        let bounds = base.determine_bounds::<A>(sampled_rows, self.params.batch_rows)?;

        Ok(StepSort {
            bounds,
            cur_bound: None,
            bound_index: -1,
            subsequent: std::mem::take(&mut self.streams),
            current: vec![],
            output_merger: None,
            params: self.params,
        })
    }
}

pub struct OutputData {
    pub block: Option<DataBlock>,
    pub bound: (u32, Option<Scalar>),
    pub finish: bool,
}

impl<A: SortAlgorithm, S: DataBlockSpill> StepSort<A, S> {
    fn next_bound(&mut self) {
        match self.bounds.next_bound() {
            Some(bound) => self.cur_bound = Some(bound),
            None => self.cur_bound = None,
        }
        self.bound_index += 1;
    }

    #[fastrace::trace(name = "StepSort::merge_current")]
    async fn merge_current(&mut self, base: &Base<S>, num_merge: usize) -> Result<()> {
        for s in &mut self.subsequent {
            s.spill(0).await?;
        }
        let batch_rows = self.params.batch_rows;
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

    #[fastrace::trace(name = "StepSort::restore_and_output")]
    async fn restore_and_output(&mut self, base: &Base<S>, num_merge: usize) -> Result<OutputData> {
        let merger = match self.output_merger.as_mut() {
            Some(merger) => merger,
            None => {
                debug_assert!(!self.current.is_empty());
                if self.current.len() == 1 {
                    let mut s = self.current.pop().unwrap();
                    s.restore_first().await?;
                    let block = Some(s.take_next_bounded_block());
                    assert!(self.bound_index >= 0);
                    let bound = (self.bound_index as _, s.bound.clone());

                    if !s.is_empty() {
                        match s.should_include_first() {
                            true => self.current.push(s),
                            false => self.subsequent.push(s),
                        }
                        return Ok(OutputData {
                            block,
                            bound,
                            finish: false,
                        });
                    }

                    return Ok(OutputData {
                        block,
                        bound,
                        finish: self.subsequent.is_empty(),
                    });
                }

                self.sort_spill(base, self.params.batch_rows, num_merge)
                    .await?;

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

            return Ok(OutputData {
                block: None,
                bound: (u32::MAX, None),
                finish: self.subsequent.is_empty(),
            });
        };

        let mut sorted = base.new_stream([base.new_block(data)].into(), self.cur_bound.clone());
        let (block, bound) = if sorted.should_include_first() {
            let block = Some(sorted.take_next_bounded_block());
            debug_assert!(self.bound_index >= 0);
            let bound = (self.bound_index as _, sorted.bound.clone());
            if sorted.is_empty() {
                return Ok(OutputData {
                    block,
                    bound,
                    finish: false,
                });
            }
            (block, bound)
        } else {
            (None, (u32::MAX, None))
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

        Ok(OutputData {
            block,
            bound,
            finish: self.subsequent.is_empty(),
        })
    }

    #[fastrace::trace(name = "StepSort::sort_spill")]
    async fn sort_spill(
        &mut self,
        base: &Base<S>,
        batch_rows: usize,
        num_merge: usize,
    ) -> Result<()> {
        let need = self
            .current
            .iter()
            .map(|s| if s.blocks[0].data.is_none() { 1 } else { 0 })
            .sum::<usize>()
            * batch_rows;

        let current_memory_rows = self.current.in_memory_rows();
        let subsequent_memory_rows = self.subsequent.in_memory_rows();
        if need + subsequent_memory_rows + current_memory_rows < batch_rows * num_merge {
            return Ok(());
        }
        log::debug!(need, current_memory_rows, subsequent_memory_rows; "subsequent need spill");

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
            r1.first().cmp(&r2.first())
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

    #[fastrace::trace(name = "StepSort::choice_streams_by_bound")]
    fn choice_streams_by_bound(&mut self) {
        debug_assert!(self.current.is_empty());
        debug_assert!(!self.subsequent.is_empty());

        self.next_bound();
        log::debug!(cur_bound:? = self.cur_bound, bound_index = self.bound_index; "next_bound");
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

    fn recalculate_num_merge(&self) -> usize {
        if self.current.len() <= 2 {
            return self.params.num_merge;
        }
        let mut max_rows = self.params.max_rows();
        let batch_rows = self.params.batch_rows;
        let mut num_merge = 0;
        for s in self.current.iter().rev() {
            // Edge case: rows may not always equal batch_rows, recalculate num_merge to mitigate risk
            let rows = s.blocks[0].rows.max(batch_rows);
            if max_rows >= rows || num_merge < 2 {
                num_merge += 1;
            }
            max_rows = max_rows.saturating_sub(rows);
        }
        num_merge
    }
}

impl<S: DataBlockSpill> Base<S> {
    fn new_stream<R: Rows>(
        &self,
        blocks: VecDeque<SpillableBlock>,
        bound: Option<Scalar>,
    ) -> BoundBlockStream<R, S> {
        assert!(!blocks.is_empty());
        BoundBlockStream {
            blocks,
            bound,
            sort_row_offset: self.sort_row_offset,
            spiller: self.spiller.clone(),
            _r: Default::default(),
        }
    }

    fn new_block(&self, data: DataBlock) -> SpillableBlock {
        SpillableBlock::new(data, self.sort_row_offset)
    }

    fn determine_bounds<A: SortAlgorithm>(
        &self,
        sampled_rows: Vec<DataBlock>,
        batch_rows: usize,
    ) -> Result<Bounds> {
        match sampled_rows.len() {
            0 => Ok(Bounds::default()),
            1 => Bounds::from_column::<A::Rows>(sampled_rows[0].get_last_column().clone()),
            _ => {
                let ls = sampled_rows
                    .into_iter()
                    .map(|data| {
                        let col = data.get_last_column().clone();
                        Bounds::from_column::<A::Rows>(col)
                    })
                    .collect::<Result<Vec<_>>>()?;
                Bounds::merge::<A::Rows>(ls, batch_rows)
            }
        }
    }

    pub async fn scatter_stream<R: Rows>(
        &self,
        mut blocks: VecDeque<SpillableBlock>,
        mut bounds: Bounds,
    ) -> Result<Vec<Vec<SpillableBlock>>> {
        let mut scattered = Vec::with_capacity(bounds.len() + 1);
        while !blocks.is_empty() {
            let bound = bounds.next_bound();
            let mut stream = self.new_stream::<R>(blocks, bound);

            let mut part = Vec::new();
            while let Some(block) = stream.take_next_bounded_spillable().await? {
                part.push(block);
            }

            scattered.push(part);
            blocks = stream.blocks;
        }
        Ok(scattered)
    }
}

impl<R: Rows, S> RowsStat for Vec<BoundBlockStream<R, S>> {
    fn total_rows(&self) -> usize {
        self.iter().map(|s| s.total_rows()).sum::<usize>()
    }

    fn in_memory_rows(&self) -> usize {
        self.iter().map(|s| s.in_memory_rows()).sum::<usize>()
    }
}

pub struct FmtMemoryUsage<'a, A: SortAlgorithm, S: DataBlockSpill>(&'a SortSpill<A, S>);

impl<A: SortAlgorithm, S: DataBlockSpill> fmt::Debug for FmtMemoryUsage<'_, A, S> {
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

pub struct SpillableBlock {
    data: Option<DataBlock>,
    rows: usize,
    location: Option<Location>,
    pub(super) domain: Column,
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

    async fn spill(&mut self, spiller: &impl DataBlockSpill) -> Result<()> {
        let data = self.data.take().unwrap();
        if self.location.is_none() {
            let location = spiller.spill(data).await?;
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
    data.get_by_offset(sort_row_offset).as_column().unwrap()
}

/// BoundBlockStream is a stream of blocks that are cutoff less or equal than bound.
struct BoundBlockStream<R: Rows, S> {
    blocks: VecDeque<SpillableBlock>,
    bound: Option<Scalar>,
    sort_row_offset: usize,
    spiller: S,
    _r: PhantomData<R>,
}

impl<R: Rows, S> Debug for BoundBlockStream<R, S> {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        f.debug_struct("BoundBlockStream")
            .field("blocks", &self.blocks)
            .field("bound", &self.bound)
            .field("sort_row_offset", &self.sort_row_offset)
            .finish()
    }
}

#[async_trait::async_trait]
impl<R: Rows, S: DataBlockSpill> SortedStream for BoundBlockStream<R, S> {
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

impl<R: Rows, S> BoundBlockStream<R, S> {
    fn should_include_first(&self) -> bool {
        let Some(block) = self.blocks.front() else {
            return false;
        };

        match &self.bound {
            None => true,
            Some(bound) => block.domain::<R>().first() <= R::scalar_as_item(bound),
        }
    }

    fn take_next_bounded_block(&mut self) -> DataBlock {
        let Some(bound) = &self.bound else {
            return self.take_next_block();
        };

        let block = self.blocks.front_mut().unwrap();
        if let Some(pos) =
            block_split_off_position::<R>(block.data.as_ref().unwrap(), bound, self.sort_row_offset)
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

    fn len(&self) -> usize {
        self.blocks.len()
    }

    fn is_empty(&self) -> bool {
        self.len() == 0
    }

    fn total_rows(&self) -> usize {
        self.blocks.iter().map(|b| b.rows).sum()
    }

    fn in_memory_rows(&self) -> usize {
        self.blocks
            .iter()
            .map(|b| if b.data.is_some() { b.rows } else { 0 })
            .sum()
    }
}

impl<R: Rows, S: DataBlockSpill> BoundBlockStream<R, S> {
    async fn restore_first(&mut self) -> Result<()> {
        let block = self.blocks.front_mut().unwrap();
        if block.data.is_some() {
            return Ok(());
        }

        let location = block.location.as_ref().unwrap();
        let data = self.spiller.restore(location).await?;
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

    async fn take_next_bounded_spillable(&mut self) -> Result<Option<SpillableBlock>> {
        let Some(bound) = &self.bound else {
            return Ok(self.blocks.pop_front());
        };
        let Some(block) = self.blocks.front() else {
            return Ok(None);
        };
        {
            let domain = block.domain::<R>();
            let bound_item = R::scalar_as_item(bound);
            if domain.first() > bound_item {
                return Ok(None);
            }
            if domain.last() <= bound_item {
                return Ok(self.blocks.pop_front());
            }
        }
        self.restore_first().await?;

        let block = self.blocks.front_mut().unwrap();
        if let Some(pos) = block_split_off_position::<R>(
            block.data.as_ref().unwrap(),
            self.bound.as_ref().unwrap(),
            self.sort_row_offset,
        ) {
            let data = block.slice(pos, self.sort_row_offset);
            Ok(Some(SpillableBlock::new(data, self.sort_row_offset)))
        } else {
            Ok(self.blocks.pop_front())
        }
    }
}

pub fn block_split_off_position<R: Rows>(
    data: &DataBlock,
    bound: &Scalar,
    sort_row_offset: usize,
) -> Option<usize> {
    let rows = R::from_column(sort_column(data, sort_row_offset)).unwrap();
    debug_assert!(rows.len() > 0);
    let bound = R::scalar_as_item(bound);
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
    pub(super) fn new(data: DataBlock, sort_row_offset: usize) -> Self {
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
    use std::collections::HashMap;
    use std::ops::Range;
    use std::sync::Arc;
    use std::sync::Mutex;

    use databend_base::uniq_id::GlobalUniq;
    use databend_common_expression::Column;
    use databend_common_expression::DataField;
    use databend_common_expression::DataSchemaRefExt;
    use databend_common_expression::FromData;
    use databend_common_expression::SortColumnDescription;
    use databend_common_expression::types::DataType;
    use databend_common_expression::types::Int32Type;
    use databend_common_expression::types::NumberDataType;
    use databend_common_expression::types::NumberScalar;
    use databend_common_expression::types::StringType;

    use super::*;
    use crate::sorts::core::SimpleRowsAsc;
    use crate::sorts::core::SimpleRowsDesc;
    use crate::sorts::core::convert_rows;

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
        spiller: impl DataBlockSpill,
        sort_desc: &[SortColumnDescription],
        bound: Scalar,
        block_part: usize,
        want: Column,
    ) -> Result<()> {
        let (schema, block) = test_data();
        let block = DataBlock::sort(&block, sort_desc, None)?;
        let bound = Some(bound);
        let sort_row_offset = schema.fields().len();

        let blocks = vec![
            block.slice(0..block_part),
            block.slice(block_part..block.num_rows()),
        ]
        .into_iter()
        .map(|mut data| {
            let col = convert_rows(schema.clone(), sort_desc, data.clone(), true).unwrap();
            data.add_column(col);

            SpillableBlock::new(data, sort_row_offset)
        })
        .collect::<VecDeque<_>>();

        let mut stream = BoundBlockStream::<R, _> {
            blocks,
            bound,
            sort_row_offset,
            spiller: spiller.clone(),
            _r: Default::default(),
        };

        let data = stream.take_next_bounded_block();
        let got = sort_column(&data, stream.sort_row_offset).clone();
        assert_eq!(want, got);

        Ok(())
    }

    #[tokio::test]
    async fn test_bound_block_stream() -> Result<()> {
        let spiller = MockSpiller {
            map: Arc::new(Mutex::new(HashMap::new())),
        };

        {
            let sort_desc = [SortColumnDescription {
                offset: 0,
                asc: true,
                nulls_first: false,
            }];

            run_bound_block_stream::<SimpleRowsAsc<Int32Type>>(
                spiller.clone(),
                &sort_desc,
                Scalar::Number(NumberScalar::Int32(5)),
                4,
                Int32Type::from_data(vec![3, 5]),
            )
            .await?;

            run_bound_block_stream::<SimpleRowsAsc<Int32Type>>(
                spiller.clone(),
                &sort_desc,
                Scalar::Number(NumberScalar::Int32(8)),
                4,
                Int32Type::from_data(vec![3, 5, 7, 7]),
            )
            .await?;
        }

        {
            let sort_desc = [SortColumnDescription {
                offset: 1,
                asc: false,
                nulls_first: false,
            }];

            run_bound_block_stream::<SimpleRowsDesc<StringType>>(
                spiller.clone(),
                &sort_desc,
                Scalar::String("f".to_string()),
                4,
                StringType::from_data(vec!["w", "h", "g", "f"]),
            )
            .await?;
        }

        Ok(())
    }

    fn create_spillable_block(
        block: &DataBlock,
        range: Range<usize>,
        schema: &DataSchemaRef,
        sort_desc: &[SortColumnDescription],
        sort_row_offset: usize,
    ) -> SpillableBlock {
        let mut sliced_block = block.slice(range);
        let col = convert_rows(schema.clone(), sort_desc, sliced_block.clone(), true).unwrap();
        sliced_block.add_column(col);
        SpillableBlock::new(sliced_block, sort_row_offset)
    }

    async fn prepare_test_blocks(
        spiller: &impl DataBlockSpill,
        sort_desc: &[SortColumnDescription],
        with_spilled: bool,
        with_sliced: bool,
    ) -> Result<(DataSchemaRef, VecDeque<SpillableBlock>, usize)> {
        let (schema, block) = test_data();
        let block = DataBlock::sort(&block, sort_desc, None)?;
        let sort_row_offset = schema.fields().len();

        // Create multiple blocks with different splits
        let mut blocks = VecDeque::new();

        // First block: 0..2
        blocks.push_back(create_spillable_block(
            &block,
            0..2,
            &schema,
            sort_desc,
            sort_row_offset,
        ));

        // Second block: 2..5
        blocks.push_back(create_spillable_block(
            &block,
            2..5,
            &schema,
            sort_desc,
            sort_row_offset,
        ));

        // Spill some blocks if requested
        if with_spilled {
            // Spill the second block
            blocks[1].spill(spiller).await?;
        }

        if !with_sliced {
            // Third block: 5..8
            blocks.push_back(create_spillable_block(
                &block,
                5..8,
                &schema,
                sort_desc,
                sort_row_offset,
            ));
        } else {
            // Create a block for values 8..11 (the last part of the sorted data)
            let mut spillable_block =
                create_spillable_block(&block, 5..8, &schema, sort_desc, sort_row_offset);

            spillable_block.spill(spiller).await?;
            spillable_block.data = Some(
                spiller
                    .restore(spillable_block.location.as_ref().unwrap())
                    .await?,
            );

            let sliced_data = spillable_block.slice(1, sort_row_offset);
            let sliced_block = SpillableBlock::new(sliced_data, sort_row_offset);

            // Add both blocks to maintain the order
            blocks.push_back(sliced_block);
            blocks.push_back(spillable_block);
        }

        Ok((schema, blocks, sort_row_offset))
    }

    async fn collect_and_verify_blocks<R: Rows>(
        stream: &mut BoundBlockStream<R, impl DataBlockSpill>,
        spiller: &impl DataBlockSpill,
        expected_blocks: &[Column],
    ) -> Result<()> {
        let mut result_blocks = Vec::new();
        while let Some(mut block) = stream.take_next_bounded_spillable().await? {
            // If the block data is None (spilled), restore it first
            if block.data.is_none() {
                block.data = Some(spiller.restore(block.location.as_ref().unwrap()).await?);
            }

            let data = block.data.unwrap();
            let col = sort_column(&data, stream.sort_row_offset).clone();
            result_blocks.push(col);
        }

        assert_eq!(
            expected_blocks.len(),
            result_blocks.len(),
            "Number of blocks doesn't match"
        );
        for (expected, actual) in expected_blocks.iter().zip(result_blocks.iter()) {
            assert_eq!(expected, actual, "Block content doesn't match");
        }

        Ok(())
    }

    async fn run_take_next_bounded_spillable<R: Rows>(
        spiller: impl DataBlockSpill,
        sort_desc: &[SortColumnDescription],
        bound: Option<Scalar>,
        expected_blocks: Vec<Column>,
        with_spilled: bool,
        with_sliced: bool,
    ) -> Result<()> {
        let (_, blocks, sort_row_offset) =
            prepare_test_blocks(&spiller, sort_desc, with_spilled, with_sliced).await?;

        let mut stream = BoundBlockStream::<R, _> {
            blocks,
            bound,
            sort_row_offset,
            spiller: spiller.clone(),
            _r: Default::default(),
        };

        collect_and_verify_blocks(&mut stream, &spiller, &expected_blocks).await
    }

    #[tokio::test]
    async fn test_take_next_bounded_spillable() -> Result<()> {
        let spiller = MockSpiller {
            map: Arc::new(Mutex::new(HashMap::new())),
        };

        // Test with ascending Int32 type
        {
            let sort_desc = [SortColumnDescription {
                offset: 0,
                asc: true,
                nulls_first: false,
            }];

            // Test 1: Basic test with bound = 5 (should return blocks with values <= 5)
            // No spilled blocks, no sliced blocks
            run_take_next_bounded_spillable::<SimpleRowsAsc<Int32Type>>(
                spiller.clone(),
                &sort_desc,
                Some(Scalar::Number(NumberScalar::Int32(5))),
                vec![Int32Type::from_data(vec![3, 5])],
                false,
                false,
            )
            .await?;

            // Test 2: With spilled blocks, bound = 8 (should return blocks with values <= 8)
            run_take_next_bounded_spillable::<SimpleRowsAsc<Int32Type>>(
                spiller.clone(),
                &sort_desc,
                Some(Scalar::Number(NumberScalar::Int32(8))),
                vec![
                    Int32Type::from_data(vec![3, 5]),
                    Int32Type::from_data(vec![7, 7, 8]),
                ],
                true,
                false,
            )
            .await?;

            // Test 3: With sliced blocks, bound = 7 (should return blocks with values <= 7)
            run_take_next_bounded_spillable::<SimpleRowsAsc<Int32Type>>(
                spiller.clone(),
                &sort_desc,
                Some(Scalar::Number(NumberScalar::Int32(7))),
                vec![
                    Int32Type::from_data(vec![3, 5]),
                    Int32Type::from_data(vec![7, 7]),
                ],
                false,
                true,
            )
            .await?;

            // Test 4: With both spilled and sliced blocks, bound = 10
            run_take_next_bounded_spillable::<SimpleRowsAsc<Int32Type>>(
                spiller.clone(),
                &sort_desc,
                Some(Scalar::Number(NumberScalar::Int32(10))),
                vec![
                    Int32Type::from_data(vec![3, 5]),
                    Int32Type::from_data(vec![7, 7, 8]),
                    Int32Type::from_data(vec![10]),
                ],
                true,
                true,
            )
            .await?;

            // Test 5: With bound = 2 (should return no blocks as all values > 2)
            run_take_next_bounded_spillable::<SimpleRowsAsc<Int32Type>>(
                spiller.clone(),
                &sort_desc,
                Some(Scalar::Number(NumberScalar::Int32(2))),
                vec![],
                true,
                true,
            )
            .await?;

            // Test 6: With bound = 12 (should return all blocks as all values <= 12)
            run_take_next_bounded_spillable::<SimpleRowsAsc<Int32Type>>(
                spiller.clone(),
                &sort_desc,
                Some(Scalar::Number(NumberScalar::Int32(12))),
                vec![
                    Int32Type::from_data(vec![3, 5]),
                    Int32Type::from_data(vec![7, 7, 8]),
                    Int32Type::from_data(vec![10, 11, 11]),
                ],
                true,
                false,
            )
            .await?;

            // Test 7: With no bound (should return all blocks)
            run_take_next_bounded_spillable::<SimpleRowsAsc<Int32Type>>(
                spiller.clone(),
                &sort_desc,
                None,
                vec![
                    Int32Type::from_data(vec![3, 5]),
                    Int32Type::from_data(vec![7, 7, 8]),
                    Int32Type::from_data(vec![10, 11, 11]),
                ],
                true,
                false,
            )
            .await?;
        }

        // Test with descending String type
        {
            let sort_desc = [SortColumnDescription {
                offset: 1,
                asc: false,
                nulls_first: false,
            }];

            // Test 8: With bound = "f" (should return blocks with values >= "f")
            run_take_next_bounded_spillable::<SimpleRowsDesc<StringType>>(
                spiller.clone(),
                &sort_desc,
                Some(Scalar::String("f".to_string())),
                vec![
                    StringType::from_data(vec!["w", "h"]),
                    StringType::from_data(vec!["g", "f"]),
                ],
                false,
                false,
            )
            .await?;

            // Test 9: With spilled blocks, bound = "e" (should return blocks with values >= "e")
            run_take_next_bounded_spillable::<SimpleRowsDesc<StringType>>(
                spiller.clone(),
                &sort_desc,
                Some(Scalar::String("e".to_string())),
                vec![
                    StringType::from_data(vec!["w", "h"]),
                    StringType::from_data(vec!["g", "f", "e"]),
                    StringType::from_data(vec!["e"]),
                ],
                true,
                false,
            )
            .await?;

            // Test 10: With sliced blocks, bound = "d" (should return blocks with values >= "d")
            run_take_next_bounded_spillable::<SimpleRowsDesc<StringType>>(
                spiller.clone(),
                &sort_desc,
                Some(Scalar::String("d".to_string())),
                vec![
                    StringType::from_data(vec!["w", "h"]),
                    StringType::from_data(vec!["g", "f", "e"]),
                    StringType::from_data(vec!["e"]),
                    StringType::from_data(vec!["d", "d"]),
                ],
                false,
                true,
            )
            .await?;

            // Test 11: With both spilled and sliced blocks, bound = "c" (should return all blocks)
            run_take_next_bounded_spillable::<SimpleRowsDesc<StringType>>(
                spiller.clone(),
                &sort_desc,
                Some(Scalar::String("c".to_string())),
                vec![
                    StringType::from_data(vec!["w", "h"]),
                    StringType::from_data(vec!["g", "f", "e"]),
                    StringType::from_data(vec!["e"]),
                    StringType::from_data(vec!["d", "d"]),
                ],
                true,
                true,
            )
            .await?;

            // Test 12: With bound = "z" (should return no blocks as all values < "z")
            run_take_next_bounded_spillable::<SimpleRowsDesc<StringType>>(
                spiller.clone(),
                &sort_desc,
                Some(Scalar::String("z".to_string())),
                vec![],
                true,
                true,
            )
            .await?;
        }

        Ok(())
    }

    #[derive(Clone)]
    struct MockSpiller {
        map: Arc<Mutex<HashMap<String, DataBlock>>>,
    }

    #[async_trait::async_trait]
    impl DataBlockSpill for MockSpiller {
        async fn spill(&self, data_block: DataBlock) -> Result<Location> {
            let name = GlobalUniq::unique();
            self.map.lock().unwrap().insert(name.clone(), data_block);
            Ok(Location::Remote(name))
        }

        async fn merge_and_spill(&self, _: Vec<DataBlock>) -> Result<Location> {
            unimplemented!()
        }

        async fn restore(&self, location: &Location) -> Result<DataBlock> {
            match location {
                Location::Remote(name) => Ok(self.map.lock().unwrap().get(name).unwrap().clone()),
                _ => unreachable!(),
            }
        }
    }
}
