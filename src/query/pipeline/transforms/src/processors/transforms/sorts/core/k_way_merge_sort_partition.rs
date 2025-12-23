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

use std::cmp::Ordering;
use std::collections::VecDeque;

use databend_common_exception::Result;
use databend_common_expression::DataBlock;
use databend_common_expression::DataSchemaRef;

use super::Rows;
use super::SortedStream;
use super::list_domain::Candidate;
use super::list_domain::EndDomain;
use super::list_domain::List;
use super::list_domain::Partition;
use crate::sorts::SortTaskMeta;

pub struct KWaySortPartitioner<R, S>
where
    R: Rows,
    S: SortedStream,
{
    schema: DataSchemaRef,
    unsorted_streams: Vec<S>,
    pending_streams: VecDeque<usize>,

    buffer: Vec<DataBlock>,
    rows: Vec<Option<R>>,
    cur_task: usize,

    limit: Option<usize>,
    total_rows: usize,

    // settings
    min_task: usize,
    max_task: usize,
    max_iter: usize,
    search_per_iter: usize,
}

impl<R, S> KWaySortPartitioner<R, S>
where
    R: Rows,
    S: SortedStream,
{
    pub fn new(
        schema: DataSchemaRef,
        streams: Vec<S>,
        batch_rows: usize,
        limit: Option<usize>,
    ) -> Self {
        debug_assert!(streams.len() > 1, "streams.len() = {}", streams.len());

        let buffer = vec![DataBlock::empty_with_schema(schema.clone()); streams.len()];
        let rows = vec![None; streams.len()];
        let pending_streams = (0..streams.len()).collect();

        fn get_env<T: std::str::FromStr + Copy>(key: &str, default: T) -> T {
            std::env::var(key).map_or(default, |s| s.parse::<T>().unwrap_or(default))
        }

        let min_task =
            (batch_rows as f64 * get_env("K_WAY_MERGE_SORT_MIN_TASK_FACTOR", 2.0)) as usize;
        assert!(min_task > 0);

        let max_task =
            (batch_rows as f64 * get_env("K_WAY_MERGE_SORT_MAX_TASK_FACTOR", 4.0)) as usize;
        assert!(max_task > 0);

        let max_iter = get_env("K_WAY_MERGE_SORT_MAX_ITER", 20);
        assert!(max_iter > 0);

        let search_per_iter = get_env("K_WAY_MERGE_SORT_SEARCH_PER_ITER", 4);
        assert!(search_per_iter > 0);

        Self {
            schema,
            unsorted_streams: streams,
            pending_streams,
            buffer,
            rows,
            cur_task: 1,
            limit,
            total_rows: 0,

            min_task,
            max_task,
            max_iter,
            search_per_iter,
        }
    }

    pub fn is_finished(&self) -> bool {
        self.limit.is_some_and(|limit| self.total_rows >= limit)
            || !self.has_pending_stream() && self.rows.iter().all(|x| x.is_none())
    }

    pub fn has_pending_stream(&self) -> bool {
        !self.pending_streams.is_empty()
    }

    pub fn poll_pending_stream(&mut self) -> Result<()> {
        let mut continue_pendings = Vec::new();
        while let Some(i) = self.pending_streams.pop_front() {
            debug_assert!(self.buffer[i].is_empty());
            let (input, pending) = self.unsorted_streams[i].next()?;
            if pending {
                continue_pendings.push(i);
                continue;
            }
            if let Some((block, col)) = input {
                self.rows[i] = Some(R::from_column(&col)?);
                self.buffer[i] = block;
            }
        }
        self.pending_streams.extend(continue_pendings);
        Ok(())
    }

    pub fn next_task(&mut self) -> Result<Vec<DataBlock>> {
        if self.is_finished() {
            return Ok(vec![]);
        }

        if self.has_pending_stream() {
            self.poll_pending_stream()?;
            if self.has_pending_stream() {
                return Ok(vec![]);
            }
        }

        if self.is_finished() {
            return Ok(vec![]);
        }

        Ok(self.build_task())
    }

    fn calc_partition_point(&self) -> Partition {
        let mut candidate =
            Candidate::new(&self.rows, EndDomain::new(self.min_task, self.max_task));

        let ok = candidate.init();
        assert!(ok, "empty candidate");

        // if candidate.is_small_task() {
        // todo: Consider loading multiple blocks at the same time so that we can avoid cutting out too small a task
        // }

        candidate.calc_partition(self.search_per_iter, self.max_iter)
    }

    fn build_task(&mut self) -> Vec<DataBlock> {
        let partition = self.calc_partition_point();
        assert!(partition.total > 0);

        let id = self.next_task_id();
        self.total_rows += partition.total;

        let task: Vec<_> = partition
            .ends
            .iter()
            .map(|&(input, pp)| {
                let mut block = self.slice(input, pp);

                block.replace_meta(Box::new(SortTaskMeta {
                    id,
                    total: partition.total,
                    input,
                }));

                block
            })
            .collect();
        task
    }

    fn next_task_id(&mut self) -> usize {
        let id = self.cur_task;
        self.cur_task += 1;
        id
    }

    fn slice(&mut self, i: usize, pp: usize) -> DataBlock {
        let block = &self.buffer[i];
        let rows = self.rows[i].as_ref();
        let n = block.num_rows();

        if pp < n {
            let first_block = block.slice(0..pp);
            self.buffer[i] = block.slice(pp..n);
            self.rows[i] = Some(rows.unwrap().slice(pp..n));
            first_block
        } else {
            let first_block = block.clone();
            self.buffer[i] = DataBlock::empty_with_schema(self.schema.clone());
            self.rows[i] = None;
            self.pending_streams.push_back(i);
            first_block
        }
    }
}

impl<R: Rows> List for Option<R> {
    type Item<'a>
        = R::Item<'a>
    where R: 'a;
    fn len(&self) -> usize {
        match self {
            Some(r) => r.len(),
            None => 0,
        }
    }

    fn cmp_value<'a>(&'a self, i: usize, target: &R::Item<'a>) -> Ordering {
        let rows = self.as_ref().unwrap();
        assert!(i < rows.len(), "len {}, index {}", rows.len(), i);
        rows.row(i).cmp(target)
    }

    fn index(&self, i: usize) -> R::Item<'_> {
        let rows = self.as_ref().unwrap();
        assert!(i < rows.len(), "len {}, index {}", rows.len(), i);
        rows.row(i)
    }
}
