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
use std::sync::Arc;

use databend_common_exception::Result;
use databend_common_expression::types::ArgType;
use databend_common_expression::types::UInt32Type;
use databend_common_expression::types::ValueType;
use databend_common_expression::BlockEntry;
use databend_common_expression::DataBlock;
use databend_common_expression::DataSchemaRef;
use databend_common_expression::SortColumnDescription;
use databend_common_expression::Value;

use super::Rows;
use super::SortedStream;

pub struct KWaySortPartition<R, S>
where
    R: Rows,
    S: SortedStream,
{
    schema: DataSchemaRef,
    sort_desc: Arc<Vec<SortColumnDescription>>,
    unsorted_streams: Vec<S>,
    buffer: Vec<DataBlock>,
    rows: Vec<Option<R>>,
    pending_streams: VecDeque<usize>,
    _limit: Option<usize>, // todo

    total_rows: usize,
    cur_task: u32,
}

impl<R, S> KWaySortPartition<R, S>
where
    R: Rows,
    S: SortedStream,
{
    pub fn new(
        schema: DataSchemaRef,
        streams: Vec<S>,
        sort_desc: Arc<Vec<SortColumnDescription>>,
        // batch_rows: usize,
        limit: Option<usize>,
    ) -> Self {
        // We only create a merger when there are at least two streams.
        debug_assert!(streams.len() > 1, "streams.len() = {}", streams.len());

        let buffer = vec![DataBlock::empty_with_schema(schema.clone()); streams.len()];
        let rows = vec![None; streams.len()];
        let pending_streams = (0..streams.len()).collect();

        Self {
            schema,
            sort_desc,
            unsorted_streams: streams,
            buffer,
            rows,
            pending_streams,
            _limit: limit,
            total_rows: 0,
            cur_task: 1,
        }
    }

    pub fn is_finished(&self) -> bool {
        self.buffer.iter().all(|b| b.is_empty()) && !self.has_pending_stream()
        // && self.temp_sorted_num_rows == 0)
        // || self.limit == Some(0)
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
                self.rows[i] = Some(R::from_column(&col, &self.sort_desc)?);
                self.buffer[i] = block;
            }
        }
        self.pending_streams.extend(continue_pendings);
        Ok(())
    }

    pub fn calc_partition_point(&self) -> Vec<(usize, usize)> {
        let task_max: Option<R::Item<'_>> =
            (0..self.buffer.len()).fold(None, |acc, i| match (acc, self.last(i)) {
                (Some(acc), Some(last)) => Some(acc.min(last)),
                (None, v @ Some(_)) | (v @ Some(_), None) => v,
                (None, None) => None,
            });

        let mut task = Vec::new();
        let task_max = match task_max {
            Some(task_max) => task_max,
            None => return task,
        };

        for i in 0..self.buffer.len() {
            match self.first(i) {
                None => continue,
                Some(first) if first > task_max => {
                    continue;
                }
                _ => (),
            }

            if self.last(i).unwrap() <= task_max {
                task.push((i, self.buffer[i].num_rows()))
            } else {
                let pp = self.rows_partition_point(i, &task_max);
                task.push((i, pp))
            };
        }
        task
    }

    pub fn next_task(&mut self) -> Result<Vec<DataBlock>> {
        // if self.is_finished() {
        //     return Ok(None);
        // }

        if self.has_pending_stream() {
            self.poll_pending_stream()?;
            if self.has_pending_stream() {
                return Ok(vec![]);
            }
        }

        Ok(self.build_task())
    }

    pub fn build_task(&mut self) -> Vec<DataBlock> {
        let partition_points = self.calc_partition_point();

        let task_id = self.next_task_id();
        let rows: usize = partition_points.iter().map(|(_, pp)| *pp).sum();
        self.total_rows += rows;
        let rows = u32_entry(rows as u32);

        let task: Vec<_> = partition_points
            .iter()
            .map(|&(input, pp)| {
                let block = self.slice(input, pp);

                let mut columns = Vec::with_capacity(block.num_columns() + 4);
                columns.extend_from_slice(block.columns());
                columns.push(task_id.clone());
                columns.push(rows.clone());
                columns.push(u32_entry(input as u32));

                DataBlock::new(columns, block.num_rows())
            })
            .collect();
        task
    }

    fn next_task_id(&mut self) -> BlockEntry {
        let id = self.cur_task;
        self.cur_task += 1;
        u32_entry(id)
    }

    fn first(&self, i: usize) -> Option<R::Item<'_>> {
        self.rows[i].as_ref().map(|rows| rows.first())
    }

    fn last(&self, i: usize) -> Option<R::Item<'_>> {
        self.rows[i].as_ref().map(|rows| rows.last())
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

    fn rows_partition_point<'a>(&'a self, i: usize, target: &R::Item<'a>) -> usize {
        let rows = self.rows[i].as_ref().unwrap();

        // INVARIANTS:
        // - 0 <= left <= left + size = right <= self.len()
        // - f returns Less for everything in self[..left]
        // - f returns Greater for everything in self[right..]
        let mut size = rows.len();
        let mut left = 0;
        let mut right = size;
        while left < right {
            let mid = left + size / 2;

            (left, right) = if rows.row(mid).cmp(target) == Ordering::Greater {
                (left, mid)
            } else {
                (mid + 1, right)
            };

            size = right - left;
        }

        // SAFETY: directly true from the overall invariant.
        // Note that this is `<=`, unlike the assume in the `Ok` path.
        // unsafe { std::hint::assert_unchecked(left <= self.len()) };
        left
    }
}

fn u32_entry(v: u32) -> BlockEntry {
    BlockEntry::new(
        UInt32Type::data_type(),
        Value::Scalar(UInt32Type::upcast_scalar(v)),
    )
}
