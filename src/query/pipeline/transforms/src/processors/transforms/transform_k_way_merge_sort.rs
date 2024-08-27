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
use std::sync::Mutex;

use databend_common_exception::Result;
use databend_common_expression::BlockEntry;
use databend_common_expression::Column;
use databend_common_expression::DataBlock;
use databend_common_expression::DataSchemaRef;
use databend_common_expression::SortColumnDescription;
use databend_common_pipeline_core::processors::Event;
use databend_common_pipeline_core::processors::InputPort;
use databend_common_pipeline_core::processors::OutputPort;
use databend_common_pipeline_core::processors::Processor;

use super::sort::algorithm::SortAlgorithm;
use super::sort::Merger;
use super::sort::SortedStream;

struct TaskInput {
    input: Arc<InputPort>,
    buffer: Vec<DataBlock>,
    ready: bool,
}

impl TaskInput {
    fn new(input: Arc<InputPort>) -> TaskInput {
        TaskInput {
            input,
            buffer: Vec::new(),
            ready: false,
        }
    }

    fn pull(&mut self) -> Result<(bool, bool)> {
        while self.input.has_data() {
            let block = self.input.pull_data().unwrap()?;

            if self.buffer.is_empty() {
                self.buffer.push(block);
                continue;
            }

            let before = &self.buffer[0];

            let task_id = get_task_id(&before);
            assert_eq!(task_id, get_task_id(&block));

            let task_rows = get_task_rows(&before);
            debug_assert_eq!(task_rows, get_task_rows(&block));

            let rows = self.buffer.iter().map(|b| b.num_rows() as u32).sum::<u32>()
                + block.num_rows() as u32;

            if task_rows > rows {
                self.buffer.push(block);
                continue;
            } else if task_rows == rows {
                self.input.set_not_need_data();
                self.ready = true;
                return Ok((true, false));
            } else {
                unreachable!()
            }
        }
        self.ready = false;
        if self.input.is_finished() {
            return Ok((false, false));
        }
        self.input.set_need_data();
        Ok((false, true))
    }
}

fn get_task_id(block: &DataBlock) -> u32 {
    let n = block.num_columns();
    debug_assert!(n >= 4);
    let cols = block.columns();
    unwrap_u32(&cols[n - 3])
}

fn get_task_rows(block: &DataBlock) -> u32 {
    let n = block.num_columns();
    debug_assert!(n >= 4);
    let cols = block.columns();
    unwrap_u32(&cols[n - 2])
}

fn get_input_id(block: &DataBlock) -> u32 {
    let n = block.num_columns();
    debug_assert!(n >= 4);
    let cols = block.columns();
    unwrap_u32(&cols[n - 1])
}

fn unwrap_u32(entry: &BlockEntry) -> u32 {
    *entry
        .value
        .as_scalar()
        .unwrap()
        .as_number()
        .unwrap()
        .as_u_int32()
        .unwrap()
}

struct TaskStream {
    input: Arc<Mutex<TaskInput>>,
    id: u32,
    remove_order_col: bool,
}

impl TaskStream {
    fn new(input: Arc<Mutex<TaskInput>>, id: usize, remove_order_col: bool) -> TaskStream {
        TaskStream {
            input,
            id: id as u32,
            remove_order_col,
        }
    }
}

impl SortedStream for TaskStream {
    fn next(&mut self) -> Result<(Option<(DataBlock, Column)>, bool)> {
        let mut input = self.input.lock().unwrap();
        if !input.ready || input.buffer.is_empty() {
            match input.pull()? {
                (_, true) => return Ok((None, true)),
                (false, false) => return Ok((None, false)),
                (true, false) => (),
            }
        }

        for (i, b) in input.buffer.iter().enumerate() {
            if get_input_id(b) != self.id {
                continue;
            }
            let mut block = input.buffer.remove(i);

            let n = block.num_columns();
            let columns = block.columns();
            let task_rows = columns[n - 2].clone();
            let task_id = columns[n - 3].clone();
            let sort_col = columns[n - 4].value.as_column().unwrap().clone();
            if self.remove_order_col {
                block.pop_columns(4);
            } else {
                block.pop_columns(3);
            }
            block.add_column(task_id);
            block.add_column(task_rows);

            return Ok((Some((block, sort_col)), false));
        }
        unreachable!()
    }
}

pub struct KWayMergeWorkerProcessor<A>
where A: SortAlgorithm
{
    merger: Merger<A, TaskStream>,

    input: Arc<InputPort>,
    output: Arc<OutputPort>,

    output_data: VecDeque<DataBlock>,
}

impl<A> KWayMergeWorkerProcessor<A>
where A: SortAlgorithm
{
    pub fn create(
        input: Arc<InputPort>,
        stream_count: usize,
        output: Arc<OutputPort>,
        schema: DataSchemaRef,
        block_size: usize,
        limit: Option<usize>,
        sort_desc: Arc<Vec<SortColumnDescription>>,
        remove_order_col: bool,
    ) -> Result<Self> {
        let task_input = Arc::new(Mutex::new(TaskInput::new(input.clone())));

        let streams = (0..stream_count)
            .map(|id| TaskStream::new(task_input.clone(), id, remove_order_col))
            .collect::<Vec<_>>();

        let merger = Merger::<A, TaskStream>::create(schema, streams, sort_desc, block_size, limit);
        Ok(Self {
            merger,
            input,
            output,
            output_data: VecDeque::new(),
        })
    }
}

impl<A> Processor for KWayMergeWorkerProcessor<A>
where A: SortAlgorithm + Send + 'static
{
    fn name(&self) -> String {
        "KWayMergeWorker".to_string()
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
            return Ok(Event::NeedConsume);
        }

        if let Some(block) = self.output_data.pop_front() {
            self.output.push_data(Ok(block));
            return Ok(Event::NeedConsume);
        }

        if self.merger.is_finished() {
            self.output.finish();
            self.input.finish();
            return Ok(Event::Finished);
        }

        self.merger.poll_pending_stream()?;

        if self.merger.has_pending_stream() {
            Ok(Event::NeedData)
        } else {
            Ok(Event::Sync)
        }
    }

    fn process(&mut self) -> Result<()> {
        while let Some(block) = self.merger.next_block()? {
            self.output_data.push_back(block);
        }
        Ok(())
    }
}
