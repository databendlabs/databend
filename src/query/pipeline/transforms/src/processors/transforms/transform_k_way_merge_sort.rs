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

pub struct KWayMergePartitionProcessor {}

impl Processor for KWayMergePartitionProcessor {
    fn name(&self) -> String {
        "KWayMergePartition".to_string()
    }

    fn as_any(&mut self) -> &mut dyn Any {
        self
    }
}

fn get_u32(block: &DataBlock, i: usize) -> u32 {
    let n = block.num_columns();
    let cols = block.columns();
    unwrap_u32(&cols[n - i])
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

pub struct KWayMergeWorkerProcessor<A>
where A: SortAlgorithm
{
    input: Arc<InputPort>,
    output: Arc<OutputPort>,
    stream_count: usize,
    schema: DataSchemaRef,
    block_size: usize,
    sort_desc: Arc<Vec<SortColumnDescription>>,
    remove_order_col: bool,

    buffer: Vec<DataBlock>,
    ready: bool,
    merger: Option<Merger<A, BlockStream>>,
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
        sort_desc: Arc<Vec<SortColumnDescription>>,
        remove_order_col: bool,
    ) -> Result<Self> {
        Ok(Self {
            merger: None,
            input,
            output,
            output_data: VecDeque::new(),
            stream_count,
            schema,
            block_size,
            sort_desc,
            remove_order_col,
            buffer: Vec::new(),
            ready: false,
        })
    }

    fn pull(&mut self) -> Result<(bool, bool)> {
        while self.input.has_data() {
            let block = self.input.pull_data().unwrap()?;

            if self.buffer.is_empty() {
                self.buffer.push(block);
                continue;
            }

            let before = &self.buffer[0];

            const TASK_ID_POS: usize = 3;
            const TASK_ROWS_POS: usize = 2;

            let task_id = get_u32(&before, TASK_ID_POS);
            assert_eq!(task_id, get_u32(&block, TASK_ID_POS));

            let task_rows = get_u32(&before, TASK_ROWS_POS);
            debug_assert_eq!(task_rows, get_u32(&block, TASK_ROWS_POS));

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

    fn streams(&mut self) -> Vec<BlockStream> {
        const INPUT_ID_POS: usize = 1;

        let mut streams = vec![VecDeque::new(); self.stream_count];

        for mut block in self.buffer.drain(..) {
            let id = get_u32(&block, INPUT_ID_POS) as usize;

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

            streams[id].push_back((block, sort_col));
        }
        self.ready = false;
        streams
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

        match self.pull()? {
            (_, true) => return Ok(Event::NeedData),
            (false, false) => return Ok(Event::Finished),
            (true, false) => Ok(Event::Sync),
        }
    }

    fn process(&mut self) -> Result<()> {
        debug_assert!(self.ready);
        let mut merger = Merger::<A, BlockStream>::create(
            self.schema.clone(),
            self.streams(),
            self.sort_desc.clone(),
            self.block_size,
            None,
        );
        while let Some(block) = merger.next_block()? {
            self.output_data.push_back(block);
        }
        debug_assert!(merger.is_finished());
        Ok(())
    }
}

pub struct KWayMergeCombineProcessor {
    inputs: Vec<Arc<InputPort>>,
    output: Arc<OutputPort>,

    cur: usize,
    buffer: VecDeque<DataBlock>,
}

impl KWayMergeCombineProcessor {
    pub fn create(
        inputs: Vec<Arc<InputPort>>,
        output: Arc<OutputPort>,
        limit: Option<usize>,
    ) -> Result<Self> {
        Ok(Self {
            inputs,
            output,
            cur: 0,
            buffer: VecDeque::new(),
        })
    }

    fn pull(&mut self) -> Result<(bool, bool)> {
        let input = &self.inputs[self.cur];

        while input.has_data() {
            let block = input.pull_data().unwrap()?;

            if self.buffer.is_empty() {
                self.buffer.push_back(block);
                continue;
            }

            let before = &self.buffer[0];

            const TASK_ID_POS: usize = 2;
            const TASK_ROWS_POS: usize = 1;

            let task_id = get_u32(&before, TASK_ID_POS);
            assert_eq!(task_id, get_u32(&block, TASK_ID_POS));

            let task_rows = get_u32(&before, TASK_ROWS_POS);
            debug_assert_eq!(task_rows, get_u32(&block, TASK_ROWS_POS));

            let rows = self.buffer.iter().map(|b| b.num_rows() as u32).sum::<u32>()
                + block.num_rows() as u32;

            if task_rows > rows {
                self.buffer.push_back(block);
                continue;
            } else if task_rows == rows {
                input.set_not_need_data();
                self.cur += 1;
                return Ok((true, false));
            } else {
                unreachable!()
            }
        }
        if input.is_finished() {
            return Ok((false, false));
        }
        input.set_need_data();
        Ok((false, true))
    }
}

impl Processor for KWayMergeCombineProcessor {
    fn name(&self) -> String {
        "KWayMergeCombine".to_string()
    }

    fn as_any(&mut self) -> &mut dyn Any {
        self
    }

    fn event(&mut self) -> Result<Event> {
        if self.output.is_finished() {
            for input in self.inputs.iter() {
                input.finish();
            }
            return Ok(Event::Finished);
        }

        if !self.output.can_push() {
            return Ok(Event::NeedConsume);
        }

        if !self.buffer.is_empty() {
            while let Some(block) = self.buffer.pop_front() {
                self.output.push_data(Ok(block));
            }
            return Ok(Event::NeedConsume);
        }

        let (data, pending) = self.pull()?;

        match self.pull()? {
            (_, true) => return Ok(Event::NeedData),
            (false, false) => {
                return Ok(Event::Finished);
                // todo
            }
            (true, false) => {
                while let Some(block) = self.buffer.pop_front() {
                    self.output.push_data(Ok(block));
                }
                return Ok(Event::NeedConsume);
            }
        }
    }
}

type BlockStream = VecDeque<(DataBlock, Column)>;

impl SortedStream for BlockStream {
    fn next(&mut self) -> Result<(Option<(DataBlock, Column)>, bool)> {
        Ok((self.pop_front(), false))
    }
}
