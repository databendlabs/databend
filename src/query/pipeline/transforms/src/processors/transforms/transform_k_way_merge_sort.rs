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
use databend_common_expression::BlockEntry;
use databend_common_expression::Column;
use databend_common_expression::DataBlock;
use databend_common_expression::DataSchemaRef;
use databend_common_expression::SortColumnDescription;
use databend_common_expression::Value;
use databend_common_pipeline_core::processors::Event;
use databend_common_pipeline_core::processors::InputPort;
use databend_common_pipeline_core::processors::OutputPort;
use databend_common_pipeline_core::processors::Processor;
use databend_common_pipeline_core::processors::ProcessorPtr;
use databend_common_pipeline_core::Pipe;
use databend_common_pipeline_core::PipeItem;

use super::sort::algorithm::SortAlgorithm;
use super::sort::KWaySortPartition;
use super::sort::Merger;
use super::sort::Rows;
use super::sort::SortedStream;
use super::transform_multi_sort_merge::InputBlockStream;

pub fn create_pipe<R: Rows + Send + 'static>(
    input_ports: Vec<Arc<InputPort>>,
    worker: usize,
    schema: DataSchemaRef,
    sort_desc: Arc<Vec<SortColumnDescription>>,
    limit: Option<usize>,
) -> Pipe {
    let output_ports = vec![OutputPort::create(); worker];
    let processor = ProcessorPtr::create(Box::new(KWayMergePartitionProcessor::<R>::create(
        input_ports.clone(),
        output_ports.clone(),
        schema,
        sort_desc,
        limit,
    )));

    Pipe::create(input_ports.len(), worker, vec![PipeItem::create(
        processor,
        input_ports,
        output_ports,
    )])
}

pub struct KWayMergePartitionProcessor<R: Rows> {
    partition: KWaySortPartition<R, InputBlockStream>,
    inputs: Vec<Arc<InputPort>>,
    outputs: Vec<Arc<OutputPort>>,

    task: VecDeque<DataBlock>,
    cur: Option<usize>,
    next: usize,
}

impl<R: Rows> KWayMergePartitionProcessor<R> {
    pub fn create(
        inputs: Vec<Arc<InputPort>>,
        outputs: Vec<Arc<OutputPort>>,
        schema: DataSchemaRef,
        sort_desc: Arc<Vec<SortColumnDescription>>,
        // batch_rows: usize,
        limit: Option<usize>,
    ) -> Self {
        let streams = inputs
            .iter()
            .map(|i| InputBlockStream::new(i.clone(), false))
            .collect::<Vec<_>>();

        Self {
            partition: KWaySortPartition::create(schema, streams, sort_desc, limit),
            inputs,
            outputs,
            task: VecDeque::new(),
            cur: None,
            next: 0,
        }
    }

    fn try_push(&self) -> Result<Event> {
        // let output = &self.outputs[self.cur];

        todo!()

        // if output.is_finished() {
        //     todo!()
        // }

        // if !output.can_push() {
        //     return Ok(Event::NeedConsume);
        // }
    }

    fn find_output(&mut self) -> Option<usize> {
        let n = self.outputs.len();
        for mut i in self.next..self.next + n {
            if i >= n {
                i = i - n;
            }
            if self.outputs[i].can_push() {
                self.cur = Some(i);
                self.next = if i + 1 == n { 0 } else { i + 1 };
                return self.cur;
            }
        }
        None
    }
}

impl<R> Processor for KWayMergePartitionProcessor<R>
where R: Rows + Send + 'static
{
    fn name(&self) -> String {
        "KWayMergePartition".to_string()
    }

    fn as_any(&mut self) -> &mut dyn Any {
        self
    }

    fn event(&mut self) -> Result<Event> {
        // todo!()
        // if self.outputs.is_finished() {
        //     for input in self.inputs.iter() {
        //         input.finish();
        //     }
        //     return Ok(Event::Finished);
        // }

        if !self.task.is_empty() {
            let output = match self.cur {
                Some(i) => &self.outputs[i],
                None => match self.find_output() {
                    Some(cur) => &self.outputs[cur],
                    None => return Ok(Event::NeedConsume),
                },
            };

            if !output.can_push() {
                return Ok(Event::NeedConsume);
            }

            if let Some(block) = self.task.pop_front() {
                output.push_data(Ok(block));
                if self.task.is_empty() {
                    self.cur = None;
                }
                return Ok(Event::NeedConsume);
            }
        } else {
            match self.find_output() {
                Some(_) => (),
                None => return Ok(Event::NeedConsume),
            };
        }

        // if self.merger.is_finished() {
        //     self.output.finish();
        //     for input in self.inputs.iter() {
        //         input.finish();
        //     }
        //     return Ok(Event::Finished);
        // }

        self.partition.poll_pending_stream()?;
        if self.partition.has_pending_stream() {
            Ok(Event::NeedData)
        } else {
            Ok(Event::Sync)
        }

        // match self.pull()? {
        //     (_, true) => return Ok(Event::NeedData),
        //     (false, false) => return Ok(Event::Finished),
        //     (true, false) => Ok(Event::Sync),
        // }
    }

    fn process(&mut self) -> Result<()> {
        let task = self.partition.next_task()?;
        self.task.extend(task.into_iter());
        Ok(())
    }
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
        if self.input.has_data() {
            let block = self.input.pull_data().unwrap()?;
            self.input.set_need_data();

            if self.buffer.is_empty() {
                self.buffer.push(block);
                return Ok((false, true));
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
                return Ok((false, true));
            } else if task_rows == rows {
                self.buffer.push(block);
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

        if self.ready {
            return Ok(Event::Sync);
        }

        match self.pull()? {
            (_, true) => Ok(Event::NeedData),
            (false, false) => Ok(Event::Finished),
            (true, false) => Ok(Event::Sync),
        }
    }

    fn process(&mut self) -> Result<()> {
        debug_assert!(self.ready);
        self.ready = false;
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

    cur: Option<usize>,
    next_task: u32,
    buffer: Vec<VecDeque<DataBlock>>,
    info: Vec<Option<Info>>,
}

#[derive(Debug, Clone, Copy)]
struct Info {
    task_id: u32,
    total: usize,
    remain: usize,
}

impl KWayMergeCombineProcessor {
    pub fn create(
        inputs: Vec<Arc<InputPort>>,
        output: Arc<OutputPort>,
        limit: Option<usize>,
    ) -> Result<Self> {
        let buffer = vec![VecDeque::new(); inputs.len()];
        let info = vec![None; inputs.len()];
        Ok(Self {
            inputs,
            output,
            cur: None,
            next_task: 1,
            buffer,
            info,
        })
    }

    fn pull(&mut self, i: usize) -> Result<(bool, bool)> {
        let input = &self.inputs[i];
        let buffer = &mut self.buffer[i];
        let info = &mut self.info[i];

        const TASK_ID_POS: usize = 2;
        const TASK_ROWS_POS: usize = 1;

        if input.has_data() {
            let mut block = input.pull_data().unwrap()?;

            match info {
                None => {
                    let task_id = get_u32(&block, TASK_ID_POS);
                    debug_assert!(task_id >= self.next_task);
                    let task_rows = get_u32(&block, TASK_ROWS_POS);
                    let remain = task_rows as usize - block.num_rows();
                    let _ = info.insert(Info {
                        task_id,
                        total: task_rows as usize,
                        remain,
                    });
                    block.pop_columns(2);
                    buffer.push_back(block);

                    if task_id > self.next_task {
                        return Ok((false, true));
                    } else {
                        input.set_need_data();
                        return Ok((true, false));
                    }
                }
                Some(info) => {
                    debug_assert_eq!(info.task_id, get_u32(&block, TASK_ID_POS));
                    debug_assert_eq!(info.total, get_u32(&block, TASK_ROWS_POS) as usize);

                    let rows = block.num_rows();
                    if rows <= info.remain {
                        block.pop_columns(2);
                        buffer.push_back(block);
                        input.set_need_data();
                        info.remain -= rows;
                        return Ok((true, false));
                    } else {
                        unreachable!()
                    }
                }
            }
        }

        if input.is_finished() {
            return Ok((false, false));
        }
        input.set_need_data();
        Ok((false, true))
    }

    fn find(&mut self) -> Result<Event> {
        for i in 0..self.inputs.len() {
            match self.pull(i)? {
                (_, true) => return Ok(Event::NeedData),
                (false, false) => todo!(),
                (true, false) => {
                    self.cur = Some(i);
                    return Ok(self.push());
                }
            }
        }
        Ok(Event::NeedData)
    }

    fn push(&mut self) -> Event {
        let cur = self.cur.unwrap();
        let buffer = &mut self.buffer[cur];
        let info = &mut self.info[cur];
        if let Some(block) = buffer.pop_front() {
            self.output.push_data(Ok(block));
            if buffer.is_empty() && info.unwrap().remain == 0 {
                self.next_task += 1;
                info.take();
                self.cur = None;
            }
            Event::NeedConsume
        } else {
            Event::NeedData
        }
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

        match self.cur {
            Some(cur) => match self.push() {
                Event::NeedConsume => return Ok(Event::NeedConsume),
                Event::NeedData => match self.pull(cur)? {
                    (_, true) => return Ok(Event::NeedData),
                    (true, false) => return Ok(self.push()),
                    (false, false) => todo!(),
                },
                _ => unreachable!(),
            },
            None => self.find(),
        }

        // match self.pull()? {
        //     (_, true) => return Ok(Event::NeedData),
        //     (false, false) => {
        //         return Ok(Event::Finished);
        //         // todo
        //     }
        //     (true, false) => {
        //         while let Some(block) = self.buffer.pop_front() {
        //             self.output.push_data(Ok(block));
        //         }
        //         return Ok(Event::NeedConsume);
        //     }
        // }
    }
}

type BlockStream = VecDeque<(DataBlock, Column)>;

impl SortedStream for BlockStream {
    fn next(&mut self) -> Result<(Option<(DataBlock, Column)>, bool)> {
        Ok((self.pop_front(), false))
    }
}

fn get_u32(block: &DataBlock, i: usize) -> u32 {
    let n = block.num_columns();
    let cols = block.columns();
    unwrap_u32(&cols[n - i])
}

fn unwrap_u32(entry: &BlockEntry) -> u32 {
    match &entry.value {
        Value::Scalar(scalar) => *scalar.as_number().unwrap().as_u_int32().unwrap(),
        Value::Column(column) => column.as_number().unwrap().as_u_int32().unwrap()[0],
    }
}
