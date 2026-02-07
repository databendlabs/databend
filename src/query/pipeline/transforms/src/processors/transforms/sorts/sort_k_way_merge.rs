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
use std::ops::ControlFlow;
use std::sync::Arc;

use databend_common_exception::ErrorCode;
use databend_common_exception::Result;
use databend_common_expression::BlockMetaInfoDowncast;
use databend_common_expression::Column;
use databend_common_expression::DataBlock;
use databend_common_expression::DataSchemaRef;
use databend_common_expression::SortColumnDescription;
use databend_common_pipeline::core::Event;
use databend_common_pipeline::core::InputPort;
use databend_common_pipeline::core::OutputPort;
use databend_common_pipeline::core::Pipe;
use databend_common_pipeline::core::PipeItem;
use databend_common_pipeline::core::Pipeline;
use databend_common_pipeline::core::Processor;
use databend_common_pipeline::core::ProcessorPtr;

use super::InputBlockStream;
use super::SortTaskMeta;
use super::core::KWaySortPartitioner;
use super::core::Merger;
use super::core::RowConverter;
use super::core::Rows;
use super::core::RowsTypeVisitor;
use super::core::SortedStream;
use super::core::algorithm::HeapSort;
use super::core::algorithm::LoserTreeSort;
use super::core::algorithm::SortAlgorithm;
use super::core::select_row_type;

pub fn add_k_way_merge_sort(
    pipeline: &mut Pipeline,
    schema: DataSchemaRef,
    worker: usize,
    block_size: usize,
    limit: Option<usize>,
    sort_desc: &[SortColumnDescription],
    remove_order_col: bool,
    enable_loser_tree: bool,
    enable_fixed_rows: bool,
) -> Result<()> {
    if pipeline.is_empty() {
        return Err(ErrorCode::Internal("Cannot resize empty pipe."));
    }

    match pipeline.output_len() {
        0 => Err(ErrorCode::Internal("Cannot resize empty pipe.")),
        1 => Ok(()),
        stream_size => {
            let mut builder = Builder {
                schema,
                stream_size,
                worker,
                block_size,
                limit,
                remove_order_col,
                enable_loser_tree,
                sort_desc,
                pipeline,
            };
            select_row_type(&mut builder, enable_fixed_rows)
        }
    }
}

struct Builder<'a> {
    schema: DataSchemaRef,
    stream_size: usize,
    worker: usize,
    block_size: usize,
    limit: Option<usize>,
    remove_order_col: bool,
    enable_loser_tree: bool,
    sort_desc: &'a [SortColumnDescription],
    pipeline: &'a mut Pipeline,
}

impl RowsTypeVisitor for Builder<'_> {
    type Result = Result<()>;

    fn schema(&self) -> DataSchemaRef {
        self.schema.clone()
    }

    fn sort_desc(&self) -> &[SortColumnDescription] {
        self.sort_desc
    }

    fn visit_type<R, C>(&mut self) -> Self::Result
    where
        R: Rows + 'static,
        C: RowConverter<R> + Send + 'static,
    {
        if self.enable_loser_tree {
            self.build::<LoserTreeSort<R>>()
        } else {
            self.build::<HeapSort<R>>()
        }
    }
}

impl Builder<'_> {
    fn build<A>(&mut self) -> Result<()>
    where
        A: SortAlgorithm + Send + 'static,
        A::Rows: 'static,
    {
        self.pipeline
            .add_pipe(self.create_partitioner::<A>(self.stream_size));

        self.pipeline.add_transform(|input, output| {
            Ok(ProcessorPtr::create(Box::new(
                KWayMergeWorkerProcessor::<A>::new(
                    input,
                    self.stream_size,
                    output,
                    self.block_size,
                    self.remove_order_col,
                ),
            )))
        })?;

        self.pipeline.add_pipe(self.create_combiner());
        Ok(())
    }

    fn create_partitioner<A>(&self, input: usize) -> Pipe
    where
        A: SortAlgorithm + Send + 'static,
        A::Rows: 'static,
    {
        let inputs_port: Vec<_> = (0..input).map(|_| InputPort::create()).collect();
        let outputs_port: Vec<_> = (0..self.worker).map(|_| OutputPort::create()).collect();

        let processor =
            ProcessorPtr::create(Box::new(KWayMergePartitionerProcessor::<A::Rows>::new(
                inputs_port.clone(),
                outputs_port.clone(),
                self.schema.clone(),
                self.block_size,
                self.limit,
            )));

        Pipe::create(inputs_port.len(), self.worker, vec![PipeItem::create(
            processor,
            inputs_port,
            outputs_port,
        )])
    }

    fn create_combiner(&self) -> Pipe {
        let inputs_port = (0..self.worker)
            .map(|_| InputPort::create())
            .collect::<Vec<_>>();
        let output = OutputPort::create();

        let processor = ProcessorPtr::create(Box::new(KWayMergeCombinerProcessor::new(
            inputs_port.clone(),
            output.clone(),
            self.limit,
        )));

        Pipe::create(self.worker, 1, vec![PipeItem::create(
            processor,
            inputs_port,
            vec![output],
        )])
    }
}

pub struct KWayMergePartitionerProcessor<R: Rows> {
    partitioner: KWaySortPartitioner<R, InputBlockStream>,
    inputs: Vec<Arc<InputPort>>,
    outputs: Vec<Arc<OutputPort>>,

    task: VecDeque<DataBlock>,
    cur_output: Option<usize>,
    next_output: usize,
}

impl<R: Rows> KWayMergePartitionerProcessor<R> {
    pub fn new(
        inputs: Vec<Arc<InputPort>>,
        outputs: Vec<Arc<OutputPort>>,
        schema: DataSchemaRef,
        batch_rows: usize,
        limit: Option<usize>,
    ) -> Self {
        let streams = inputs
            .iter()
            .map(|i| InputBlockStream::new(i.clone(), false))
            .collect::<Vec<_>>();

        Self {
            partitioner: KWaySortPartitioner::new(schema, streams, batch_rows, limit),
            inputs,
            outputs,
            task: VecDeque::new(),
            cur_output: None,
            next_output: 0,
        }
    }

    fn find_output(&mut self) -> Option<usize> {
        let n = self.outputs.len();
        for mut i in self.next_output..self.next_output + n {
            if i >= n {
                i -= n;
            }
            if self.outputs[i].can_push() {
                self.cur_output = Some(i);
                self.next_output = if i + 1 == n { 0 } else { i + 1 };
                return self.cur_output;
            }
        }
        None
    }
}

impl<R> Processor for KWayMergePartitionerProcessor<R>
where R: Rows + 'static
{
    fn name(&self) -> String {
        "KWayMergePartitioner".to_string()
    }

    fn as_any(&mut self) -> &mut dyn Any {
        self
    }

    fn event(&mut self) -> Result<Event> {
        if self.outputs.iter().all(|o| o.is_finished()) {
            self.inputs.iter().for_each(|i| i.finish());
            return Ok(Event::Finished);
        }

        if !self.task.is_empty() {
            let output = match self.cur_output {
                Some(i) => &self.outputs[i],
                None => match self.find_output() {
                    Some(cur) => &self.outputs[cur],
                    None => return Ok(Event::NeedConsume),
                },
            };

            if !output.can_push() {
                return Ok(Event::NeedConsume);
            }

            let block = self.task.pop_front().unwrap();
            debug_assert!(block.num_rows() > 0);
            output.push_data(Ok(block));
            if self.task.is_empty() {
                self.cur_output = None;
            }
            return Ok(Event::NeedConsume);
        }

        if self.partitioner.is_finished() {
            self.outputs.iter().for_each(|o| o.finish());
            self.inputs.iter().for_each(|i| i.finish());
            return Ok(Event::Finished);
        }

        if self.cur_output.is_none() && self.find_output().is_none() {
            return Ok(Event::NeedConsume);
        }

        self.partitioner.poll_pending_stream()?;
        if self.partitioner.has_pending_stream() {
            Ok(Event::NeedData)
        } else {
            Ok(Event::Sync)
        }
    }

    fn process(&mut self) -> Result<()> {
        let task = self.partitioner.next_task()?;
        self.task.extend(task);
        Ok(())
    }
}

pub struct KWayMergeWorkerProcessor<A>
where A: SortAlgorithm
{
    input: Arc<InputPort>,
    output: Arc<OutputPort>,
    stream_size: usize,
    batch_rows: usize,
    remove_order_col: bool,

    buffer: Vec<DataBlock>,
    task: Option<TaskState>,
    output_data: VecDeque<DataBlock>,
    _a: PhantomData<A>,
}

impl<A> KWayMergeWorkerProcessor<A>
where A: SortAlgorithm
{
    pub fn new(
        input: Arc<InputPort>,
        stream_size: usize,
        output: Arc<OutputPort>,
        batch_rows: usize,
        remove_order_col: bool,
    ) -> Self {
        Self {
            input,
            output,
            output_data: VecDeque::new(),
            stream_size,
            batch_rows,
            remove_order_col,
            buffer: Vec::new(),
            task: None,
            _a: Default::default(),
        }
    }

    fn ready(&self) -> bool {
        self.task.is_some_and(|state| state.done())
    }

    fn pull(&mut self) -> Result<Event> {
        if self.ready() {
            return Ok(Event::Sync);
        }
        if !self.input.has_data() {
            if self.input.is_finished() {
                self.output.finish();
                return Ok(Event::Finished);
            }
            self.input.set_need_data();
            return Ok(Event::NeedData);
        }

        let mut block = self.input.pull_data().unwrap()?;
        self.input.set_need_data();

        let incoming = SortTaskMeta::downcast_ref_from(block.get_meta().unwrap()).unwrap();

        let rows = block.num_rows();
        match &mut self.task {
            Some(task) => {
                debug_assert_eq!(incoming.id, task.id);
                debug_assert_eq!(incoming.total, task.total);
                assert!(task.remain >= rows);
                task.remain -= rows;

                if task.done() {
                    self.buffer.push(block);
                    Ok(Event::Sync)
                } else {
                    self.buffer.push(block);
                    Ok(Event::NeedData)
                }
            }
            None if incoming.total == rows => {
                if self.remove_order_col {
                    block.pop_columns(1);
                }

                self.output_data.push_back(block);
                Ok(Event::NeedConsume)
            }
            None => {
                assert!(incoming.total >= rows);
                self.task = Some(TaskState {
                    id: incoming.id,
                    total: incoming.total,
                    remain: incoming.total - rows,
                });
                self.buffer.push(block);
                Ok(Event::NeedData)
            }
        }
    }

    fn streams(&mut self) -> Vec<BlockStream> {
        let mut streams = vec![VecDeque::new(); self.stream_size];

        for mut block in self.buffer.drain(..) {
            let meta = SortTaskMeta::downcast_from(block.take_meta().unwrap()).unwrap();

            let sort_col = block.get_last_column().clone();
            if self.remove_order_col {
                block.pop_columns(1);
            }

            streams[meta.input].push_back((block, sort_col));
        }
        streams
    }
}

impl<A> Processor for KWayMergeWorkerProcessor<A>
where A: SortAlgorithm + 'static
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
            debug_assert!(block.num_rows() > 0);
            self.output.push_data(Ok(block));
            return Ok(Event::NeedConsume);
        }

        match self.pull()? {
            e @ (Event::NeedData | Event::Finished | Event::Sync) => Ok(e),
            Event::NeedConsume => {
                let block = self.output_data.pop_front().unwrap();
                debug_assert!(block.num_rows() > 0);
                self.output.push_data(Ok(block));
                Ok(Event::NeedConsume)
            }
            _ => unreachable!(),
        }
    }

    fn process(&mut self) -> Result<()> {
        debug_assert!(self.ready());
        let task = self.task.take().unwrap();

        let mut merger = Merger::<A, _>::new(
            self.streams(),
            if task.total > self.batch_rows {
                task.total / (task.total / self.batch_rows)
            } else {
                self.batch_rows
            },
            None,
        );

        let mut rows = 0;
        while let Some(block) = merger.next_block()? {
            rows += block.num_rows();

            let meta = SortTaskMeta {
                id: task.id,
                total: task.total,
                input: 0,
            }
            .boxed();
            self.output_data.push_back(block.add_meta(Some(meta))?);
        }
        debug_assert_eq!(rows, task.total);
        debug_assert!(merger.is_finished());
        Ok(())
    }
}

pub struct KWayMergeCombinerProcessor {
    inputs: Vec<Arc<InputPort>>,
    output: Arc<OutputPort>,

    cur_input: Option<usize>,
    cur_task: usize,
    buffer: Vec<VecDeque<DataBlock>>,
    state: Vec<Option<TaskState>>,
    limit: Option<usize>,
}

#[derive(Debug, Clone, Copy)]
struct TaskState {
    id: usize,
    total: usize,
    remain: usize,
}

impl TaskState {
    fn done(&self) -> bool {
        self.remain == 0
    }
}

impl KWayMergeCombinerProcessor {
    pub fn new(inputs: Vec<Arc<InputPort>>, output: Arc<OutputPort>, limit: Option<usize>) -> Self {
        let buffer = vec![VecDeque::new(); inputs.len()];
        let state = vec![None; inputs.len()];
        Self {
            inputs,
            output,
            cur_input: None,
            cur_task: 1,
            buffer,
            state,
            limit,
        }
    }

    fn pull(&mut self, i: usize) -> Result<PullEvent> {
        let input = &self.inputs[i];
        let buffer = &mut self.buffer[i];
        let cur_state = &mut self.state[i];

        if let Some(cur) = cur_state
            && cur.done()
        {
            return if cur.id == self.cur_task {
                Ok(PullEvent::Data)
            } else {
                Ok(PullEvent::Pending)
            };
        }

        if input.has_data() {
            let mut block = input.pull_data().unwrap()?;
            input.set_need_data();
            let incoming = SortTaskMeta::downcast_from(block.take_meta().unwrap()).unwrap();

            let task_id = match cur_state {
                None => {
                    debug_assert!(incoming.total > 0);
                    debug_assert!(
                        incoming.id >= self.cur_task,
                        "disorder task. cur_task: {} incoming.id: {} incoming.total: {}",
                        self.cur_task,
                        incoming.id,
                        incoming.total
                    );
                    let SortTaskMeta { id, total, .. } = incoming;
                    let _ = cur_state.insert(TaskState {
                        id,
                        total,
                        remain: total - block.num_rows(),
                    });
                    buffer.push_back(block);
                    id
                }
                Some(cur) => {
                    debug_assert_eq!(cur.id, incoming.id);
                    debug_assert_eq!(cur.total, incoming.total);

                    let rows = block.num_rows();
                    if rows <= cur.remain {
                        buffer.push_back(block);
                        cur.remain -= rows;
                        cur.id
                    } else {
                        unreachable!()
                    }
                }
            };
            return if task_id == self.cur_task {
                Ok(PullEvent::Data)
            } else {
                Ok(PullEvent::Pending)
            };
        }

        if !buffer.is_empty() {
            return if cur_state.unwrap().id == self.cur_task {
                Ok(PullEvent::Data)
            } else {
                Ok(PullEvent::Pending)
            };
        }

        if input.is_finished() {
            return Ok(PullEvent::Finished);
        }

        input.set_need_data();
        Ok(PullEvent::Pending)
    }

    fn push(&mut self) -> bool {
        let cur_input = self.cur_input.unwrap();
        let buffer = &mut self.buffer[cur_input];
        let cur_state = &mut self.state[cur_input];
        debug_assert_eq!(cur_state.unwrap().id, self.cur_task);
        if let Some(block) = buffer.pop_front() {
            let block = match &mut self.limit {
                None => block,
                Some(limit) => {
                    let n = block.num_rows();
                    if *limit >= n {
                        *limit -= n;
                        block
                    } else {
                        let range = 0..*limit;
                        *limit = 0;
                        block.slice(range)
                    }
                }
            };
            debug_assert!(block.num_rows() > 0);
            self.output.push_data(Ok(block));
            if buffer.is_empty() && cur_state.unwrap().done() {
                self.cur_task += 1;
                cur_state.take();
                self.cur_input = None;
            }
            true
        } else {
            false
        }
    }

    fn find(&mut self) -> Result<PullEvent> {
        let event =
            (0..self.inputs.len()).try_fold(PullEvent::Finished, |e, i| match self.pull(i) {
                Ok(PullEvent::Data) => {
                    self.cur_input = Some(i);
                    ControlFlow::Break(None)
                }
                Ok(PullEvent::Pending) => ControlFlow::Continue(PullEvent::Pending),
                Ok(PullEvent::Finished) => ControlFlow::Continue(e),
                Err(e) => ControlFlow::Break(Some(e)),
            });
        match event {
            ControlFlow::Continue(e) => Ok(e),
            ControlFlow::Break(None) => Ok(PullEvent::Data),
            ControlFlow::Break(Some(e)) => Err(e),
        }
    }
}

impl Processor for KWayMergeCombinerProcessor {
    fn name(&self) -> String {
        "KWayMergeCombiner".to_string()
    }

    fn as_any(&mut self) -> &mut dyn Any {
        self
    }

    fn event(&mut self) -> Result<Event> {
        if self.output.is_finished() || self.limit == Some(0) {
            self.inputs.iter().for_each(|i| i.finish());
            self.output.finish();
            return Ok(Event::Finished);
        }

        if !self.output.can_push() {
            return Ok(Event::NeedConsume);
        }

        if self.cur_input.is_none() {
            match self.find()? {
                PullEvent::Pending => return Ok(Event::NeedData),
                PullEvent::Finished => {
                    self.output.finish();
                    return Ok(Event::Finished);
                }
                PullEvent::Data => (),
            }
        }

        let pushed = self.push();
        match self.cur_input {
            Some(cur) => match (pushed, self.pull(cur)?) {
                (true, PullEvent::Pending | PullEvent::Data) => Ok(Event::NeedConsume),
                (true, PullEvent::Finished) => {
                    self.cur_input = None;
                    Ok(Event::NeedConsume)
                }
                (false, PullEvent::Pending) => Ok(Event::NeedData),
                (false, PullEvent::Data) => {
                    if self.push() {
                        Ok(Event::NeedConsume)
                    } else {
                        unreachable!()
                    }
                }
                (false, PullEvent::Finished) => {
                    todo!("unexpected finish")
                }
            },
            None => match self.find()? {
                PullEvent::Pending | PullEvent::Data => Ok(Event::NeedConsume),
                PullEvent::Finished => {
                    self.output.finish();
                    Ok(Event::NeedConsume)
                }
            },
        }
    }
}

type BlockStream = VecDeque<(DataBlock, Column)>;

impl SortedStream for BlockStream {
    fn next(&mut self) -> Result<(Option<(DataBlock, Column)>, bool)> {
        Ok((self.pop_front(), false))
    }
}

enum PullEvent {
    Data,
    Pending,
    Finished,
}
