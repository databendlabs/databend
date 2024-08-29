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
use databend_common_expression::types::binary::BinaryColumn;
use databend_common_expression::types::*;
use databend_common_expression::with_number_mapped_type;
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
use databend_common_pipeline_core::Pipeline;
use match_template::match_template;

use super::sort::algorithm::HeapSort;
use super::sort::algorithm::LoserTreeSort;
use super::sort::algorithm::SortAlgorithm;
use super::sort::utils::u32_entry;
use super::sort::KWaySortPartitioner;
use super::sort::Merger;
use super::sort::Rows;
use super::sort::SimpleRows;
use super::sort::SortedStream;
use super::transform_multi_sort_merge::InputBlockStream;

pub fn add_k_way_merge_sort(
    pipeline: &mut Pipeline,
    schema: DataSchemaRef,
    worker: usize,
    block_size: usize,
    limit: Option<usize>,
    sort_desc: Arc<Vec<SortColumnDescription>>,
    remove_order_col: bool,
    enable_loser_tree: bool,
) -> Result<()> {
    if pipeline.is_empty() {
        return Err(ErrorCode::Internal("Cannot resize empty pipe."));
    }

    match pipeline.output_len() {
        0 => Err(ErrorCode::Internal("Cannot resize empty pipe.")),
        1 => Ok(()),
        pipe_size => {
            struct Args {
                schema: DataSchemaRef,
                stream_size: usize,
                worker: usize,
                sort_desc: Arc<Vec<SortColumnDescription>>,
                block_size: usize,
                limit: Option<usize>,
                remove_order_col: bool,
                enable_loser_tree: bool,
            }

            fn add<R>(args: Args, pipeline: &mut Pipeline) -> Result<()>
            where R: Rows + Send + 'static {
                if args.enable_loser_tree {
                    let b = Builder::<LoserTreeSort<R>> {
                        schema: args.schema,
                        stream_size: args.stream_size,
                        worker: args.worker,
                        sort_desc: args.sort_desc,
                        block_size: args.block_size,
                        limit: args.limit,
                        remove_order_col: args.remove_order_col,
                        _a: Default::default(),
                    };
                    b.build(pipeline)
                } else {
                    let b = Builder::<HeapSort<R>> {
                        schema: args.schema,
                        stream_size: args.stream_size,
                        worker: args.worker,
                        sort_desc: args.sort_desc,
                        block_size: args.block_size,
                        limit: args.limit,
                        remove_order_col: args.remove_order_col,
                        _a: Default::default(),
                    };
                    b.build(pipeline)
                }
            }

            let args = Args {
                schema,
                stream_size: pipe_size,
                worker,
                sort_desc,
                block_size,
                limit,
                remove_order_col,
                enable_loser_tree,
            };

            if args.sort_desc.len() == 1 {
                let sort_type = args.schema.field(args.sort_desc[0].offset).data_type();
                match_template! {
                    T = [ Date => DateType, Timestamp => TimestampType, String => StringType ],
                    match sort_type {
                        DataType::T => add::<SimpleRows<T>>(args, pipeline),
                        DataType::Number(num_ty) => with_number_mapped_type!(|NUM_TYPE| match num_ty {
                            NumberDataType::NUM_TYPE =>
                                add::<SimpleRows<NumberType<NUM_TYPE>>>(args, pipeline),
                        }),
                        _ => add::<BinaryColumn>(args, pipeline),
                    }
                }
            } else {
                add::<BinaryColumn>(args, pipeline)
            }
        }
    }
}

struct Builder<A>
where A: SortAlgorithm
{
    schema: DataSchemaRef,
    stream_size: usize,
    worker: usize,
    sort_desc: Arc<Vec<SortColumnDescription>>,
    block_size: usize,
    limit: Option<usize>,
    remove_order_col: bool,
    _a: PhantomData<A>,
}

impl<A> Builder<A>
where
    A: SortAlgorithm + Send + 'static,
    <A as SortAlgorithm>::Rows: Send + 'static,
{
    fn create_partitioner(&self, input: usize) -> Pipe {
        create_partitioner_pipe::<A::Rows>(
            (0..input).map(|_| InputPort::create()).collect(),
            self.worker,
            self.schema.clone(),
            self.sort_desc.clone(),
            self.block_size,
            self.limit,
        )
    }

    fn create_worker(
        &self,
        input: Arc<InputPort>,
        stream_size: usize,
        output: Arc<OutputPort>,
        batch_rows: usize,
    ) -> KWayMergeWorkerProcessor<A> {
        KWayMergeWorkerProcessor::<A>::new(
            input,
            stream_size,
            output,
            self.schema.clone(),
            batch_rows,
            self.sort_desc.clone(),
            self.remove_order_col,
        )
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

    fn build(&self, pipeline: &mut Pipeline) -> Result<()> {
        pipeline.add_pipe(self.create_partitioner(self.stream_size));

        pipeline.add_transform(|input, output| {
            Ok(ProcessorPtr::create(Box::new(self.create_worker(
                input,
                self.stream_size,
                output,
                self.block_size,
            ))))
        })?;

        pipeline.add_pipe(self.create_combiner());
        Ok(())
    }
}

pub fn create_partitioner_pipe<R>(
    inputs_port: Vec<Arc<InputPort>>,
    worker: usize,
    schema: DataSchemaRef,
    sort_desc: Arc<Vec<SortColumnDescription>>,
    batch_rows: usize,
    limit: Option<usize>,
) -> Pipe
where
    R: Rows + Send + 'static,
{
    let outputs_port: Vec<_> = (0..worker).map(|_| OutputPort::create()).collect();
    let processor = ProcessorPtr::create(Box::new(KWayMergePartitionerProcessor::<R>::new(
        inputs_port.clone(),
        outputs_port.clone(),
        schema,
        sort_desc,
        batch_rows,
        limit,
    )));

    Pipe::create(inputs_port.len(), worker, vec![PipeItem::create(
        processor,
        inputs_port,
        outputs_port,
    )])
}

pub struct KWayMergePartitionerProcessor<R: Rows> {
    partitioner: KWaySortPartitioner<R, InputBlockStream>,
    inputs: Vec<Arc<InputPort>>,
    outputs: Vec<Arc<OutputPort>>,

    task: VecDeque<DataBlock>,
    cur_output: Option<usize>,
    next: usize,
}

impl<R: Rows> KWayMergePartitionerProcessor<R> {
    pub fn new(
        inputs: Vec<Arc<InputPort>>,
        outputs: Vec<Arc<OutputPort>>,
        schema: DataSchemaRef,
        sort_desc: Arc<Vec<SortColumnDescription>>,
        batch_rows: usize,
        limit: Option<usize>,
    ) -> Self {
        let streams = inputs
            .iter()
            .map(|i| InputBlockStream::new(i.clone(), false))
            .collect::<Vec<_>>();

        Self {
            partitioner: KWaySortPartitioner::new(schema, streams, sort_desc, batch_rows, limit),
            inputs,
            outputs,
            task: VecDeque::new(),
            cur_output: None,
            next: 0,
        }
    }

    fn find_output(&mut self) -> Option<usize> {
        let n = self.outputs.len();
        for mut i in self.next..self.next + n {
            if i >= n {
                i -= n;
            }
            if self.outputs[i].can_push() {
                self.cur_output = Some(i);
                self.next = if i + 1 == n { 0 } else { i + 1 };
                return self.cur_output;
            }
        }
        None
    }
}

impl<R> Processor for KWayMergePartitionerProcessor<R>
where R: Rows + Send + 'static
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
    schema: DataSchemaRef,
    batch_rows: usize,
    sort_desc: Arc<Vec<SortColumnDescription>>,
    remove_order_col: bool,

    buffer: Vec<DataBlock>,
    info: Option<Info>,
    output_data: VecDeque<DataBlock>,
    _a: PhantomData<A>,
}

impl<A> KWayMergeWorkerProcessor<A>
where A: SortAlgorithm
{
    const INPUT_ID_POS: usize = 1;
    const TASK_ROWS_POS: usize = 2;
    const TASK_POS: usize = 3;
    const SORT_COL_POS: usize = 4;

    pub fn new(
        input: Arc<InputPort>,
        stream_size: usize,
        output: Arc<OutputPort>,
        schema: DataSchemaRef,
        batch_rows: usize,
        sort_desc: Arc<Vec<SortColumnDescription>>,
        remove_order_col: bool,
    ) -> Self {
        Self {
            input,
            output,
            output_data: VecDeque::new(),
            stream_size,
            schema,
            batch_rows,
            sort_desc,
            remove_order_col,
            buffer: Vec::new(),
            info: None,
            _a: Default::default(),
        }
    }

    fn ready(&self) -> bool {
        self.info.map_or(false, |info| info.done())
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

        let task = get_u32(&block, Self::TASK_POS);
        let task_rows = get_u32(&block, Self::TASK_ROWS_POS);

        let rows = block.num_rows();
        match &mut self.info {
            Some(info) => {
                debug_assert_eq!(task, info.task);
                debug_assert_eq!(task_rows as usize, info.total);
                assert!(info.remain >= rows);
                info.remain -= rows;

                if info.done() {
                    self.buffer.push(block);
                    Ok(Event::Sync)
                } else {
                    self.buffer.push(block);
                    Ok(Event::NeedData)
                }
            }
            None if task_rows as usize == rows => {
                let n = block.num_columns();
                let task_id = block.get_by_offset(n - Self::TASK_POS).clone();
                let task_rows = block.get_by_offset(n - Self::TASK_ROWS_POS).clone();

                if self.remove_order_col {
                    block.pop_columns(Self::SORT_COL_POS);
                } else {
                    block.pop_columns(Self::TASK_POS);
                }

                block.add_column(task_id);
                block.add_column(task_rows);

                self.output_data.push_back(block);
                Ok(Event::NeedConsume)
            }
            None => {
                let total = task_rows as usize;
                assert!(total >= rows);
                self.info = Some(Info {
                    task,
                    total,
                    remain: total - rows,
                });
                self.buffer.push(block);
                Ok(Event::NeedData)
            }
        }
    }

    fn streams(&mut self) -> Vec<BlockStream> {
        let mut streams = vec![VecDeque::new(); self.stream_size];

        for mut block in self.buffer.drain(..) {
            let n = block.num_columns();
            let id = get_u32(&block, Self::INPUT_ID_POS) as usize;
            let sort_col = block
                .get_by_offset(n - Self::SORT_COL_POS)
                .value
                .as_column()
                .unwrap()
                .clone();

            if self.remove_order_col {
                block.pop_columns(Self::SORT_COL_POS);
            } else {
                block.pop_columns(Self::TASK_POS);
            }

            streams[id].push_back((block, sort_col));
        }
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
        let info = self.info.take().unwrap();
        let mut merger = Merger::<A, BlockStream>::create(
            self.schema.clone(),
            self.streams(),
            self.sort_desc.clone(),
            self.batch_rows,
            None,
        );
        let task_id = u32_entry(info.task);
        let task_rows = u32_entry(info.total as u32);
        let mut rows = 0;
        while let Some(mut block) = merger.next_block()? {
            block.add_column(task_id.clone());
            block.add_column(task_rows.clone());
            rows += block.num_rows();
            self.output_data.push_back(block);
        }
        debug_assert_eq!(rows, info.total);
        debug_assert!(merger.is_finished());
        Ok(())
    }
}

pub struct KWayMergeCombinerProcessor {
    inputs: Vec<Arc<InputPort>>,
    output: Arc<OutputPort>,

    cur_input: Option<usize>,
    cur_task: u32,
    buffer: Vec<VecDeque<DataBlock>>,
    info: Vec<Option<Info>>,
    limit: Option<usize>,
}

#[derive(Debug, Clone, Copy)]
struct Info {
    task: u32,
    total: usize,
    remain: usize,
}

impl Info {
    fn done(&self) -> bool {
        self.remain == 0
    }
}

impl KWayMergeCombinerProcessor {
    pub fn new(inputs: Vec<Arc<InputPort>>, output: Arc<OutputPort>, limit: Option<usize>) -> Self {
        let buffer = vec![VecDeque::new(); inputs.len()];
        let info = vec![None; inputs.len()];
        Self {
            inputs,
            output,
            cur_input: None,
            cur_task: 1,
            buffer,
            info,
            limit,
        }
    }

    fn pull(&mut self, i: usize) -> Result<PullEvent> {
        let input = &self.inputs[i];
        let buffer = &mut self.buffer[i];
        let info = &mut self.info[i];

        const TASK_POS: usize = 2;
        const TASK_ROWS_POS: usize = 1;

        if let Some(info) = info {
            if info.done() {
                return if info.task == self.cur_task {
                    Ok(PullEvent::Data)
                } else {
                    Ok(PullEvent::Pending)
                };
            }
        }

        if input.has_data() {
            let mut block = input.pull_data().unwrap()?;
            input.set_need_data();

            let task = match info {
                None => {
                    let task = get_u32(&block, TASK_POS);
                    let task_rows = get_u32(&block, TASK_ROWS_POS);
                    debug_assert!(task_rows > 0);
                    debug_assert!(
                        task >= self.cur_task,
                        "disorder task. cur_task: {} task: {task} task_rows: {task_rows}",
                        self.cur_task
                    );
                    let remain = task_rows as usize - block.num_rows();
                    let _ = info.insert(Info {
                        task,
                        total: task_rows as usize,
                        remain,
                    });
                    block.pop_columns(2);
                    buffer.push_back(block);
                    task
                }
                Some(info) => {
                    debug_assert_eq!(info.task, get_u32(&block, TASK_POS));
                    debug_assert_eq!(info.total, get_u32(&block, TASK_ROWS_POS) as usize);

                    let rows = block.num_rows();
                    if rows <= info.remain {
                        block.pop_columns(2);
                        buffer.push_back(block);
                        info.remain -= rows;
                        info.task
                    } else {
                        unreachable!()
                    }
                }
            };
            return if task == self.cur_task {
                Ok(PullEvent::Data)
            } else {
                Ok(PullEvent::Pending)
            };
        }

        if !buffer.is_empty() {
            return if info.unwrap().task == self.cur_task {
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
        let info = &mut self.info[cur_input];
        debug_assert_eq!(info.unwrap().task, self.cur_task);
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
            if buffer.is_empty() && info.unwrap().done() {
                self.cur_task += 1;
                info.take();
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
        if self.output.is_finished() || self.limit.map_or(false, |limit| limit == 0) {
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

fn get_u32(block: &DataBlock, i: usize) -> u32 {
    let n = block.num_columns();
    unwrap_u32(block.get_by_offset(n - i))
}

fn unwrap_u32(entry: &BlockEntry) -> u32 {
    match &entry.value {
        Value::Scalar(scalar) => *scalar.as_number().unwrap().as_u_int32().unwrap(),
        Value::Column(column) => column.as_number().unwrap().as_u_int32().unwrap()[0],
    }
}

enum PullEvent {
    Data,
    Pending,
    Finished,
}
