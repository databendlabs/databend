use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::mpsc::{Receiver, RecvError, Sender, SyncSender};
use std::thread::JoinHandle;
use common_datablocks::DataBlock;
use crate::pipelines::new::executor::pipeline_threads_executor::PipelineThreadsExecutor;
use crate::pipelines::new::executor::PipelineExecutor;
use crate::pipelines::new::{NewPipe, NewPipeline};
use common_exception::{ErrorCode, Result};
use common_infallible::Mutex;
use crate::pipelines::new::pipe::SinkPipeBuilder;
use crate::pipelines::new::processors::port::InputPort;
use crate::pipelines::new::processors::processor::ProcessorPtr;
use crate::pipelines::new::processors::{Sink, Sinker};

enum Executor {
    Inited(Arc<PipelineThreadsExecutor>),
    Running(Arc<PipelineThreadsExecutor>),
    Finished(Result<()>),
}

pub struct PipelinePullingExecutor {
    executor: Arc<Mutex<Executor>>,
    receiver: Receiver<DataBlock>,
}

impl PipelinePullingExecutor {
    fn wrap_pipeline(pipeline: &mut NewPipeline) -> Result<Receiver<DataBlock>> {
        if pipeline.pipes.is_empty() {
            return Err(ErrorCode::PipelineUnInitialized(""));
        }

        if pipeline.pipes[0].input_size() != 0 {
            return Err(ErrorCode::PipelineUnInitialized(""));
        }

        if pipeline.output_len() == 0 {
            return Err(ErrorCode::PipelineUnInitialized(""));,
        }

        let (tx, rx) = std::sync::mpsc::sync_channel(2);
        let mut pipe_builder = SinkPipeBuilder::create();

        for _index in 0..pipeline.output_len() {
            let input = InputPort::create();
            let pulling_sink = PullingSink::create(tx.clone(), input.clone());
            pipe_builder.add_sink(input.clone(), pulling_sink);
        }

        pipeline.add_pipe(pipe_builder.finalize());
        Ok(rx)
    }

    pub fn try_create(mut pipeline: NewPipeline) -> Result<PipelinePullingExecutor> {
        let receiver = Self::wrap_pipeline(&mut pipeline)?;
        let pipeline_executor = PipelineThreadsExecutor::create(pipeline)?;
        let executor = Arc::new(Mutex::new(Executor::Inited(pipeline_executor)));
        Ok(PipelinePullingExecutor { receiver, executor })
    }

    pub fn start(&mut self) -> Result<()> {
        let mut executor = self.executor.lock();

        if let Executor::Inited(threads_executor) = &*executor {
            let state = self.executor.clone();
            let threads_executor = threads_executor.clone();
            let thread_function = Self::thread_function(state, threads_executor.clone());

            std::thread::spawn(thread_function);
            *executor = Executor::Running(threads_executor);
        }

        Ok(())
    }

    fn thread_function(state: Arc<Mutex<Executor>>, threads_executor: Arc<PipelineThreadsExecutor>) -> impl Fn() {
        move || {
            let res = threads_executor.execute();
            let mut state = state.lock();
            match res {
                Ok(_) => { *state = Executor::Finished(Ok(())); }
                Err(cause) => { *state = Executor::Finished(Err(cause)); }
            }
        }
    }

    pub fn finish(&mut self) -> Result<()> {
        let mutex_guard = self.executor.lock();
        match &*mutex_guard {
            Executor::Inited(_) => Ok(()),
            Executor::Running(executor) => executor.finish(),
            Executor::Finished(res) => res.clone(),
        }
    }

    pub fn pull_data(&mut self) -> Option<DataBlock> {
        match self.receiver.recv() {
            Err(_recv_err) => None,
            Ok(data_block) => Some(data_block),
        }
    }
}

enum PullingSink {
    Running(SyncSender<DataBlock>),
    Finished,
}

impl PullingSink {
    pub fn create(tx: SyncSender<DataBlock>, input: Arc<InputPort>) -> ProcessorPtr {
        Sinker::create(input, PullingSink::Running(tx))
    }
}

impl Sink for PullingSink {
    const NAME: &'static str = "PullingExecutorSink";

    fn on_finish(&mut self) -> Result<()> {
        if let PullingSink::Running(v) = self {
            *self = PullingSink::Finished;
        }

        Ok(())
    }

    fn consume(&mut self, data_block: DataBlock) -> Result<()> {
        match self {
            PullingSink::Finished => Ok(()),
            PullingSink::Running(tx) => match tx.send(data_block) {
                Ok(_) => Ok(()),
                Err(cause) => Err(ErrorCode::LogicalError(format!("{:?}", cause)))
            }
        }
    }
}
