use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::mpsc::{Receiver, RecvError, Sender, SyncSender};
use common_datablocks::DataBlock;
use crate::pipelines::new::executor::pipeline_threads_executor::PipelineThreadsExecutor;
use crate::pipelines::new::executor::PipelineExecutor;
use crate::pipelines::new::{NewPipe, NewPipeline};
use common_exception::{ErrorCode, Result};
use crate::pipelines::new::pipe::SinkPipeBuilder;
use crate::pipelines::new::processors::port::InputPort;
use crate::pipelines::new::processors::processor::ProcessorPtr;
use crate::pipelines::new::processors::{Sink, Sinker};

pub struct PipelinePullingExecutor {
    finish: bool,
    initialized: bool,
    base: PipelineThreadsExecutor,
    receiver: Receiver<Option<DataBlock>>,
}

impl PipelinePullingExecutor {
    fn wrap_pipeline(pipeline: &mut NewPipeline) -> Result<Receiver<Option<DataBlock>>> {
        if pipeline.pipes.is_empty() {
            return Err(ErrorCode::PipelineUnInitialized(""));
        }

        if pipeline.pipes[0].input_size() != 0 {
            return Err(ErrorCode::PipelineUnInitialized(""));
        }

        pipeline.resize(1)?;
        let input = InputPort::create();
        let (tx, rx) = std::sync::mpsc::sync_channel(2);
        let pulling_sink = PullingSink::create(tx, input.clone());
        pipeline.add_pipe(NewPipe::SimplePipe {
            processors: vec![pulling_sink],
            inputs_port: vec![input],
            outputs_port: vec![],
        });

        Ok(rx)
    }

    pub fn try_create(mut pipeline: NewPipeline) -> Result<PipelinePullingExecutor> {
        let receiver = Self::wrap_pipeline(&mut pipeline)?;
        Ok(PipelinePullingExecutor {
            receiver,
            finish: false,
            initialized: false,
            base: PipelineThreadsExecutor::create(pipeline)?,
        })
    }

    pub fn finish(&mut self) -> Result<()> {
        match self.finish {
            true => Ok(()),
            false => {
                self.finish = true;
                self.base.finish()
            }
        }
    }

    pub fn pull_data(&mut self) -> Result<Option<DataBlock>> {
        if !self.initialized {
            self.initialized = true;
            // self.base.execute()
        }

        if self.finish {
            return Ok(None);
        }

        match self.receiver.recv() {
            Ok(Some(data_block)) => Ok(Some(data_block)),
            Err(cause) => Err(ErrorCode::LogicalError(format!("{:?}", cause))),
            Ok(None) => {
                self.finish = true;
                Ok(None)
            }
        }
    }
}

struct PullingSink {
    tx: SyncSender<Option<DataBlock>>,
}

impl PullingSink {
    pub fn create(tx: SyncSender<Option<DataBlock>>, input: Arc<InputPort>) -> ProcessorPtr {
        Sinker::create(input, PullingSink { tx })
    }
}

impl Sink for PullingSink {
    const NAME: &'static str = "PullingExecutorSink";

    fn on_finish(&mut self) -> Result<()> {
        match self.tx.send(None) {
            Ok(_) => Ok(()),
            Err(cause) => Err(ErrorCode::LogicalError(format!("{:?}", cause)))
        }
    }

    fn consume(&mut self, data_block: DataBlock) -> Result<()> {
        match self.tx.send(Some(data_block)) {
            Ok(_) => Ok(()),
            Err(cause) => Err(ErrorCode::LogicalError(format!("{:?}", cause)))
        }
    }
}
