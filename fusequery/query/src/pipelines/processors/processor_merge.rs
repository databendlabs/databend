// Copyright 2020-2021 The Datafuse Authors.
//
// SPDX-License-Identifier: Apache-2.0.

use std::any::Any;
use std::pin::Pin;
use std::sync::Arc;
use std::task::Context;
use std::task::Poll;

use common_datablocks::DataBlock;
use common_exception::ErrorCodes;
use common_exception::Result;
use common_streams::SendableDataBlockStream;
use futures::Stream;
use tokio::runtime::Runtime;
use tokio::sync::mpsc;
use tokio::sync::mpsc::Receiver;
use tokio_stream::StreamExt;

use crate::pipelines::processors::IProcessor;

pub struct MergeProcessor {
    inputs: Vec<Arc<dyn IProcessor>>
}

impl MergeProcessor {
    pub fn create() -> Self {
        MergeProcessor { inputs: vec![] }
    }
}

#[async_trait::async_trait]
impl IProcessor for MergeProcessor {
    fn name(&self) -> &str {
        "MergeProcessor"
    }

    fn connect_to(&mut self, input: Arc<dyn IProcessor>) -> Result<()> {
        self.inputs.push(input);
        Ok(())
    }

    fn inputs(&self) -> Vec<Arc<dyn IProcessor>> {
        self.inputs.clone()
    }

    fn as_any(&self) -> &dyn Any {
        self
    }

    async fn execute(&self) -> Result<SendableDataBlockStream> {
        match self.inputs.len() {
            0 => Result::Err(ErrorCodes::IllegalTransformConnectionState(
                "Merge processor inputs cannot be zero".to_string()
            )),
            1 => self.inputs[0].execute().await,
            _ => MergeProcessorStream::try_create(self.inputs.clone())
        }
    }
}

#[derive(Debug)]
pub struct MergeProcessorStream {
    inner: Receiver<Result<DataBlock>>,
    handler: Option<std::thread::JoinHandle<Vec<ErrorCodes>>>
}

impl MergeProcessorStream {
    /// Create a new `MergeProcessorStream`.
    pub fn try_create(inputs: Vec<Arc<dyn IProcessor>>) -> Result<SendableDataBlockStream> {
        let (sender, receiver) = mpsc::channel(inputs.len());

        Ok(Box::pin(Self {
            inner: receiver,
            handler: Some(std::thread::spawn(move || {
                fn build_runtime(max_threads: usize) -> Result<Runtime> {
                    tokio::runtime::Builder::new_multi_thread()
                        .enable_io()
                        .enable_time()
                        .worker_threads(max_threads)
                        .build()
                        .map_err(|tokio_error| ErrorCodes::TokioError(format!("{}", tokio_error)))
                }

                match build_runtime(inputs.len()) {
                    Err(e) => vec![e],
                    Ok(runtime) => {
                        let handlers = (0..inputs.len())
                            .map(|index| {
                                let input = inputs[index].clone();
                                let sender = sender.clone();

                                runtime.spawn(async move {
                                    let mut stream = match input.execute().await {
                                        Err(code) => {
                                            sender.send(Result::Err(code)).await.ok();
                                            return;
                                        }
                                        Ok(stream) => stream
                                    };

                                    while let Some(item) = stream.next().await {
                                        sender.send(item).await.ok();
                                    }
                                })
                            })
                            .collect::<Vec<_>>();

                        let mut errors: Vec<ErrorCodes> = vec![];
                        for handler in handlers {
                            match futures::executor::block_on(handler) {
                                Ok(_) => {}
                                Err(error) => errors.push(ErrorCodes::TokioError(error.to_string()))
                            };
                        }

                        errors
                    }
                }
            }))
        }))
    }
}

impl Stream for MergeProcessorStream {
    type Item = Result<DataBlock>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        // startup processor
        self.inner.poll_recv(cx)
    }
}

impl Drop for MergeProcessorStream {
    fn drop(&mut self) {
        match self.handler.take().unwrap().join() {
            Err(error) => panic!(
                "Cannot join thread in drop MergeProcessorStream: {:?}",
                error
            ),
            Ok(errors) => {
                if !errors.is_empty() {
                    panic!(
                        "Cannot join thread in drop MergeProcessorStream: {:?}",
                        errors
                    )
                }
            }
        }
    }
}

impl AsRef<Receiver<Result<DataBlock>>> for MergeProcessorStream {
    fn as_ref(&self) -> &Receiver<Result<DataBlock>> {
        &self.inner
    }
}

impl AsMut<Receiver<Result<DataBlock>>> for MergeProcessorStream {
    fn as_mut(&mut self) -> &mut Receiver<Result<DataBlock>> {
        &mut self.inner
    }
}
