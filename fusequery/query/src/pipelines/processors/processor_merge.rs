// Copyright 2020-2021 The Datafuse Authors.
//
// SPDX-License-Identifier: Apache-2.0.

use std::any::Any;
use std::sync::Arc;

use common_datablocks::DataBlock;
use common_exception::ErrorCodes;
use common_exception::Result;
use common_streams::SendableDataBlockStream;
use log::error;
use tokio::sync::mpsc;
use tokio_stream::wrappers::ReceiverStream;
use tokio_stream::StreamExt;

use crate::pipelines::processors::IProcessor;
use crate::sessions::FuseQueryContextRef;

pub struct MergeProcessor {
    ctx: FuseQueryContextRef,
    inputs: Vec<Arc<dyn IProcessor>>,
}

impl MergeProcessor {
    pub fn create(ctx: FuseQueryContextRef) -> Self {
        MergeProcessor {
            ctx,
            inputs: vec![],
        }
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
        let inputs = self.inputs.len();
        match inputs {
            0 => Result::Err(ErrorCodes::IllegalTransformConnectionState(
                "Merge processor inputs cannot be zero",
            )),
            1 => self.inputs[0].execute().await,
            _ => {
                let (sender, receiver) = mpsc::channel::<Result<DataBlock>>(inputs);
                for i in 0..inputs {
                    let input = self.inputs[i].clone();
                    let sender = sender.clone();
                    self.ctx.execute_task(async move {
                        let mut stream = match input.execute().await {
                            Err(e) => {
                                if let Err(error) = sender.send(Result::Err(e)).await {
                                    error!("Merge processor cannot push data: {}", error);
                                }
                                return;
                            }
                            Ok(stream) => stream,
                        };

                        while let Some(item) = stream.next().await {
                            match item {
                                Ok(item) => {
                                    if let Err(error) = sender.send(Ok(item)).await {
                                        // Stop pulling data
                                        error!("Merge processor cannot push data: {}", error);
                                        return;
                                    }
                                }
                                Err(error) => {
                                    // Stop pulling data
                                    if let Err(error) = sender.send(Err(error)).await {
                                        error!("Merge processor cannot push data: {}", error);
                                    }
                                    return;
                                }
                            }
                        }
                    });
                }
                Ok(Box::pin(ReceiverStream::new(receiver)))
            }
        }
    }
}
