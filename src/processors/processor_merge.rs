// Copyright 2020-2021 The FuseQuery Authors.
//
// SPDX-License-Identifier: Apache-2.0.

use std::any::Any;
use std::sync::Arc;

use async_trait::async_trait;
use futures::stream::StreamExt;
use tokio::sync::mpsc;

use crate::datablocks::DataBlock;
use crate::datastreams::SendableDataBlockStream;
use crate::error::{FuseQueryError, FuseQueryResult};
use crate::processors::IProcessor;

pub struct MergeProcessor {
    inputs: Vec<Arc<dyn IProcessor>>,
}

impl MergeProcessor {
    pub fn create() -> Self {
        MergeProcessor { inputs: vec![] }
    }
}

#[async_trait]
impl IProcessor for MergeProcessor {
    fn name(&self) -> &str {
        "MergeProcessor"
    }

    fn connect_to(&mut self, input: Arc<dyn IProcessor>) -> FuseQueryResult<()> {
        self.inputs.push(input);
        Ok(())
    }

    fn inputs(&self) -> Vec<Arc<dyn IProcessor>> {
        self.inputs.clone()
    }

    fn as_any(&self) -> &dyn Any {
        self
    }

    async fn execute(&self) -> FuseQueryResult<SendableDataBlockStream> {
        let inputs = self.inputs.len();
        match inputs {
            0 => Err(FuseQueryError::Internal(
                "Merge processor inputs cannot be zero".to_string(),
            )),
            1 => self.inputs[0].execute().await,
            _ => {
                let (sender, receiver) = mpsc::channel::<FuseQueryResult<DataBlock>>(inputs);
                for i in 0..inputs {
                    let input = self.inputs[i].clone();
                    let sender = sender.clone();
                    tokio::spawn(async move {
                        let mut stream = match input.execute().await {
                            Err(e) => {
                                sender.send(Err(e)).await.ok();
                                return;
                            }
                            Ok(stream) => stream,
                        };

                        while let Some(item) = stream.next().await {
                            sender.send(item).await.ok();
                        }
                    });
                }
                Ok(Box::pin(tokio_stream::wrappers::ReceiverStream::new(
                    receiver,
                )))
            }
        }
    }
}
