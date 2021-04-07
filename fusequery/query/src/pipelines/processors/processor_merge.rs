// Copyright 2020-2021 The Datafuse Authors.
//
// SPDX-License-Identifier: Apache-2.0.

use std::any::Any;
use std::sync::Arc;

use anyhow::{bail, Result};
use common_datablocks::DataBlock;
use common_streams::SendableDataBlockStream;
use futures::stream::StreamExt;
use tokio::sync::mpsc;
use tokio_stream::wrappers::ReceiverStream;

use crate::pipelines::processors::IProcessor;

pub struct MergeProcessor {
    inputs: Vec<Arc<dyn IProcessor>>,
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
        let inputs = self.inputs.len();
        match inputs {
            0 => bail!("Merge processor inputs cannot be zero"),
            1 => self.inputs[0].execute().await,
            _ => {
                let (sender, receiver) = mpsc::channel::<Result<DataBlock>>(inputs);
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
                Ok(Box::pin(ReceiverStream::new(receiver)))
            }
        }
    }
}
