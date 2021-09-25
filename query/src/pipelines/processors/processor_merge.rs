// Copyright 2020 Datafuse Labs.
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
use std::sync::Arc;

use common_datablocks::DataBlock;
use common_exception::ErrorCode;
use common_exception::Result;
use common_base::tokio::sync::mpsc;
use common_streams::SendableDataBlockStream;
use log::error;
use tokio_stream::wrappers::ReceiverStream;
use tokio_stream::StreamExt;

use crate::pipelines::processors::Processor;
use crate::sessions::DatabendQueryContextRef;

pub struct MergeProcessor {
    ctx: DatabendQueryContextRef,
    inputs: Vec<Arc<dyn Processor>>,
}

impl MergeProcessor {
    pub fn create(ctx: DatabendQueryContextRef) -> Self {
        MergeProcessor {
            ctx,
            inputs: vec![],
        }
    }
}

#[async_trait::async_trait]
impl Processor for MergeProcessor {
    fn name(&self) -> &str {
        "MergeProcessor"
    }

    fn connect_to(&mut self, input: Arc<dyn Processor>) -> Result<()> {
        self.inputs.push(input);
        Ok(())
    }

    fn inputs(&self) -> Vec<Arc<dyn Processor>> {
        self.inputs.clone()
    }

    fn as_any(&self) -> &dyn Any {
        self
    }

    async fn execute(&self) -> Result<SendableDataBlockStream> {
        let inputs = self.inputs.len();
        match inputs {
            0 => Result::Err(ErrorCode::IllegalTransformConnectionState(
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
                    })?;
                }
                Ok(Box::pin(ReceiverStream::new(receiver)))
            }
        }
    }
}
