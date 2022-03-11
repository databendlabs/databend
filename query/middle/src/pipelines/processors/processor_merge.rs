// Copyright 2021 Datafuse Labs.
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

use common_base::tokio::sync::mpsc;
use common_base::TrySpawn;
use common_datablocks::DataBlock;
use common_exception::ErrorCode;
use common_exception::Result;
use common_streams::SendableDataBlockStream;
use common_tracing::tracing;
use common_tracing::tracing::Instrument;
use tokio_stream::wrappers::ReceiverStream;
use tokio_stream::StreamExt;

use crate::pipelines::processors::Processor;
use crate::sessions::QueryContext;

pub struct MergeProcessor {
    ctx: Arc<QueryContext>,
    inputs: Vec<Arc<dyn Processor>>,
}

impl MergeProcessor {
    pub fn create(ctx: Arc<QueryContext>) -> Self {
        MergeProcessor {
            ctx,
            inputs: vec![],
        }
    }

    #[tracing::instrument(level = "debug", skip(self), fields(ctx.id = self.ctx.get_id().as_str()))]
    pub fn merge(&self) -> Result<SendableDataBlockStream> {
        let len = self.inputs.len();
        if len == 0 {
            return Result::Err(ErrorCode::IllegalTransformConnectionState(
                "Merge processor inputs cannot be zero",
            ));
        }

        let (sender, receiver) = mpsc::channel::<Result<DataBlock>>(len);
        for i in 0..len {
            let processor = self.inputs[i].clone();
            let sender = sender.clone();
            self.ctx.try_spawn(
                async move {
                    let mut stream = match processor.execute().await {
                        Err(e) => {
                            if let Err(error) = sender.send(Result::Err(e)).await {
                                tracing::error!("Merge processor cannot push data: {}", error);
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
                                    tracing::error!("Merge processor cannot push data: {}", error);
                                    return;
                                }
                            }
                            Err(error) => {
                                // Stop pulling data
                                if let Err(error) = sender.send(Err(error)).await {
                                    tracing::error!("Merge processor cannot push data: {}", error);
                                }
                                return;
                            }
                        }
                    }
                }
                .instrument(tracing::debug_span!("merge_input").or_current()),
            )?;
        }
        Ok(Box::pin(ReceiverStream::new(receiver)))
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

    #[tracing::instrument(level = "debug", name="merge_processor_execute", skip(self), fields(ctx.id = self.ctx.get_id().as_str()))]
    async fn execute(&self) -> Result<SendableDataBlockStream> {
        match self.inputs.len() {
            1 => self.inputs[0].execute().await,
            _ => self.merge(),
        }
    }
}
