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
use std::sync::Arc;

use async_channel::Receiver;
use async_channel::Sender;
use databend_common_exception::ErrorCode;
use databend_common_exception::Result;
use databend_common_expression::DataBlock;
use databend_common_pipeline_core::processors::Event;
use databend_common_pipeline_core::processors::InputPort;
use databend_common_pipeline_core::processors::OutputPort;
use databend_common_pipeline_core::processors::Processor;
use databend_common_pipeline_core::processors::ProcessorPtr;
use databend_common_pipeline_sources::AsyncSource;
use databend_common_pipeline_sources::AsyncSourcer;
use databend_common_storages_fuse::TableContext;

pub struct MaterializedCteSink {
    input: Arc<InputPort>,
    senders: Vec<Sender<DataBlock>>,
}

impl MaterializedCteSink {
    pub fn create(input: Arc<InputPort>, senders: Vec<Sender<DataBlock>>) -> Result<ProcessorPtr> {
        Ok(ProcessorPtr::create(Box::new(Self { input, senders })))
    }

    fn should_apply_backpressure(&self) -> bool {
        self.senders.iter().all(|sender| !sender.is_empty())
    }
}

#[async_trait::async_trait]
impl Processor for MaterializedCteSink {
    fn name(&self) -> String {
        "MaterializedCteSink".to_string()
    }

    fn as_any(&mut self) -> &mut dyn Any {
        self
    }

    fn event(&mut self) -> Result<Event> {
        if self.input.is_finished() {
            for sender in self.senders.iter() {
                sender.close();
            }
            self.input.finish();
            return Ok(Event::Finished);
        }

        if self.input.has_data() {
            if self.should_apply_backpressure() {
                return Ok(Event::NeedConsume);
            }
            return Ok(Event::Async);
        }

        self.input.set_need_data();
        Ok(Event::NeedData)
    }

    async fn async_process(&mut self) -> Result<()> {
        let data_block = self.input.pull_data().ok_or_else(|| {
            ErrorCode::Internal("Failed to pull data from input port in materialized cte sink")
        })??;
        for sender in self.senders.iter() {
            sender.send(data_block.clone()).await.map_err(|_| {
                ErrorCode::Internal("Failed to send blocks to materialized cte consumer")
            })?;
        }
        Ok(())
    }
}

pub struct CTESource {
    receiver: Receiver<DataBlock>,
}

impl CTESource {
    pub fn create(
        ctx: Arc<dyn TableContext>,
        output_port: Arc<OutputPort>,
        receiver: Receiver<DataBlock>,
    ) -> Result<ProcessorPtr> {
        AsyncSourcer::create(ctx, output_port, Self { receiver })
    }
}

#[async_trait::async_trait]
impl AsyncSource for CTESource {
    const NAME: &'static str = "MaterializeCTESource";

    #[async_backtrace::framed]
    async fn generate(&mut self) -> Result<Option<DataBlock>> {
        if let Ok(data) = self.receiver.recv().await {
            return Ok(Some(data));
        }
        Ok(None)
    }
}
