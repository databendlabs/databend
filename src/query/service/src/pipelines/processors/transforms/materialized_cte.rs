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

use std::sync::Arc;

use async_channel::Receiver;
use async_channel::Sender;
use databend_common_exception::ErrorCode;
use databend_common_exception::Result;
use databend_common_expression::DataBlock;
use databend_common_pipeline::core::InputPort;
use databend_common_pipeline::core::OutputPort;
use databend_common_pipeline::core::ProcessorPtr;
use databend_common_pipeline::sinks::AsyncSink;
use databend_common_pipeline::sinks::AsyncSinker;
use databend_common_pipeline::sources::AsyncSource;
use databend_common_pipeline::sources::AsyncSourcer;
use databend_common_storages_fuse::TableContext;

pub struct MaterializedCteSink {
    senders: Vec<Sender<DataBlock>>,
}

impl MaterializedCteSink {
    pub fn create(input: Arc<InputPort>, senders: Vec<Sender<DataBlock>>) -> Result<ProcessorPtr> {
        Ok(ProcessorPtr::create(AsyncSinker::create(input, Self {
            senders,
        })))
    }
}

#[async_trait::async_trait]
impl AsyncSink for MaterializedCteSink {
    const NAME: &'static str = "MaterializedCteSink";

    async fn consume(&mut self, data_block: DataBlock) -> Result<bool> {
        for sender in self.senders.iter() {
            sender.send(data_block.clone()).await.map_err(|_| {
                ErrorCode::Internal("Failed to send blocks to materialized cte consumer")
            })?;
        }
        Ok(false)
    }

    async fn on_finish(&mut self) -> Result<()> {
        for sender in self.senders.iter() {
            sender.close();
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
        AsyncSourcer::create(ctx.get_scan_progress(), output_port, Self { receiver })
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
