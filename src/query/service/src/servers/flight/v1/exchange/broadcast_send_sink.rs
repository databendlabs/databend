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

use databend_common_exception::Result;
use databend_common_expression::DataBlock;
use databend_common_pipeline::core::InputPort;
use databend_common_pipeline::core::PipeItem;
use databend_common_pipeline::core::Processor;
use databend_common_pipeline::core::ProcessorPtr;
use databend_common_pipeline::sinks::AsyncSink;
use databend_common_pipeline::sinks::AsyncSinker;

use crate::servers::flight::v1::network::OutboundChannel;

pub struct BroadcastExchangeSink {
    channels: Vec<Arc<dyn OutboundChannel>>,
}

impl BroadcastExchangeSink {
    pub fn create(
        input: Arc<InputPort>,
        channels: Vec<Arc<dyn OutboundChannel>>,
    ) -> Box<dyn Processor> {
        AsyncSinker::create(input, BroadcastExchangeSink { channels })
    }
}

#[async_trait::async_trait]
impl AsyncSink for BroadcastExchangeSink {
    const NAME: &'static str = "BroadcastExchangeSink";

    #[async_backtrace::framed]
    async fn on_finish(&mut self) -> Result<()> {
        for channel in &self.channels {
            channel.close();
        }
        Ok(())
    }

    #[async_backtrace::framed]
    async fn consume(&mut self, data_block: DataBlock) -> Result<bool> {
        let mut futures = Vec::with_capacity(self.channels.len());
        for channel in &self.channels {
            let channel = channel.clone();
            let data_block = data_block.clone();
            futures.push(async move { channel.add_block(data_block).await });
        }

        futures::future::try_join_all(futures).await?;
        Ok(false)
    }
}

pub fn create_broadcast_sink_item(channels: Vec<Arc<dyn OutboundChannel>>) -> PipeItem {
    let input = InputPort::create();
    PipeItem::create(
        ProcessorPtr::create(BroadcastExchangeSink::create(input.clone(), channels)),
        vec![input],
        vec![],
    )
}
