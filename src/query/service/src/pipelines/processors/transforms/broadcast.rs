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
use databend_common_catalog::table_context::TableContext;
use databend_common_exception::ErrorCode;
use databend_common_exception::Result;
use databend_common_expression::BlockMetaInfoPtr;
use databend_common_expression::DataBlock;
use databend_common_pipeline::core::InputPort;
use databend_common_pipeline::core::OutputPort;
use databend_common_pipeline::core::ProcessorPtr;
use databend_common_pipeline::sinks::AsyncSink;
use databend_common_pipeline::sinks::AsyncSinker;
use databend_common_pipeline::sources::AsyncSource;
use databend_common_pipeline::sources::AsyncSourcer;

pub struct BroadcastSourceProcessor {
    pub receiver: Receiver<BlockMetaInfoPtr>,
}

impl BroadcastSourceProcessor {
    pub fn create(
        ctx: Arc<dyn TableContext>,
        receiver: Receiver<BlockMetaInfoPtr>,
        output_port: Arc<OutputPort>,
    ) -> Result<ProcessorPtr> {
        AsyncSourcer::create(ctx.get_scan_progress(), output_port, Self { receiver })
    }
}

#[async_trait::async_trait]
impl AsyncSource for BroadcastSourceProcessor {
    const NAME: &'static str = "BroadcastSource";
    const SKIP_EMPTY_DATA_BLOCK: bool = false;

    #[async_backtrace::framed]
    async fn generate(&mut self) -> Result<Option<DataBlock>> {
        let received = self.receiver.recv().await;
        match received {
            Ok(meta) => Ok(Some(DataBlock::empty_with_meta(meta))),
            Err(_) => {
                // The channel is closed, we should return None to stop generating
                Ok(None)
            }
        }
    }
}

pub struct BroadcastSinkProcessor {
    sender: Sender<BlockMetaInfoPtr>,
}

impl BroadcastSinkProcessor {
    pub fn create(input: Arc<InputPort>, sender: Sender<BlockMetaInfoPtr>) -> Result<ProcessorPtr> {
        Ok(ProcessorPtr::create(AsyncSinker::create(input, Self {
            sender,
        })))
    }
}

#[async_trait::async_trait]
impl AsyncSink for BroadcastSinkProcessor {
    const NAME: &'static str = "BroadcastSink";

    async fn on_finish(&mut self) -> Result<()> {
        self.sender.close();
        Ok(())
    }

    async fn consume(&mut self, mut data_block: DataBlock) -> Result<bool> {
        let meta = data_block
            .take_meta()
            .ok_or_else(|| ErrorCode::Internal("Cannot downcast meta to BroadcastMeta"))?;
        self.sender
            .send(meta)
            .await
            .map_err(|_| ErrorCode::Internal("BroadcastSinkProcessor send error"))?;
        Ok(false)
    }
}
