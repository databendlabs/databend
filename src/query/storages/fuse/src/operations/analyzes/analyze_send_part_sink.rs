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
use databend_common_catalog::plan::PartInfoPtr;
use databend_common_catalog::table_context::TableContext;
use databend_common_exception::ErrorCode;
use databend_common_exception::Result;
use databend_common_expression::BlockMetaInfoDowncast;
use databend_common_expression::DataBlock;
use databend_common_pipeline_core::processors::InputPort;
use databend_common_pipeline_core::processors::OutputPort;
use databend_common_pipeline_core::processors::ProcessorPtr;
use databend_common_pipeline_sinks::AsyncSink;
use databend_common_pipeline_sinks::AsyncSinker;
use databend_common_pipeline_sources::AsyncSource;
use databend_common_pipeline_sources::AsyncSourcer;

use crate::operations::analyzes::analyze_meta::AnalyzeNDVMeta;

pub struct AnalyzeSendPartSink {
    sender: Option<Sender<Result<PartInfoPtr>>>,
}

impl AnalyzeSendPartSink {
    pub fn create(
        input: Arc<InputPort>,
        sender: Sender<Result<PartInfoPtr>>,
    ) -> Result<ProcessorPtr> {
        Ok(ProcessorPtr::create(AsyncSinker::create(
            input,
            AnalyzeSendPartSink {
                sender: Some(sender),
            },
        )))
    }
}

#[async_trait::async_trait]
impl AsyncSink for AnalyzeSendPartSink {
    const NAME: &'static str = "AnalyzeSendPartSink";

    async fn on_finish(&mut self) -> Result<()> {
        drop(self.sender.take());
        Ok(())
    }

    async fn consume(&mut self, mut data_block: DataBlock) -> Result<bool> {
        if let Some(meta) = data_block.take_meta() {
            if let Some(data) = AnalyzeNDVMeta::downcast_from(meta) {
                if let Some(sender) = &self.sender {
                    let info = data.to_part();
                    let _ = sender.send(Ok(info)).await;
                }
                return Ok(false);
            }
        }
        Err(ErrorCode::Internal(
            "Cannot downcast data block meta to BlockPruneResult".to_string(),
        ))
    }
}

pub struct PartitionReceiverSource {
    pub meta_receiver: Option<Receiver<Result<PartInfoPtr>>>,
}

impl PartitionReceiverSource {
    pub fn create(
        ctx: Arc<dyn TableContext>,
        receiver: Receiver<Result<PartInfoPtr>>,
        output_port: Arc<OutputPort>,
    ) -> Result<ProcessorPtr> {
        AsyncSourcer::create(ctx, output_port, Self {
            meta_receiver: Some(receiver),
        })
    }
}

#[async_trait::async_trait]
impl AsyncSource for PartitionReceiverSource {
    const NAME: &'static str = "PartitionReceiverSource";
    const SKIP_EMPTY_DATA_BLOCK: bool = false;

    #[async_backtrace::framed]
    async fn generate(&mut self) -> Result<Option<DataBlock>> {
        if let Some(rx) = &self.meta_receiver {
            match rx.recv().await {
                Ok(Ok(part)) => {
                    let info = AnalyzeNDVMeta::from_part(&part)?;
                    Ok(Some(DataBlock::empty_with_meta(Box::new(info.clone()))))
                }
                Ok(Err(e)) => Err(
                    // The error is occurred in pruning process
                    e,
                ),
                Err(_) => {
                    // The channel is closed, we should return None to stop generating
                    Ok(None)
                }
            }
        } else {
            Ok(None)
        }
    }

    #[async_backtrace::framed]
    async fn on_finish(&mut self) -> Result<()> {
        drop(self.meta_receiver.take());
        Ok(())
    }
}
