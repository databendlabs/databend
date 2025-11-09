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
use databend_common_catalog::plan::PartInfoPtr;
use databend_common_catalog::table_context::TableContext;
use databend_common_exception::Result;
use databend_common_expression::DataBlock;
use databend_common_pipeline::core::OutputPort;
use databend_common_pipeline::core::ProcessorPtr;
use databend_common_pipeline::sources::AsyncSource;
use databend_common_pipeline::sources::AsyncSourcer;

use crate::operations::read::block_partition_meta::BlockPartitionMeta;

pub struct BlockPartitionReceiverSource {
    pub meta_receiver: Option<Receiver<Result<PartInfoPtr>>>,
}

impl BlockPartitionReceiverSource {
    pub fn create(
        ctx: Arc<dyn TableContext>,
        receiver: Receiver<Result<PartInfoPtr>>,
        output_port: Arc<OutputPort>,
    ) -> Result<ProcessorPtr> {
        AsyncSourcer::create(ctx.get_scan_progress(), output_port, Self {
            meta_receiver: Some(receiver),
        })
    }
}

#[async_trait::async_trait]
impl AsyncSource for BlockPartitionReceiverSource {
    const NAME: &'static str = "BlockPartitionReceiverSource";
    const SKIP_EMPTY_DATA_BLOCK: bool = false;

    #[async_backtrace::framed]
    async fn generate(&mut self) -> Result<Option<DataBlock>> {
        if let Some(rx) = &self.meta_receiver {
            match rx.recv().await {
                Ok(Ok(part)) => Ok(Some(DataBlock::empty_with_meta(
                    BlockPartitionMeta::create(vec![part]),
                ))),
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
