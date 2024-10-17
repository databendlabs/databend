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
use databend_common_catalog::plan::Partitions;
use databend_common_catalog::table_context::TableContext;
use databend_common_expression::DataBlock;
use databend_common_pipeline_core::processors::{OutputPort, ProcessorPtr};
use databend_common_pipeline_sources::{AsyncSource, AsyncSourcer};

use crate::pruning_pipeline::meta_info::PartitionsMeta;

pub struct AsyncMetaReceiverSource {
    pub receiver: Receiver<Partitions>,
}

impl AsyncMetaReceiverSource {
    pub fn create(
        ctx: Arc<dyn TableContext>,
        receiver: Receiver<Partitions>,
        output_port: Arc<OutputPort>) -> databend_common_exception::Result<ProcessorPtr> {
        AsyncSourcer::create(
            ctx, output_port, Self { receiver, }
        )
    }
}

#[async_trait::async_trait]
impl AsyncSource for AsyncMetaReceiverSource {
    const NAME: &'static str = "AsyncMetaReceiverSource";

    #[async_backtrace::framed]
    async fn generate(&mut self) -> databend_common_exception::Result<Option<DataBlock>> {
        match self.receiver.recv().await {
            Ok(part) => Ok(Some(DataBlock::empty_with_meta(PartitionsMeta::create(
                part,
            )))),
            Err(_) => {
                // The channel is closed, we should return None to stop generating
                Ok(None)
            }
        }
    }
}
