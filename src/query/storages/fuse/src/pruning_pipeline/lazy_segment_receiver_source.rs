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
use databend_common_catalog::table_context::TableContext;
use databend_common_exception::Result;
use databend_common_expression::DataBlock;
use databend_common_pipeline::core::OutputPort;
use databend_common_pipeline::core::ProcessorPtr;
use databend_common_pipeline::sources::AsyncSource;
use databend_common_pipeline::sources::AsyncSourcer;

use crate::pruning_pipeline::LazySegmentMeta;
use crate::SegmentLocation;

pub struct LazySegmentReceiverSource {
    pub meta_receiver: Receiver<SegmentLocation>,
}

impl LazySegmentReceiverSource {
    pub fn create(
        ctx: Arc<dyn TableContext>,
        receiver: Receiver<SegmentLocation>,
        output_port: Arc<OutputPort>,
    ) -> Result<ProcessorPtr> {
        AsyncSourcer::create(ctx.get_scan_progress(), output_port, Self {
            meta_receiver: receiver,
        })
    }
}

#[async_trait::async_trait]
impl AsyncSource for LazySegmentReceiverSource {
    const NAME: &'static str = "LazySegmentReceiverSource";
    const SKIP_EMPTY_DATA_BLOCK: bool = false;

    #[async_backtrace::framed]
    async fn generate(&mut self) -> Result<Option<DataBlock>> {
        match self.meta_receiver.recv().await {
            Ok(segments) => Ok(Some(DataBlock::empty_with_meta(LazySegmentMeta::create(
                segments,
            )))),
            Err(_) => {
                // The channel is closed, we should return None to stop generating
                Ok(None)
            }
        }
    }
}
