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

use async_channel::Sender;
use async_trait::async_trait;
use async_trait::unboxed_simple;
use databend_common_exception::ErrorCode;
use databend_common_exception::Result;
use databend_common_expression::DataBlock;
use databend_common_pipeline_core::processors::InputPort;
use databend_common_pipeline_core::processors::Processor;

use crate::AsyncSink;
use crate::AsyncSinker;

pub struct UnionReceiveSink {
    sender: Option<Sender<DataBlock>>,
}

impl UnionReceiveSink {
    pub fn create(tx: Option<Sender<DataBlock>>, input: Arc<InputPort>) -> Box<dyn Processor> {
        AsyncSinker::create(input, UnionReceiveSink { sender: tx })
    }
}

#[async_trait]
impl AsyncSink for UnionReceiveSink {
    const NAME: &'static str = "UnionReceiveSink";

    #[async_backtrace::framed]
    async fn on_finish(&mut self) -> Result<()> {
        drop(self.sender.take());
        Ok(())
    }

    #[unboxed_simple]
    #[async_backtrace::framed]
    async fn consume(&mut self, data_block: DataBlock) -> Result<bool> {
        if let Some(sender) = self.sender.as_ref() {
            if sender.send(data_block).await.is_err() {
                return Err(ErrorCode::Internal("UnionReceiveSink sender failed"));
            };
        }

        Ok(false)
    }
}
