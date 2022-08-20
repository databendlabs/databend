// Copyright 2022 Datafuse Labs.
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
use common_datablocks::DataBlock;
use common_exception::ErrorCode;
use common_exception::Result;
use common_pipeline_core::processors::port::InputPort;
use common_pipeline_core::processors::processor::ProcessorPtr;

use crate::processors::sinks::AsyncSink;
use crate::processors::sinks::AsyncSinker;

pub struct UnionReceiveSink {
    sender: Option<Sender<DataBlock>>,
}

impl UnionReceiveSink {
    pub fn create(sender: Option<Sender<DataBlock>>, input: Arc<InputPort>) -> ProcessorPtr {
        AsyncSinker::create(input, UnionReceiveSink { sender })
    }
}

#[async_trait]
impl AsyncSink for UnionReceiveSink {
    const NAME: &'static str = "UnionReceiveSink";

    async fn on_finish(&mut self) -> Result<()> {
        drop(self.sender.take());
        Ok(())
    }

    #[unboxed_simple]
    async fn consume(&mut self, data_block: DataBlock) -> Result<()> {
        if let Some(sender) = self.sender.as_ref() {
            if sender.send(data_block).await.is_err() {
                return Err(ErrorCode::UnexpectedError("UnionReceiveSink sender failed"));
            };
        }
        Ok(())
    }
}
