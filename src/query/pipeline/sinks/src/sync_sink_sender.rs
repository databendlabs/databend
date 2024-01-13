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

use databend_common_base::base::tokio::sync::mpsc::Sender;
use databend_common_exception::Result;
use databend_common_expression::DataBlock;
use databend_common_pipeline_core::processors::InputPort;
use databend_common_pipeline_core::processors::Processor;

use crate::Sink;
use crate::Sinker;

pub struct SyncSenderSink {
    sender: Sender<Result<DataBlock>>,
}

impl SyncSenderSink {
    pub fn create(sender: Sender<Result<DataBlock>>, input: Arc<InputPort>) -> Box<dyn Processor> {
        Sinker::create(input, SyncSenderSink { sender })
    }
}

#[async_trait::async_trait]
impl Sink for SyncSenderSink {
    const NAME: &'static str = "SyncSenderSink";

    fn consume(&mut self, data_block: DataBlock) -> Result<()> {
        self.sender.blocking_send(Ok(data_block)).unwrap();
        Ok(())
    }
}
