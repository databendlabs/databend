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

use common_base::tokio::sync::mpsc::Receiver;
use common_datablocks::DataBlock;
use common_exception::Result;

use crate::pipelines::new::processors::port::OutputPort;
use crate::pipelines::new::processors::processor::ProcessorPtr;
use crate::pipelines::new::processors::sources::sync_source::SyncSource;
use crate::pipelines::new::processors::sources::SyncSourcer;

pub struct SyncReceiverSource {
    receiver: Receiver<Result<DataBlock>>,
}

impl SyncReceiverSource {
    pub fn create(rx: Receiver<Result<DataBlock>>, out: Arc<OutputPort>) -> Result<ProcessorPtr> {
        SyncSourcer::create(out, SyncReceiverSource { receiver: rx })
    }
}

#[async_trait::async_trait]
impl SyncSource for SyncReceiverSource {
    const NAME: &'static str = "SyncReceiverSource";

    fn generate(&mut self) -> Result<Option<DataBlock>> {
        match self.receiver.blocking_recv() {
            None => Ok(None),
            Some(Err(cause)) => Err(cause),
            Some(Ok(data_block)) => Ok(Some(data_block)),
        }
    }
}
