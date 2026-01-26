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

use databend_common_base::base::Progress;
use databend_common_exception::Result;
use databend_common_expression::DataBlock;
use tokio::sync::mpsc::Receiver;

use crate::core::OutputPort;
use crate::core::ProcessorPtr;
use crate::sources::sync_source::SyncSource;
use crate::sources::sync_source::SyncSourcer;

#[allow(dead_code)]
pub struct SyncReceiverSource {
    receiver: Receiver<Result<DataBlock>>,
}

impl SyncReceiverSource {
    pub fn create(
        scan: Arc<Progress>,
        rx: Receiver<Result<DataBlock>>,
        out: Arc<OutputPort>,
    ) -> Result<ProcessorPtr> {
        SyncSourcer::create(scan, out, SyncReceiverSource { receiver: rx })
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
