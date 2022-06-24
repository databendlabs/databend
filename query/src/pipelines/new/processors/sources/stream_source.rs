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

use common_datablocks::DataBlock;
use common_exception::ErrorCode;
use common_exception::Result;
use common_streams::SendableDataBlockStream;
use futures::StreamExt;

use crate::pipelines::new::processors::port::OutputPort;
use crate::pipelines::new::processors::processor::ProcessorPtr;
use crate::pipelines::new::processors::AsyncSource;
use crate::pipelines::new::processors::AsyncSourcer;
use crate::sessions::QueryContext;

pub struct StreamSource {
    stream: Option<SendableDataBlockStream>,
}

impl StreamSource {
    pub fn create(
        ctx: Arc<QueryContext>,
        stream: Option<SendableDataBlockStream>,
        out: Arc<OutputPort>,
    ) -> Result<ProcessorPtr> {
        AsyncSourcer::create(ctx, out, StreamSource { stream })
    }
}

#[async_trait::async_trait]
impl AsyncSource for StreamSource {
    const NAME: &'static str = "stream source";

    #[async_trait::unboxed_simple]
    async fn generate(&mut self) -> Result<Option<DataBlock>> {
        match self
            .stream
            .as_mut()
            .ok_or_else(|| ErrorCode::EmptyData("input stream not exist or consumed"))?
            .next()
            .await
        {
            Some(Ok(block)) => Ok(Some(block)),
            Some(Err(e)) => Err(e),
            None => Ok(None),
        }
    }
}
