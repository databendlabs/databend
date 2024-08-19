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

use databend_common_catalog::table_context::TableContext;
use databend_common_exception::ErrorCode;
use databend_common_exception::Result;
use databend_common_expression::DataBlock;
use databend_common_expression::SendableDataBlockStream;
use databend_common_pipeline_core::processors::OutputPort;
use databend_common_pipeline_core::processors::ProcessorPtr;
use futures::StreamExt;

use crate::AsyncSource;
use crate::AsyncSourcer;

/// AsyncSource backed by a stream
pub struct AsyncStreamSource<const SKIP_EMPTY_DATA_BLOCK: bool> {
    stream: Option<SendableDataBlockStream>,
}

/// AsyncSource backed by a stream, and will skip empty data blocks
pub type StreamSource = AsyncStreamSource<true>;

/// AsyncSource backed by a stream, which will NOT skip empty data blocks.
/// Needed in situations where an empty block with schema should be returned
pub type StreamSourceNoSkipEmpty = AsyncStreamSource<false>;

impl<const T: bool> AsyncStreamSource<T> {
    pub fn new(stream: Option<SendableDataBlockStream>) -> Self {
        AsyncStreamSource { stream }
    }

    pub fn create(
        ctx: Arc<dyn TableContext>,
        stream: Option<SendableDataBlockStream>,
        out: Arc<OutputPort>,
    ) -> Result<ProcessorPtr> {
        AsyncSourcer::create(ctx, out, AsyncStreamSource::<T> { stream })
    }
}

#[async_trait::async_trait]
impl<const T: bool> AsyncSource for AsyncStreamSource<T> {
    const NAME: &'static str = "stream source";
    const SKIP_EMPTY_DATA_BLOCK: bool = T;

    #[async_backtrace::framed]
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
