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

use common_catalog::table_context::TableContext;
use common_exception::Result;
use common_expression::DataBlock;
use common_pipeline_core::processors::port::OutputPort;
use common_pipeline_core::processors::processor::ProcessorPtr;
use common_pipeline_sources::AsyncSource;
use common_pipeline_sources::AsyncSourcer;

use super::parquet_reader::ParquetRSReader;
use super::parquet_table::ParquetRSPart;

pub struct ParquetSource {
    ctx: Arc<dyn TableContext>,
    reader: Arc<ParquetRSReader>,
}

impl ParquetSource {
    pub fn create(
        ctx: Arc<dyn TableContext>,
        output: Arc<OutputPort>,
        reader: Arc<ParquetRSReader>,
    ) -> Result<ProcessorPtr> {
        AsyncSourcer::create(ctx.clone(), output, ParquetSource { ctx, reader })
    }
}

#[async_trait::async_trait]
impl AsyncSource for ParquetSource {
    const NAME: &'static str = "ParquetRSSource";

    #[async_trait::unboxed_simple]
    async fn generate(&mut self) -> Result<Option<DataBlock>> {
        if let Some(part) = self.ctx.get_partition() {
            let part = ParquetRSPart::from_part(&part)?;
            let block = self
                .reader
                .read_block(self.ctx.clone(), &part.location)
                .await?;
            Ok(Some(block))
        } else {
            // No more partition, finish this source.
            Ok(None)
        }
    }
}
