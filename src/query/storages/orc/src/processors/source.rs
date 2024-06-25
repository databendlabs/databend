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

use std::mem;
use std::sync::Arc;

use databend_common_catalog::table_context::TableContext;
use databend_common_exception::ErrorCode;
use databend_common_exception::Result;
use databend_common_expression::DataBlock;
use databend_common_pipeline_core::processors::OutputPort;
use databend_common_pipeline_core::processors::ProcessorPtr;
use databend_common_pipeline_sources::AsyncSource;
use databend_common_pipeline_sources::AsyncSourcer;
use opendal::Operator;
use orc_rust::async_arrow_reader::StripeFactory;
use orc_rust::ArrowReaderBuilder;

use crate::chunk_reader_impl::OrcChunkReader;
use crate::orc_file_partition::OrcFilePartition;
use crate::strip::StripeInMemory;

pub struct ORCSource {
    table_ctx: Arc<dyn TableContext>,
    op: Operator,
    pub(crate) reader: Option<(String, Box<StripeFactory<OrcChunkReader>>)>,

    arrow_schema: arrow_schema::SchemaRef,
    schema_from: String,
}

impl ORCSource {
    pub fn try_create(
        output: Arc<OutputPort>,
        table_ctx: Arc<dyn TableContext>,
        op: Operator,
        arrow_schema: arrow_schema::SchemaRef,
        schema_from: String,
    ) -> Result<ProcessorPtr> {
        AsyncSourcer::create(table_ctx.clone(), output, ORCSource {
            table_ctx,
            op,
            reader: None,
            arrow_schema,
            schema_from,
        })
    }

    fn check_file_schema(&self, arrow_schema: arrow_schema::SchemaRef, path: &str) -> Result<()> {
        if self.arrow_schema.fields != arrow_schema.fields {
            return Err(ErrorCode::TableSchemaMismatch(format!(
                "infer schema from '{}', but get diff schema in file '{}'. Expected schema: {:?}, actual: {:?}",
                self.schema_from, path, self.arrow_schema, arrow_schema
            )));
        }
        Ok(())
    }

    async fn next_part(&mut self) -> Result<bool> {
        let part = match self.table_ctx.get_partition() {
            Some(part) => part,
            None => return Ok(false),
        };
        let file = OrcFilePartition::from_part(&part)?.clone();
        let path = file.path.clone();

        let file = OrcChunkReader {
            operator: self.op.clone(),
            size: file.size as u64,
            path: file.path,
        };
        let builder = ArrowReaderBuilder::try_new_async(file)
            .await
            .map_err(|e| map_orc_error(e, &path))?;
        let mut reader = builder.build_async();
        let factory = mem::take(&mut reader.factory).unwrap();
        let schema = reader.schema();
        self.check_file_schema(schema, &path)?;

        self.reader = Some((path, factory));
        Ok(true)
    }
}

#[async_trait::async_trait]
impl AsyncSource for ORCSource {
    const NAME: &'static str = "ORCSource";
    const SKIP_EMPTY_DATA_BLOCK: bool = false;

    #[async_trait::unboxed_simple]
    #[async_backtrace::framed]
    async fn generate(&mut self) -> Result<Option<DataBlock>> {
        loop {
            if self.reader.is_none() && !self.next_part().await? {
                return Ok(None);
            }
            if let Some((path, factory)) = mem::take(&mut self.reader) {
                let (factory, stripe) = factory
                    .read_next_stripe()
                    .await
                    .map_err(|e| ErrorCode::StorageOther(e.to_string()))?;
                match stripe {
                    None => {
                        self.reader = None;
                        continue;
                    }
                    Some(stripe) => {
                        self.reader = Some((path.clone(), Box::new(factory)));
                        let meta = Box::new(StripeInMemory {
                            path,
                            stripe,
                            schema: None,
                        });
                        return Ok(Some(DataBlock::empty_with_meta(meta)));
                    }
                }
            } else {
                return Err(ErrorCode::Internal(
                    "Bug: ORCSource: should not be called with reader != None.",
                ));
            }
        }
    }
}
