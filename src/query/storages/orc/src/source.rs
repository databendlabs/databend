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
use databend_common_expression::DataSchema;
use databend_common_pipeline_core::processors::OutputPort;
use databend_common_pipeline_core::processors::ProcessorPtr;
use databend_common_pipeline_sources::SyncSource;
use databend_common_pipeline_sources::SyncSourcer;
use opendal::Operator;
use orc_rust::ArrowReader;
use orc_rust::ArrowReaderBuilder;

use super::file_reader::OrcFileReader;
use crate::one_file_partition::OrcFilePartition;

pub struct ORCSource {
    table_ctx: Arc<dyn TableContext>,
    op: Operator,
    reader: Option<ArrowReader<OrcFileReader>>,
    data_schema: Arc<DataSchema>,
}

impl ORCSource {
    pub fn try_create(
        output: Arc<OutputPort>,
        table_ctx: Arc<dyn TableContext>,
        op: Operator,
        data_schema: Arc<DataSchema>,
    ) -> Result<ProcessorPtr> {
        SyncSourcer::create(table_ctx.clone(), output, ORCSource {
            table_ctx,
            op,
            data_schema,
            reader: None,
        })
    }
}

impl SyncSource for ORCSource {
    const NAME: &'static str = "ORCSource";

    fn generate(&mut self) -> Result<Option<DataBlock>> {
        if self.reader.is_none() {
            let part = match self.table_ctx.get_partition() {
                Some(part) => part,
                None => return Ok(None),
            };
            let file = OrcFilePartition::from_part(&part)?.clone();

            let file = OrcFileReader {
                operator: self.op.clone(),
                size: file.size as u64,
                path: file.path,
            };
            let builder = ArrowReaderBuilder::try_new(file)
                .map_err(|e| ErrorCode::StorageOther(e.to_string()))?;
            let reader = builder.build();

            self.reader = Some(reader)
        }
        if let Some(reader) = &mut self.reader {
            match reader.next() {
                None => Ok(None),
                Some(batch) => {
                    let (block, _) =
                        DataBlock::from_record_batch(self.data_schema.as_ref(), &batch?)?;
                    Ok(Some(block))
                }
            }
        } else {
            Err(ErrorCode::Internal(
                "Bug: ORCSource: should not be called with reader != None.",
            ))
        }
    }
}
