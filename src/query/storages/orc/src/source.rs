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

use databend_common_catalog::query_kind::QueryKind;
use databend_common_catalog::table_context::TableContext;
use databend_common_exception::ErrorCode;
use databend_common_exception::Result;
use databend_common_expression::DataBlock;
use databend_common_expression::DataSchema;
use databend_common_pipeline_core::processors::OutputPort;
use databend_common_pipeline_core::processors::ProcessorPtr;
use databend_common_pipeline_sources::SyncSource;
use databend_common_pipeline_sources::SyncSourcer;
use databend_common_storage::CopyStatus;
use databend_common_storage::FileStatus;
use opendal::Operator;
use orc_rust::ArrowReader;
use orc_rust::ArrowReaderBuilder;

use super::file_reader::OrcFileReader;
use crate::one_file_partition::OrcFilePartition;

pub struct ORCSource {
    table_ctx: Arc<dyn TableContext>,
    op: Operator,
    reader: Option<(String, ArrowReader<OrcFileReader>)>,
    data_schema: Arc<DataSchema>,

    copy_status: Option<Arc<CopyStatus>>,
}

impl ORCSource {
    pub fn try_create(
        output: Arc<OutputPort>,
        table_ctx: Arc<dyn TableContext>,
        op: Operator,
        data_schema: Arc<DataSchema>,
    ) -> Result<ProcessorPtr> {
        let copy_status = if matches!(table_ctx.get_query_kind(), QueryKind::CopyIntoTable) {
            Some(table_ctx.get_copy_status())
        } else {
            None
        };
        SyncSourcer::create(table_ctx.clone(), output, ORCSource {
            table_ctx,
            op,
            data_schema,
            reader: None,
            copy_status,
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
            let path = file.path.clone();

            let file = OrcFileReader {
                operator: self.op.clone(),
                size: file.size as u64,
                path: file.path,
            };
            let builder = ArrowReaderBuilder::try_new(file)
                .map_err(|e| ErrorCode::StorageOther(e.to_string()))?;
            let reader = builder.build();

            self.reader = Some((path, reader))
        }
        if let Some((path, reader)) = &mut self.reader {
            match reader.next() {
                None => Ok(None),
                Some(batch) => {
                    let (block, _) =
                        DataBlock::from_record_batch(self.data_schema.as_ref(), &batch?)?;
                    if let Some(copy_status) = &self.copy_status {
                        copy_status.add_chunk(path, FileStatus {
                            num_rows_loaded: block.num_rows(),
                            error: None,
                        })
                    }
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
