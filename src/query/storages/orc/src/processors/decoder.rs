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

use arrow_array::RecordBatch;
use databend_common_catalog::query_kind::QueryKind;
use databend_common_catalog::table_context::TableContext;
use databend_common_exception::ErrorCode;
use databend_common_exception::Result;
use databend_common_expression::BlockMetaInfoDowncast;
use databend_common_expression::DataBlock;
use databend_common_expression::DataSchema;
use databend_common_pipeline_transforms::processors::AccumulatingTransform;
use databend_common_storage::CopyStatus;
use databend_common_storage::FileStatus;
use orc_rust::array_decoder::NaiveStripeDecoder;

use crate::strip::StripeInMemory;

pub struct StripeDecoder {
    data_schema: Arc<DataSchema>,
    arrow_schema: arrow_schema::SchemaRef,
    copy_status: Option<Arc<CopyStatus>>,
}

impl StripeDecoder {
    pub fn try_create(
        table_ctx: Arc<dyn TableContext>,
        data_schema: Arc<DataSchema>,
        arrow_schema: arrow_schema::SchemaRef,
    ) -> Result<Self> {
        let copy_status = if matches!(table_ctx.get_query_kind(), QueryKind::CopyIntoTable) {
            Some(table_ctx.get_copy_status())
        } else {
            None
        };
        Ok(StripeDecoder {
            copy_status,
            arrow_schema,
            data_schema,
        })
    }
}

impl AccumulatingTransform for StripeDecoder {
    const NAME: &'static str = "StripeDecoder";

    fn transform(&mut self, data: DataBlock) -> Result<Vec<DataBlock>> {
        let stripe = data
            .get_owned_meta()
            .and_then(StripeInMemory::downcast_from)
            .unwrap();

        let decoder = NaiveStripeDecoder::new(stripe.stripe, self.arrow_schema.clone(), 8192)
            .map_err(|e| ErrorCode::BadBytes(e.to_string()))?;
        let batches: Result<Vec<RecordBatch>, _> = decoder.into_iter().collect();
        let batches = batches.map_err(|e| ErrorCode::BadBytes(e.to_string()))?;
        let mut blocks = vec![];
        for batch in batches {
            let (block, _) = DataBlock::from_record_batch(self.data_schema.as_ref(), &batch)?;
            if let Some(copy_status) = &self.copy_status {
                copy_status.add_chunk(&stripe.path, FileStatus {
                    num_rows_loaded: block.num_rows(),
                    error: None,
                })
            }
            blocks.push(block);
        }
        Ok(blocks)
    }
}
