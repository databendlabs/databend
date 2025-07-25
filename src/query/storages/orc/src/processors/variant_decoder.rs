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
use databend_common_expression::TableDataType;
use databend_common_expression::TableSchema;
use databend_common_pipeline_transforms::processors::AccumulatingTransform;
use databend_common_storage::CopyStatus;
use databend_common_storage::FileStatus;
use databend_storages_common_stage::record_batch_to_variant_block;
use jiff::tz::TimeZone;
use orc_rust::array_decoder::NaiveStripeDecoder;

use crate::strip::StripeInMemory;
use crate::utils::map_orc_error;

pub struct StripeDecoderForVariantTable {
    copy_status: Option<Arc<CopyStatus>>,
    tz: TimeZone,
}

impl StripeDecoderForVariantTable {
    pub fn new(table_ctx: Arc<dyn TableContext>, tz: TimeZone) -> Self {
        let copy_status = if matches!(table_ctx.get_query_kind(), QueryKind::CopyIntoTable) {
            Some(table_ctx.get_copy_status())
        } else {
            None
        };
        StripeDecoderForVariantTable { copy_status, tz }
    }
}

fn schema_to_tuple_type(schema: &TableSchema) -> TableDataType {
    TableDataType::Tuple {
        fields_name: schema.fields.iter().map(|f| f.name.clone()).collect(),
        fields_type: schema.fields.iter().map(|f| f.data_type.clone()).collect(),
    }
}

impl AccumulatingTransform for StripeDecoderForVariantTable {
    const NAME: &'static str = "StripeDecoder";

    fn transform(&mut self, data: DataBlock) -> Result<Vec<DataBlock>> {
        let stripe = data
            .get_owned_meta()
            .and_then(StripeInMemory::downcast_from)
            .unwrap();
        let schemas = stripe
            .schema
            .ok_or_else(|| ErrorCode::Internal("stripe.schema should not be None".to_string()))?;

        let typ = schema_to_tuple_type(&schemas.table_schema);
        let decoder = NaiveStripeDecoder::new(stripe.stripe, schemas.arrow_schema.clone(), 8192)
            .map_err(|e| map_orc_error(e, &stripe.path))?;
        let batches: std::result::Result<Vec<RecordBatch>, _> = decoder.into_iter().collect();
        let batches = batches.map_err(|e| map_orc_error(e, &stripe.path))?;

        let mut blocks = vec![];
        for batch in batches {
            let block = record_batch_to_variant_block(batch, &self.tz, &typ, &schemas.data_schema)?;
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
