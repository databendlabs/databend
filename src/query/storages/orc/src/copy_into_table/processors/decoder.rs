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
use std::time::Instant;

use arrow_array::RecordBatch;
use databend_common_catalog::query_kind::QueryKind;
use databend_common_catalog::table_context::TableContext;
use databend_common_exception::Result;
use databend_common_expression::BlockEntry;
use databend_common_expression::BlockMetaInfoDowncast;
use databend_common_expression::DataBlock;
use databend_common_expression::DataSchemaRef;
use databend_common_expression::Evaluator;
use databend_common_expression::Expr;
use databend_common_expression::FunctionContext;
use databend_common_functions::BUILTIN_FUNCTIONS;
use databend_common_pipeline_transforms::processors::AccumulatingTransform;
use databend_common_storage::CopyStatus;
use databend_common_storage::FileStatus;
use orc_rust::array_decoder::NaiveStripeDecoder;

use crate::copy_into_table::projection::ProjectionFactory;
use crate::strip::StripeInMemory;
use crate::utils::map_orc_error;

pub struct StripeDecoderForCopy {
    projections: Arc<ProjectionFactory>,
    copy_status: Option<Arc<CopyStatus>>,
    output_schema: DataSchemaRef,
    func_ctx: FunctionContext,
}

impl StripeDecoderForCopy {
    pub fn try_create(
        table_ctx: Arc<dyn TableContext>,
        projections: Arc<ProjectionFactory>,
        output_schema: DataSchemaRef,
    ) -> Result<Self> {
        let copy_status = if matches!(table_ctx.get_query_kind(), QueryKind::CopyIntoTable) {
            Some(table_ctx.get_copy_status())
        } else {
            None
        };
        let func_ctx = table_ctx.get_function_context()?;
        Ok(StripeDecoderForCopy {
            output_schema,
            copy_status,
            projections,
            func_ctx,
        })
    }

    fn project(&self, block: DataBlock, projection: &[Expr]) -> Result<DataBlock> {
        let evaluator = Evaluator::new(&block, &self.func_ctx, &BUILTIN_FUNCTIONS);
        let mut columns = Vec::with_capacity(projection.len());
        for (field, expr) in self.output_schema.fields().iter().zip(projection.iter()) {
            let value = evaluator.run(expr)?;
            let column = BlockEntry::new(field.data_type().clone(), value);
            columns.push(column);
        }
        Ok(DataBlock::new(columns, block.num_rows()))
    }
}

impl AccumulatingTransform for StripeDecoderForCopy {
    const NAME: &'static str = "StripeDecoderForCopy";

    fn transform(&mut self, data: DataBlock) -> Result<Vec<DataBlock>> {
        let stripe = data
            .get_owned_meta()
            .and_then(StripeInMemory::downcast_from)
            .unwrap();
        let schema = stripe.schema.expect("schema not none");
        let projection = self.projections.get(&schema, &stripe.path)?;
        let start = Instant::now();

        let decoder = NaiveStripeDecoder::new(stripe.stripe, schema.arrow_schema.clone(), 8192)
            .map_err(|e| map_orc_error(e, &stripe.path))?;
        let batches: Result<Vec<RecordBatch>, _> = decoder.into_iter().collect();
        let batches = batches.map_err(|e| map_orc_error(e, &stripe.path))?;
        let mut blocks = vec![];
        for batch in batches {
            let (block, _) = DataBlock::from_record_batch(schema.data_schema.as_ref(), &batch)?;
            let block = self.project(block, &projection)?;
            if let Some(copy_status) = &self.copy_status {
                copy_status.add_chunk(&stripe.path, FileStatus {
                    num_rows_loaded: block.num_rows(),
                    error: None,
                })
            }
            blocks.push(block);
        }
        log::info!(
            "decode {} blocks use {} secs",
            blocks.len(),
            start.elapsed().as_secs_f32()
        );
        Ok(blocks)
    }
}
