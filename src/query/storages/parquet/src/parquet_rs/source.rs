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

use arrow_array::BooleanArray;
use arrow_schema::ArrowError;
use common_catalog::table_context::TableContext;
use common_exception::ErrorCode;
use common_exception::Result;
use common_expression::types::DataType;
use common_expression::DataBlock;
use common_expression::Evaluator;
use common_expression::Expr;
use common_functions::BUILTIN_FUNCTIONS;
use common_pipeline_core::processors::port::OutputPort;
use common_pipeline_core::processors::processor::ProcessorPtr;
use common_pipeline_sources::AsyncSource;
use common_pipeline_sources::AsyncSourcer;
use futures::StreamExt;
use opendal::Operator;
use parquet::arrow::arrow_reader::ArrowPredicateFn;
use parquet::arrow::arrow_reader::RowFilter;
use parquet::arrow::ParquetRecordBatchStreamBuilder;
use parquet::arrow::ProjectionMask;

use super::partition::ParquetRSPart;

pub struct ParquetPredicate {
    pub projection: ProjectionMask,
    pub filter: Expr,
}

pub struct ParquetSource {
    ctx: Arc<dyn TableContext>,
    op: Operator,
    projection: ProjectionMask,
    predicate: Arc<Option<ParquetPredicate>>,
}

impl ParquetSource {
    pub fn create(
        ctx: Arc<dyn TableContext>,
        output: Arc<OutputPort>,
        op: Operator,
        projection: ProjectionMask,
        predicate: Arc<Option<ParquetPredicate>>,
    ) -> Result<ProcessorPtr> {
        AsyncSourcer::create(ctx.clone(), output, ParquetSource {
            ctx,
            op,
            projection,
            predicate,
        })
    }
}

#[async_trait::async_trait]
impl AsyncSource for ParquetSource {
    const NAME: &'static str = "ParquetRSSource";

    #[async_trait::unboxed_simple]
    async fn generate(&mut self) -> Result<Option<DataBlock>> {
        if let Some(part) = self.ctx.get_partition() {
            let part = ParquetRSPart::from_part(&part)?;
            let reader = self.op.reader(&part.location).await?;

            // TODOs:
            // - set batch size to generate block one by one.
            // - specify row groups to read.
            // - set row selections.
            let mut builder = ParquetRecordBatchStreamBuilder::new(reader)
                .await?
                .with_projection(self.projection.clone());

            if let Some(ParquetPredicate { projection, filter }) = self.predicate.as_ref() {
                let func_ctx = self.ctx.get_function_context()?;
                let projection = projection.clone();
                let filter = filter.clone();
                let predicate_fn = move |batch| {
                    let res: Result<BooleanArray> = try {
                        let (block, _) = DataBlock::from_record_batch(&batch)?;
                        let evaluator = Evaluator::new(&block, &func_ctx, &BUILTIN_FUNCTIONS);
                        let res = evaluator
                            .run(&filter)?
                            .convert_to_full_column(&DataType::Boolean, batch.num_rows())
                            .into_arrow_rs()?;
                        BooleanArray::from(res.to_data())
                    };
                    res.map_err(|e| ArrowError::from_external_error(Box::new(e)))
                };
                builder = builder.with_row_filter(RowFilter::new(vec![Box::new(
                    ArrowPredicateFn::new(projection, predicate_fn),
                )]));
            }

            let stream = builder.build()?;
            let record_batches = stream.collect::<Vec<_>>().await;
            let blocks = record_batches
                .into_iter()
                .map(|b| {
                    b.map_err(|e| {
                        ErrorCode::Internal(format!("Read from parquet stream failed: {e}"))
                    })
                    .and_then(|b| {
                        let (block, _) = DataBlock::from_record_batch(&b)?;
                        Ok(block)
                    })
                })
                .collect::<Result<Vec<_>>>()?;
            let block = DataBlock::concat(&blocks)?;
            Ok(Some(block))
        } else {
            // No more partition, finish this source.
            Ok(None)
        }
    }
}
