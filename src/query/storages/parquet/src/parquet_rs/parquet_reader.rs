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
use common_catalog::plan::DataSourcePlan;
use common_catalog::plan::PushDownInfo;
use common_catalog::table_context::TableContext;
use common_exception::ErrorCode;
use common_exception::Result;
use common_expression::types::DataType;
use common_expression::DataBlock;
use common_expression::Evaluator;
use common_expression::Expr;
use common_expression::TableSchema;
use common_functions::BUILTIN_FUNCTIONS;
use futures::StreamExt;
use opendal::Operator;
use parquet::arrow::arrow_reader::ArrowPredicateFn;
use parquet::arrow::arrow_reader::RowFilter;
use parquet::arrow::arrow_to_parquet_schema;
use parquet::arrow::ParquetRecordBatchStreamBuilder;
use parquet::arrow::ProjectionMask;

struct ParquetPredicate {
    pub projection: ProjectionMask,
    pub filter: Expr,
}

pub struct ParquetRSReader {
    op: Operator,
    predicate: Option<ParquetPredicate>,
    projection: ProjectionMask,
}

impl ParquetRSReader {
    pub fn create(
        op: Operator,
        table_schema: &TableSchema,
        arrow_schema: &arrow_schema::Schema,
        plan: &DataSourcePlan,
    ) -> Result<Self> {
        let source_projection =
            PushDownInfo::projection_of_push_downs(table_schema, &plan.push_downs);
        let schema_descr = arrow_to_parquet_schema(arrow_schema)?;
        let projection = source_projection.to_arrow_projection(&schema_descr);
        let predicate = PushDownInfo::prewhere_of_push_downs(&plan.push_downs).map(|prewhere| {
            let schema = prewhere.prewhere_columns.project_schema(table_schema);
            let filter = prewhere
                .filter
                .as_expr(&BUILTIN_FUNCTIONS)
                .project_column_ref(|name| schema.index_of(name).unwrap());
            let projection = prewhere.prewhere_columns.to_arrow_projection(&schema_descr);
            ParquetPredicate { projection, filter }
        });
        Ok(Self {
            op,
            predicate,
            projection,
        })
    }

    /// Read a [`DataBlock`] from parquet file using native apache arrow-rs APIs.
    pub async fn read_block(&self, ctx: Arc<dyn TableContext>, loc: &str) -> Result<DataBlock> {
        let reader = self.op.reader(loc).await?;
        // TODOs:
        // - set batch size to generate block one by one.
        // - specify row groups to read.
        // - set row selections.
        let mut builder = ParquetRecordBatchStreamBuilder::new(reader)
            .await?
            .with_projection(self.projection.clone());

        if let Some(ParquetPredicate { projection, filter }) = &self.predicate {
            let func_ctx = ctx.get_function_context()?;
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
                b.map_err(|e| ErrorCode::Internal(format!("Read from parquet stream failed: {e}")))
                    .and_then(|b| {
                        let (block, _) = DataBlock::from_record_batch(&b)?;
                        Ok(block)
                    })
            })
            .collect::<Result<Vec<_>>>()?;
        DataBlock::concat(&blocks)
    }
}
