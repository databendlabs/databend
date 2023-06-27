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

use std::io::Cursor;
use std::sync::Arc;

use common_arrow::arrow::io::parquet::read::infer_schema;
use common_arrow::arrow::io::parquet::read::read_metadata;
use common_arrow::arrow::io::parquet::read::FileReader;
use common_catalog::plan::AggIndexInfo;
use common_catalog::plan::AggIndexMeta;
use common_catalog::table_context::TableContext;
use common_exception::Result;
use common_expression::types::BooleanType;
use common_expression::types::DataType;
use common_expression::BlockEntry;
use common_expression::DataBlock;
use common_expression::DataSchema;
use common_expression::Evaluator;
use common_expression::Expr;
use common_expression::FunctionContext;
use common_expression::Scalar;
use common_expression::Value;
use common_functions::BUILTIN_FUNCTIONS;
use opendal::Operator;
use tracing::warn;

#[derive(Clone)]
pub struct AggIndexReader {
    index_id: u64,

    // TODO: use `BlockReader` to support partitially reading.
    dal: Operator,

    func_ctx: FunctionContext,
    schema: DataSchema,
    selection: Vec<(Expr, Option<usize>)>,
    filter: Option<Expr>,

    /// The size of the output fields of a table scan plan without the index.
    pub actual_table_field_len: usize,
}

impl AggIndexReader {
    pub fn try_create(
        ctx: Arc<dyn TableContext>,
        dal: Operator,
        agg: &AggIndexInfo,
    ) -> Result<Self> {
        let func_ctx = ctx.get_function_context()?;
        let selection = agg
            .selection
            .iter()
            .map(|(sel, offset)| (sel.as_expr(&BUILTIN_FUNCTIONS), *offset))
            .collect();
        let filter = agg.filter.as_ref().map(|f| f.as_expr(&BUILTIN_FUNCTIONS));

        Ok(Self {
            index_id: agg.index_id,
            dal,
            func_ctx,
            schema: agg.schema.clone(),
            selection,
            filter,
            actual_table_field_len: agg.actual_table_field_len,
        })
    }

    #[inline(always)]
    pub fn index_id(&self) -> u64 {
        self.index_id
    }

    pub fn sync_read_data(&self, loc: &str) -> Option<Vec<u8>> {
        match self.dal.blocking().read(loc) {
            Ok(data) => Some(data),
            Err(e) => {
                if e.kind() == opendal::ErrorKind::NotFound {
                    warn!("Aggregating index `{loc}` not found.")
                } else {
                    warn!("Read aggregating index `{loc}` failed: {e}");
                }
                None
            }
        }
    }

    pub async fn read_data(&self, loc: &str) -> Option<Vec<u8>> {
        match self.dal.read(loc).await {
            Ok(data) => Some(data),
            Err(e) => {
                if e.kind() == opendal::ErrorKind::NotFound {
                    warn!("Aggregating index `{loc}` not found.")
                } else {
                    warn!("Read aggregating index `{loc}` failed: {e}");
                }
                None
            }
        }
    }

    pub fn deserialize(&self, data: &[u8]) -> Result<DataBlock> {
        let mut reader = Cursor::new(data);
        let metadata = read_metadata(&mut reader)?;
        let schema = infer_schema(&metadata)?;
        let reader = FileReader::new(reader, metadata.row_groups, schema, None, None, None);
        let chunks = reader.collect::<common_arrow::arrow::error::Result<Vec<_>>>()?;
        debug_assert_eq!(chunks.len(), 1);

        // 1. Deserialize to `DataBlock`
        let block = DataBlock::from_arrow_chunk(&chunks[0], &self.schema)?;
        let evaluator = Evaluator::new(&block, &self.func_ctx, &BUILTIN_FUNCTIONS);

        // 2. Filter the block if there is a filter.
        let block = if let Some(filter) = self.filter.as_ref() {
            let filter = evaluator
                .run(filter)?
                .try_downcast::<BooleanType>()
                .unwrap();
            block.filter_boolean_value(&filter)?
        } else {
            block
        };

        // 3. Compute the output block
        // Fill dummy columns first.
        let mut output_columns = vec![
            BlockEntry {
                data_type: DataType::Null,
                value: Value::Scalar(Scalar::Null),
            };
            self.actual_table_field_len
        ];
        let evaluator = Evaluator::new(&block, &self.func_ctx, &BUILTIN_FUNCTIONS);
        for (expr, offset) in self.selection.iter() {
            let data_type = expr.data_type().clone();
            let value = evaluator.run(expr)?;
            let col = BlockEntry { data_type, value };

            if let Some(pos) = offset {
                output_columns[*pos] = col;
            } else {
                output_columns.push(col);
            }
        }

        Ok(DataBlock::new_with_meta(
            output_columns,
            block.num_rows(),
            Some(AggIndexMeta::create()),
        ))
    }
}
