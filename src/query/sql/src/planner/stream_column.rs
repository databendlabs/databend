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

use std::collections::HashMap;
use std::sync::Arc;

use databend_common_catalog::plan::StreamColumn;
use databend_common_catalog::plan::StreamColumnMeta;
use databend_common_catalog::plan::StreamColumnType;
use databend_common_exception::Result;
use databend_common_expression::DataBlock;
use databend_common_expression::FunctionContext;
use databend_common_expression::TableSchema;
use databend_common_expression::ORIGIN_BLOCK_ID_COL_NAME;
use databend_common_expression::ORIGIN_BLOCK_ROW_NUM_COL_NAME;
use databend_common_expression::ORIGIN_VERSION_COL_NAME;

use crate::evaluator::BlockOperator;
use crate::plans::BoundColumnRef;
use crate::plans::ConstantExpr;
use crate::plans::FunctionCall;
use crate::ColumnBindingBuilder;
use crate::ScalarExpr;
use crate::Visibility;

pub const CURRENT_BLOCK_ID_COL_NAME: &str = "_current_block_id";
pub const CURRENT_BLOCK_ROW_NUM_COL_NAME: &str = "_current_block_row_num";

#[derive(Clone)]
pub struct StreamContext {
    pub stream_columns: Vec<StreamColumn>,
    pub operators: Vec<BlockOperator>,
    pub func_ctx: FunctionContext,
}

impl StreamContext {
    pub fn try_create(
        func_ctx: FunctionContext,
        schema: Arc<TableSchema>,
        table_version: u64,
        is_delete: bool,
    ) -> Result<Self> {
        let input_schema = schema.remove_virtual_computed_fields();
        let num_fields = input_schema.fields().len();

        let stream_columns = [
            StreamColumn::new(ORIGIN_VERSION_COL_NAME, StreamColumnType::OriginVersion),
            StreamColumn::new(ORIGIN_BLOCK_ID_COL_NAME, StreamColumnType::OriginBlockId),
            StreamColumn::new(
                ORIGIN_BLOCK_ROW_NUM_COL_NAME,
                StreamColumnType::OriginRowNum,
            ),
        ];

        let mut new_schema_index = HashMap::with_capacity(stream_columns.len());
        let mut exprs = Vec::with_capacity(stream_columns.len());
        for stream_column in stream_columns.iter() {
            let schema_index = input_schema.index_of(stream_column.column_name()).unwrap();

            let origin_stream_column_scalar_expr = ScalarExpr::BoundColumnRef(BoundColumnRef {
                span: None,
                column: ColumnBindingBuilder::new(
                    stream_column.column_name().to_string(),
                    schema_index,
                    Box::new(stream_column.data_type()),
                    Visibility::Visible,
                )
                .build(),
            });

            let current_stream_column_scalar_expr = match stream_column.column_type() {
                StreamColumnType::OriginVersion => {
                    new_schema_index.insert(schema_index, num_fields + 2);
                    ScalarExpr::ConstantExpr(ConstantExpr {
                        span: None,
                        value: table_version.into(),
                    })
                }
                StreamColumnType::OriginBlockId => {
                    new_schema_index.insert(schema_index, num_fields + 3);
                    ScalarExpr::BoundColumnRef(BoundColumnRef {
                        span: None,
                        column: ColumnBindingBuilder::new(
                            CURRENT_BLOCK_ID_COL_NAME.to_string(),
                            num_fields + 1,
                            Box::new(stream_column.data_type()),
                            Visibility::Visible,
                        )
                        .build(),
                    })
                }
                StreamColumnType::OriginRowNum => {
                    new_schema_index.insert(schema_index, num_fields + 4);
                    ScalarExpr::BoundColumnRef(BoundColumnRef {
                        span: None,
                        column: ColumnBindingBuilder::new(
                            CURRENT_BLOCK_ROW_NUM_COL_NAME.to_string(),
                            num_fields,
                            Box::new(stream_column.data_type()),
                            Visibility::Visible,
                        )
                        .build(),
                    })
                }
                StreamColumnType::RowVersion => unreachable!(),
            };

            let new_stream_column_scalar_expr = ScalarExpr::FunctionCall(FunctionCall {
                span: None,
                func_name: "if".to_string(),
                params: vec![],
                arguments: vec![
                    ScalarExpr::FunctionCall(FunctionCall {
                        span: None,
                        func_name: "is_not_null".to_string(),
                        params: vec![],
                        arguments: vec![origin_stream_column_scalar_expr.clone()],
                    }),
                    origin_stream_column_scalar_expr,
                    current_stream_column_scalar_expr,
                ],
            });

            exprs.push(
                new_stream_column_scalar_expr
                    .as_expr()?
                    .project_column_ref(|col| col.index),
            );
        }

        // Add projection to keep input schema.
        let mut projections = Vec::with_capacity(num_fields);
        for i in 0..num_fields {
            if let Some(index) = new_schema_index.get(&i) {
                projections.push(*index);
            } else {
                projections.push(i);
            }
        }

        let operators = vec![
            BlockOperator::Map {
                exprs,
                projections: None,
            },
            BlockOperator::Project {
                projection: projections,
            },
        ];

        let stream_columns = if is_delete {
            // ORIGIN_BLOCK_ID.
            vec![stream_columns[1].clone()]
        } else {
            // ORIGIN_BLOCK_ROW_NUM, ORIGIN_BLOCK_ID.
            vec![stream_columns[2].clone(), stream_columns[1].clone()]
        };

        Ok(StreamContext {
            stream_columns,
            operators,
            func_ctx,
        })
    }

    pub fn apply(&self, block: DataBlock, meta: &StreamColumnMeta) -> Result<DataBlock> {
        let num_rows = block.num_rows();
        let mut new_block = block;
        for stream_column in self.stream_columns.iter() {
            let entry = stream_column.generate_column_values(meta, num_rows);
            new_block.add_column(entry);
        }

        self.operators
            .iter()
            .try_fold(new_block, |input, op| op.execute(&self.func_ctx, input))
    }
}
