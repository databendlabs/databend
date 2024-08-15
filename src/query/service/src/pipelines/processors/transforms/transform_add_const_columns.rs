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
use databend_common_exception::Result;
use databend_common_expression::DataBlock;
use databend_common_expression::DataSchemaRef;
use databend_common_expression::Expr;
use databend_common_expression::Scalar as DataScalar;
use databend_common_pipeline_transforms::processors::Transform;
use databend_common_sql::evaluator::BlockOperator;
use databend_common_sql::evaluator::CompoundBlockOperator;

use crate::sessions::QueryContext;

pub struct TransformAddConstColumns {
    expression_transform: CompoundBlockOperator,
    input_len: usize,
}

impl TransformAddConstColumns
where Self: Transform
{
    /// used in insert with placeholder.
    /// e.g. for `insert into t1 (a, b, c) values (?, 1, ?)`,
    /// output_schema has all 3 columns,
    /// input_schema has columns (a, c) to load data from attachment,
    /// const_values contains a scalar 1
    pub fn try_new(
        ctx: Arc<QueryContext>,
        input_schema: DataSchemaRef,
        output_schema: DataSchemaRef,
        mut const_values: Vec<DataScalar>,
    ) -> Result<Self> {
        let fields = output_schema.fields();
        let mut exprs = Vec::with_capacity(fields.len());

        for f in fields.iter() {
            let expr = if !input_schema.has_field(f.name()) {
                Expr::Constant {
                    span: None,
                    scalar: const_values.remove(0),
                    data_type: f.data_type().clone(),
                }
            } else {
                let field = input_schema.field_with_name(f.name()).unwrap();
                let id = input_schema.index_of(f.name()).unwrap();
                Expr::ColumnRef {
                    span: None,
                    id,
                    data_type: field.data_type().clone(),
                    display_name: field.name().clone(),
                }
            };
            exprs.push(expr);
        }

        let func_ctx = ctx.get_function_context()?;
        let expression_transform = CompoundBlockOperator {
            ctx: func_ctx,
            operators: vec![BlockOperator::Map {
                exprs,
                projections: None,
            }],
        };

        Ok(Self {
            expression_transform,
            input_len: input_schema.num_fields(),
        })
    }
}

impl Transform for TransformAddConstColumns {
    const NAME: &'static str = "AddConstColumnsTransform";

    fn transform(&mut self, mut block: DataBlock) -> Result<DataBlock> {
        block = self.expression_transform.transform(block)?;
        let columns = block.columns()[self.input_len..].to_owned();
        Ok(DataBlock::new(columns, block.num_rows()))
    }
}
