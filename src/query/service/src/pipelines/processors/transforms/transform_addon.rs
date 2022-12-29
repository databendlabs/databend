// Copyright 2022 Datafuse Labs.
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

use common_catalog::table_context::TableContext;
use common_exception::Result;
use common_expression::BlockEntry;
use common_expression::DataBlock;
use common_expression::DataField;
use common_expression::DataSchema;
use common_expression::DataSchemaRef;
use common_expression::Expr;
use common_sql::evaluator::BlockOperator;
use common_sql::evaluator::CompoundBlockOperator;
use common_sql::parse_exprs;
use common_storages_factory::Table;

use crate::pipelines::processors::port::InputPort;
use crate::pipelines::processors::port::OutputPort;
use crate::pipelines::processors::processor::ProcessorPtr;
use crate::pipelines::processors::transforms::transform::Transform;
use crate::pipelines::processors::transforms::transform::Transformer;
use crate::sessions::QueryContext;

pub struct TransformAddOn {
    expression_transform: CompoundBlockOperator,
    input_len: usize,
}

impl TransformAddOn
where Self: Transform
{
    pub fn try_create(
        input: Arc<InputPort>,
        output: Arc<OutputPort>,
        input_schema: DataSchemaRef,
        table: Arc<dyn Table>,
        ctx: Arc<QueryContext>,
    ) -> Result<ProcessorPtr> {
        let fields = table
            .schema()
            .fields()
            .iter()
            .map(DataField::from)
            .collect::<Vec<_>>();

        let mut ops = Vec::with_capacity(fields.len());
        for f in fields.iter() {
            let expr = if !input_schema.has_field(f.name()) {
                if let Some(default_expr) = f.default_expr() {
                    let mut expr = parse_exprs(ctx.clone(), table.clone(), default_expr)?;
                    expr.remove(0)
                } else {
                    let default_value = f.data_type().default_value();
                    Expr::Constant {
                        span: None,
                        scalar: default_value,
                        data_type: f.data_type().clone(),
                    }
                }
            } else {
                let field = input_schema.field_with_name(f.name()).unwrap();
                let id = input_schema.index_of(f.name()).unwrap();
                Expr::ColumnRef {
                    span: None,
                    id,
                    data_type: field.data_type().clone(),
                }
            };

            ops.push(BlockOperator::Map { expr });
        }

        let func_ctx = ctx.try_get_function_context()?;
        let expression_transform = CompoundBlockOperator {
            ctx: func_ctx,
            operators: ops,
        };

        Ok(Transformer::create(input, output, Self {
            expression_transform,
            input_len: input_schema.num_fields(),
        }))
    }
}

impl Transform for TransformAddOn {
    const NAME: &'static str = "AddOnTransform";

    fn transform(&mut self, mut block: DataBlock) -> Result<DataBlock> {
        block = self.expression_transform.transform(block)?;
        let columns = block.columns()[self.input_len..].to_owned();
        Ok(DataBlock::new(columns, block.num_rows()))
    }
}
