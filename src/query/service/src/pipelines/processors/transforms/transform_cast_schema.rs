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

use databend_common_exception::Result;
use databend_common_expression::type_check::check_function;
use databend_common_expression::types::DataType;
use databend_common_expression::BlockEntry;
use databend_common_expression::DataBlock;
use databend_common_expression::DataSchemaRef;
use databend_common_expression::Evaluator;
use databend_common_expression::Expr;
use databend_common_expression::FunctionContext;
use databend_common_functions::BUILTIN_FUNCTIONS;
use databend_common_pipeline_transforms::processors::Transform;
use databend_common_pipeline_transforms::processors::Transformer;

use crate::pipelines::processors::InputPort;
use crate::pipelines::processors::OutputPort;
use crate::pipelines::processors::ProcessorPtr;

pub struct TransformCastSchema {
    func_ctx: FunctionContext,
    insert_schema: DataSchemaRef,
    exprs: Vec<Expr>,
}

impl TransformCastSchema
where Self: Transform
{
    pub fn try_create(
        input_port: Arc<InputPort>,
        output_port: Arc<OutputPort>,
        select_schema: DataSchemaRef,
        insert_schema: DataSchemaRef,
        func_ctx: FunctionContext,
    ) -> Result<ProcessorPtr> {
        let exprs = select_schema
            .fields()
            .iter()
            .zip(insert_schema.fields().iter().enumerate())
            .map(|(from, (index, to))| {
                let expr = Expr::ColumnRef {
                    span: None,
                    id: index,
                    data_type: from.data_type().clone(),
                    display_name: from.name().clone(),
                };
                if from != to {
                    if from.data_type().remove_nullable() == DataType::String
                        && to.data_type().remove_nullable() == DataType::Variant
                    {
                        // parse string to JSON value, avoid cast string to JSON string
                        check_function(None, "parse_json", &[], &[expr], &BUILTIN_FUNCTIONS)
                            .unwrap()
                    } else {
                        Expr::Cast {
                            span: None,
                            is_try: false,
                            expr: Box::new(expr),
                            dest_type: to.data_type().clone(),
                        }
                    }
                } else {
                    expr
                }
            })
            .collect();
        Ok(ProcessorPtr::create(Transformer::create(
            input_port,
            output_port,
            Self {
                func_ctx,
                insert_schema,
                exprs,
            },
        )))
    }
}

impl Transform for TransformCastSchema {
    const NAME: &'static str = "CastSchemaTransform";

    fn transform(&mut self, data_block: DataBlock) -> Result<DataBlock> {
        let mut columns = Vec::with_capacity(self.exprs.len());
        let evaluator = Evaluator::new(&data_block, &self.func_ctx, &BUILTIN_FUNCTIONS);
        for (field, expr) in self.insert_schema.fields().iter().zip(self.exprs.iter()) {
            let value = evaluator.run(expr)?;
            let column = BlockEntry::new(field.data_type().clone(), value);
            columns.push(column);
        }
        Ok(DataBlock::new(columns, data_block.num_rows()))
    }
}
