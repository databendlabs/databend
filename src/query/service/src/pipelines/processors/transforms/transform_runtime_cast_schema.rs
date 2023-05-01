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

use common_exception::Result;
use common_expression::BlockEntry;
use common_expression::DataBlock;
use common_expression::DataSchemaRef;
use common_expression::Evaluator;
use common_expression::Expr;
use common_expression::FunctionContext;
use common_functions::BUILTIN_FUNCTIONS;

use crate::pipelines::processors::port::InputPort;
use crate::pipelines::processors::port::OutputPort;
use crate::pipelines::processors::processor::ProcessorPtr;
use crate::pipelines::processors::transforms::transform::Transform;
use crate::pipelines::processors::transforms::transform::Transformer;

/// TransformRuntimeCastSchema is used to cast block to the specified schema.
/// Different from `TransformCastSchema`, it is used at the runtime
pub struct TransformRuntimeCastSchema {
    func_ctx: FunctionContext,
    insert_schema: DataSchemaRef,
}

impl TransformRuntimeCastSchema
where Self: Transform
{
    pub fn try_create(
        input_port: Arc<InputPort>,
        output_port: Arc<OutputPort>,
        insert_schema: DataSchemaRef,
        func_ctx: FunctionContext,
    ) -> Result<ProcessorPtr> {
        Ok(ProcessorPtr::create(Transformer::create(
            input_port,
            output_port,
            Self {
                func_ctx,
                insert_schema,
            },
        )))
    }
}

impl Transform for TransformRuntimeCastSchema {
    const NAME: &'static str = "CastSchemaTransform";

    fn transform(&mut self, data_block: DataBlock) -> Result<DataBlock> {
        let exprs: Vec<Expr> = data_block
            .columns()
            .iter()
            .zip(self.insert_schema.fields().iter().enumerate())
            .map(|(from, (index, to))| {
                let expr = Expr::ColumnRef {
                    span: None,
                    id: index,
                    data_type: from.data_type.clone(),
                    display_name: to.name().clone(),
                };
                if &from.data_type != to.data_type() {
                    Expr::Cast {
                        span: None,
                        is_try: false,
                        expr: Box::new(expr),
                        dest_type: to.data_type().clone(),
                    }
                } else {
                    expr
                }
            })
            .collect();

        let mut columns = Vec::with_capacity(exprs.len());
        let evaluator = Evaluator::new(&data_block, &self.func_ctx, &BUILTIN_FUNCTIONS);

        for (field, expr) in self.insert_schema.fields().iter().zip(exprs.iter()) {
            let value = evaluator.run(expr)?;
            let column = BlockEntry {
                data_type: field.data_type().clone(),
                value,
            };
            columns.push(column);
        }
        Ok(DataBlock::new(columns, data_block.num_rows()))
    }
}
