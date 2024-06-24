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

use databend_common_exception::Result;
use databend_common_expression::type_check::check_cast;
use databend_common_expression::BlockEntry;
use databend_common_expression::DataBlock;
use databend_common_expression::DataSchemaRef;
use databend_common_expression::Evaluator;
use databend_common_expression::Expr;
use databend_common_expression::FunctionContext;
use databend_common_functions::BUILTIN_FUNCTIONS;
use databend_common_pipeline_transforms::processors::Transform;

/// TransformRuntimeCastSchema is used to cast block to the specified schema.
/// Different from `TransformCastSchema`, it is used at the runtime
pub struct TransformRuntimeCastSchema {
    func_ctx: FunctionContext,
    insert_schema: DataSchemaRef,
}

impl TransformRuntimeCastSchema
where Self: Transform
{
    pub fn new(insert_schema: DataSchemaRef, func_ctx: FunctionContext) -> Self {
        Self {
            func_ctx,
            insert_schema,
        }
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
                check_cast(None, false, expr, to.data_type(), &BUILTIN_FUNCTIONS)
            })
            .collect::<Result<Vec<_>>>()?;

        let mut columns = Vec::with_capacity(exprs.len());
        let evaluator = Evaluator::new(&data_block, &self.func_ctx, &BUILTIN_FUNCTIONS);

        for (field, expr) in self.insert_schema.fields().iter().zip(exprs.iter()) {
            let value = evaluator.run(expr)?;
            let column = BlockEntry::new(field.data_type().clone(), value);
            columns.push(column);
        }
        Ok(DataBlock::new(columns, data_block.num_rows()))
    }
}
