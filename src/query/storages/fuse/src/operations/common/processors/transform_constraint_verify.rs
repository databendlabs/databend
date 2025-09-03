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

use databend_common_exception::ErrorCode;
use databend_common_expression::DataBlock;
use databend_common_expression::Evaluator;
use databend_common_expression::Expr;
use databend_common_expression::FunctionContext;
use databend_common_expression::Value;
use databend_common_functions::BUILTIN_FUNCTIONS;
use databend_common_pipeline_transforms::AsyncAccumulatingTransform;

pub struct TransformConstraintVerify {
    constraints: Vec<(String, Expr)>,
    func_ctx: FunctionContext,
    table_name: String,
}

impl TransformConstraintVerify {
    pub fn new(
        constraints: Vec<(String, Expr)>,
        func_ctx: FunctionContext,
        table_name: String,
    ) -> Self {
        Self {
            constraints,
            func_ctx,
            table_name,
        }
    }
}

#[async_trait::async_trait]
impl AsyncAccumulatingTransform for TransformConstraintVerify {
    const NAME: &'static str = "TransformConstraintVerify";

    async fn transform(
        &mut self,
        data: DataBlock,
    ) -> databend_common_exception::Result<Option<DataBlock>> {
        // TODO: only Check Constraint now
        let evaluator = Evaluator::new(&data, &self.func_ctx, &BUILTIN_FUNCTIONS);

        for (name, expr) in self.constraints.iter() {
            match evaluator.run(expr)? {
                Value::Scalar(scalar) => {
                    if !matches!(scalar.as_boolean(), Some(true)) {
                        return Err(ErrorCode::ConstraintError(format!("CHECK constraint failed on table {} with constraint {name} expression {}", self.table_name, expr.sql_display())));
                    }
                }
                Value::Column(column) => {
                    if column
                        .iter()
                        .any(|scalar| !matches!(scalar.as_boolean(), Some(true)))
                    {
                        return Err(ErrorCode::ConstraintError(format!("CHECK constraint failed on table {} with constraint {name} expression {}", self.table_name, expr.sql_display())));
                    }
                }
            }
        }

        Ok(Some(data))
    }
}
