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

use databend_common_expression::ColumnBuilder;
use databend_common_expression::DataBlock;
use databend_common_expression::Evaluator;
use databend_common_expression::Expr;
use databend_common_expression::FunctionContext;
use databend_common_expression::RemoteDefaultExpr;
use databend_common_expression::Scalar;
use databend_common_expression::TableSchemaRef;
use databend_common_expression::Value;
use databend_common_functions::BUILTIN_FUNCTIONS;
use databend_common_storage::FileParseError;

enum DefaultExpr {
    Constant(Scalar),
    Expr(Expr),
    Seq,
}

pub struct DefaultExprEvaluator {
    block: DataBlock,
    schema: TableSchemaRef,
    func_ctx: FunctionContext,
    default_exprs: Vec<DefaultExpr>,
}

impl DefaultExprEvaluator {
    pub fn new(
        remote_default_expr: Vec<RemoteDefaultExpr>,
        func_ctx: FunctionContext,
        schema: TableSchemaRef,
    ) -> Self {
        let mut default_exprs = Vec::with_capacity(remote_default_expr.len());
        for e in remote_default_expr.iter() {
            match e {
                RemoteDefaultExpr::RemoteExpr(remote_expr) => {
                    let expr = remote_expr.as_expr(&BUILTIN_FUNCTIONS);
                    if let Expr::Constant { scalar, .. } = expr {
                        default_exprs.push(DefaultExpr::Constant(scalar.clone()));
                    } else {
                        default_exprs.push(DefaultExpr::Expr(expr));
                    }
                }
                RemoteDefaultExpr::Sequence(_) => default_exprs.push(DefaultExpr::Seq),
            };
        }
        Self {
            default_exprs,
            block: DataBlock::new(vec![], 1),
            func_ctx,
            schema,
        }
    }

    pub fn push_default_value(
        &self,
        column_builder: &mut ColumnBuilder,
        column_index: usize,
    ) -> std::result::Result<(), FileParseError> {
        match &self.default_exprs[column_index] {
            DefaultExpr::Constant(s) => column_builder.push(s.as_ref()),
            DefaultExpr::Expr(expr) => {
                let evaluator = Evaluator::new(&self.block, &self.func_ctx, &BUILTIN_FUNCTIONS);
                let value = evaluator
                    .run(expr)
                    .map_err(|e| FileParseError::Unexpected {
                        message: format!("get error when eval default value: {}", e.message()),
                    })?;
                match value {
                    Value::Scalar(s) => {
                        column_builder.push(s.as_ref());
                    }
                    Value::Column(c) => {
                        let v = unsafe { c.index_unchecked(0) };
                        column_builder.push(v);
                    }
                }
            }
            DefaultExpr::Seq => {
                let field = self.schema.field(column_index);
                return Err(FileParseError::ColumnMissingError {
                    column_index,
                    column_name: field.name().to_string(),
                    column_type: field.data_type().to_string(),
                });
            }
        }
        Ok(())
    }
}
