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
use databend_common_expression::types::DataType;
use databend_common_expression::DataBlock;
use databend_common_expression::Evaluator;
use databend_common_expression::Expr;
use databend_common_expression::FieldDefaultExpr;
use databend_common_expression::FunctionContext;
use databend_common_expression::Scalar;
use databend_common_functions::BUILTIN_FUNCTIONS;

use crate::parse_default_expr_string;

enum DefaultValueParsed {
    Const(Scalar),
    Expr(Expr),
}

pub struct FieldDefaultExprEvaluator {
    fields: Vec<DefaultValueParsed>,
    func_ctx: FunctionContext,
}

impl FieldDefaultExprEvaluator {
    pub fn try_create(
        ctx: &Arc<dyn TableContext>,
        input_fields: Vec<FieldDefaultExpr>,
    ) -> Result<Self> {
        let mut func_ctx = ctx.get_function_context()?;
        func_ctx.force_scalar = true;
        let mut fields = Vec::with_capacity(input_fields.len());
        for f in input_fields {
            let value = match f {
                FieldDefaultExpr::Expr(s) => {
                    let expr = parse_default_expr_string(ctx, &s)?.0.as_expr()?;
                    let expr = expr.project_column_ref::<usize>(|_col| {
                        unreachable!("default value expr should not contain column ref")
                    });
                    DefaultValueParsed::Expr(expr)
                }
                FieldDefaultExpr::Const(s) => DefaultValueParsed::Const(s),
            };
            fields.push(value)
        }
        Ok(Self { fields, func_ctx })
    }

    pub fn get_scalar(&self, field_index: usize) -> Result<Scalar> {
        match &self.fields[field_index] {
            DefaultValueParsed::Const(s) => Ok(s.clone()),
            DefaultValueParsed::Expr(expr) => {
                let input = DataBlock::empty();
                let evaluator = Evaluator::new(&input, &self.func_ctx, &BUILTIN_FUNCTIONS);
                Ok(evaluator.run(expr)?.into_scalar().unwrap())
            }
        }
    }

    pub fn get_expr(&self, field_index: usize, data_type: DataType) -> Expr {
        match &self.fields[field_index] {
            DefaultValueParsed::Const(s) => Expr::Constant {
                span: None,
                scalar: s.clone(),
                data_type,
            },
            DefaultValueParsed::Expr(expr) => expr.clone(),
        }
    }
}
