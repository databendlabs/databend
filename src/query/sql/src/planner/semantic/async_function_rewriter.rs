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

use databend_common_async_functions::AsyncFunctionCall;
use databend_common_exception::Result;

use crate::optimizer::SExpr;
use crate::plans::AsyncFunction;
use crate::plans::BoundColumnRef;
use crate::plans::RelOperator;
use crate::ColumnBindingBuilder;
use crate::IndexType;
use crate::ScalarExpr;
use crate::Visibility;

pub struct AsyncFunctionRewriter {
    async_functions: Vec<(IndexType, AsyncFunctionCall)>,
}

impl AsyncFunctionRewriter {
    pub(crate) fn new() -> Self {
        Self {
            async_functions: Default::default(),
        }
    }

    pub(crate) fn rewrite(&mut self, s_expr: &SExpr) -> Result<SExpr> {
        let s_expr = s_expr.clone();

        match (*s_expr.plan).clone() {
            RelOperator::EvalScalar(mut plan) => {
                for item in &mut plan.items {
                    if let ScalarExpr::AsyncFunctionCall(async_func) = &item.scalar {
                        self.async_functions.push((item.index, async_func.clone()));
                        // Generate a ColumnBinding for each async function
                        let column = ColumnBindingBuilder::new(
                            async_func.display_name.clone(),
                            item.index,
                            async_func.return_type.clone(),
                            Visibility::Visible,
                        )
                        .build();

                        item.scalar = ScalarExpr::BoundColumnRef(BoundColumnRef {
                            span: async_func.span,
                            column,
                        });
                    }
                }

                if self.async_functions.is_empty() {
                    Ok(s_expr)
                } else {
                    let expr = self.create_async_function_expr(s_expr.children[0].clone());
                    Ok(SExpr::create_unary(Arc::new(plan.into()), expr))
                }
            }
            _ => Ok(s_expr),
        }
    }

    fn create_async_function_expr(&mut self, mut child_expr: Arc<SExpr>) -> Arc<SExpr> {
        for (index, func) in &self.async_functions {
            let async_func = AsyncFunction {
                func_name: func.func_name.clone(),
                arguments: func.arguments.clone(),
                display_name: func.display_name.clone(),
                return_type: func.return_type.as_ref().clone(),
                index: *index,
            };

            child_expr = Arc::new(SExpr::create_unary(Arc::new(async_func.into()), child_expr))
        }

        child_expr
    }
}
