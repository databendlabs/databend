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
use std::mem;
use std::sync::Arc;

use databend_common_exception::ErrorCode;
use databend_common_exception::Result;

use crate::optimizer::SExpr;
use crate::plans::walk_expr_mut;
use crate::plans::AsyncFunction;
use crate::plans::AsyncFunctionCall;
use crate::plans::BoundColumnRef;
use crate::plans::EvalScalar;
use crate::plans::RelOperator;
use crate::plans::ScalarExpr;
use crate::plans::ScalarItem;
use crate::plans::VisitorMut;
use crate::ColumnBindingBuilder;
use crate::IndexType;
use crate::MetadataRef;
use crate::Visibility;

pub(crate) struct AsyncFunctionRewriter {
    metadata: MetadataRef,
    /// Arguments of async functions
    async_func_arguments: Vec<ScalarItem>,
    /// Async functions
    async_functions: Vec<ScalarItem>,
    /// Mapping: (async function display name) -> (derived column ref)
    /// This is used to replace async function with a derived column.
    async_functions_map: HashMap<String, BoundColumnRef>,
    /// Mapping: (async function display name) -> (derived index)
    /// This is used to reuse already generated derived columns
    async_functions_index_map: HashMap<String, IndexType>,
}

impl AsyncFunctionRewriter {
    pub(crate) fn new(metadata: MetadataRef) -> Self {
        Self {
            metadata,
            async_func_arguments: Default::default(),
            async_functions: Default::default(),
            async_functions_map: Default::default(),
            async_functions_index_map: Default::default(),
        }
    }

    #[recursive::recursive]
    pub(crate) fn rewrite(&mut self, s_expr: &SExpr) -> Result<SExpr> {
        let mut s_expr = s_expr.clone();
        if !s_expr.children.is_empty() {
            let mut children = Vec::with_capacity(s_expr.children.len());
            for child in s_expr.children.iter() {
                children.push(Arc::new(self.rewrite(child)?));
            }
            s_expr.children = children;
        }

        // Rewrite async function and its arguments as derived column.
        match (*s_expr.plan).clone() {
            RelOperator::EvalScalar(mut plan) => {
                for item in &plan.items {
                    // The index of async function item can be reused.
                    if let ScalarExpr::AsyncFunctionCall(async_func) = &item.scalar {
                        self.async_functions_index_map
                            .insert(async_func.display_name.clone(), item.index);
                    }
                }
                for item in &mut plan.items {
                    self.visit(&mut item.scalar)?;
                }
                let child_expr = self.create_async_func_expr(s_expr.children[0].clone());
                let new_expr = SExpr::create_unary(Arc::new(plan.into()), child_expr);
                Ok(new_expr)
            }
            RelOperator::Filter(mut plan) => {
                for scalar in &mut plan.predicates {
                    self.visit(scalar)?;
                }
                let child_expr = self.create_async_func_expr(s_expr.children[0].clone());
                let new_expr = SExpr::create_unary(Arc::new(plan.into()), child_expr);
                Ok(new_expr)
            }
            RelOperator::Mutation(mut plan) => {
                for matched_evaluator in plan.matched_evaluators.iter_mut() {
                    if let Some(condition) = matched_evaluator.condition.as_mut() {
                        self.visit(condition)?;
                    }
                    if let Some(update) = matched_evaluator.update.as_mut() {
                        for (_, scalar) in update.iter_mut() {
                            self.visit(scalar)?;
                        }
                    }
                }
                let child_expr = self.create_async_func_expr(s_expr.children[0].clone());
                let new_expr = SExpr::create_unary(Arc::new(plan.into()), child_expr);
                Ok(new_expr)
            }
            _ => Ok(s_expr),
        }
    }

    fn create_async_func_expr(&mut self, mut child_expr: Arc<SExpr>) -> Arc<SExpr> {
        if !self.async_func_arguments.is_empty() {
            // Add an EvalScalar for the arguments of async function.
            let mut scalar_items = mem::take(&mut self.async_func_arguments);
            scalar_items.sort_by_key(|item| item.index);
            let eval_scalar = EvalScalar {
                items: scalar_items,
            };

            child_expr = Arc::new(SExpr::create_unary(
                Arc::new(eval_scalar.into()),
                child_expr,
            ));
        }

        if !self.async_functions.is_empty() {
            let mut async_fun_items = mem::take(&mut self.async_functions);
            async_fun_items.sort_by_key(|item| item.index);
            let async_func_plan = AsyncFunction {
                items: async_fun_items,
            };
            child_expr = Arc::new(SExpr::create_unary(
                Arc::new(async_func_plan.into()),
                child_expr,
            ));
        }
        child_expr
    }
}

impl<'a> VisitorMut<'a> for AsyncFunctionRewriter {
    fn visit(&mut self, expr: &'a mut ScalarExpr) -> Result<()> {
        walk_expr_mut(self, expr)?;
        // replace udf with derived column
        if let ScalarExpr::AsyncFunctionCall(async_func) = expr {
            if let Some(column_ref) = self.async_functions_map.get(&async_func.display_name) {
                *expr = ScalarExpr::BoundColumnRef(column_ref.clone());
            } else {
                return Err(ErrorCode::Internal("Rewrite async function failed"));
            }
        }
        Ok(())
    }

    fn visit_async_function_call(&mut self, async_func: &'a mut AsyncFunctionCall) -> Result<()> {
        for (i, arg) in async_func.arguments.iter_mut().enumerate() {
            self.visit(arg)?;

            let new_column_ref = if let ScalarExpr::BoundColumnRef(ref column_ref) = &arg {
                column_ref.clone()
            } else {
                let name = format!("{}_arg_{}", &async_func.display_name, i);
                let index = self.metadata.write().add_derived_column(
                    name.clone(),
                    arg.data_type()?,
                    Some(arg.clone()),
                );

                // Generate a ColumnBinding for each argument of async function
                let column = ColumnBindingBuilder::new(
                    name,
                    index,
                    Box::new(arg.data_type()?),
                    Visibility::Visible,
                )
                .build();

                BoundColumnRef {
                    span: arg.span(),
                    column,
                }
            };

            self.async_func_arguments.push(ScalarItem {
                index: new_column_ref.column.index,
                scalar: arg.clone(),
            });
            *arg = new_column_ref.into();
        }

        let index = match self.async_functions_index_map.get(&async_func.display_name) {
            Some(index) => *index,
            None => self.metadata.write().add_derived_column(
                async_func.display_name.clone(),
                (*async_func.return_type).clone(),
                Some(async_func.clone().into()),
            ),
        };

        // Generate a ColumnBinding for the async function
        let column = ColumnBindingBuilder::new(
            async_func.display_name.clone(),
            index,
            async_func.return_type.clone(),
            Visibility::Visible,
        )
        .build();

        let replaced_column = BoundColumnRef {
            span: async_func.span,
            column,
        };

        self.async_functions_map
            .insert(async_func.display_name.clone(), replaced_column);
        self.async_functions.push(ScalarItem {
            index,
            scalar: async_func.clone().into(),
        });

        Ok(())
    }
}
