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

use common_exception::ErrorCode;
use common_exception::Result;

use crate::optimizer::SExpr;
use crate::plans::walk_expr_mut;
use crate::plans::BoundColumnRef;
use crate::plans::EvalScalar;
use crate::plans::Lambda;
use crate::plans::LambdaFunc;
use crate::plans::RelOperator;
use crate::plans::ScalarExpr;
use crate::plans::ScalarItem;
use crate::plans::VisitorMut;
use crate::ColumnBindingBuilder;
use crate::IndexType;
use crate::MetadataRef;
use crate::Visibility;

#[derive(Default, Clone, PartialEq, Eq, Debug)]
pub struct LambdaInfo {
    /// Arguments of lambda functions
    pub lambda_arguments: Vec<ScalarItem>,
    /// Lambda functions
    pub lambda_functions: Vec<ScalarItem>,
    /// Mapping: (lambda function display name) -> (derived column ref)
    /// This is used to replace lambda with a derived column.
    pub lambda_functions_map: HashMap<String, BoundColumnRef>,
    /// Mapping: (lambda function display name) -> (derived index)
    /// This is used to reuse already generated derived columns
    pub lambda_functions_index_map: HashMap<String, IndexType>,
}

pub(crate) struct LambdaRewriter {
    lambda_info: LambdaInfo,
    metadata: MetadataRef,
}

impl LambdaRewriter {
    pub(crate) fn new(metadata: MetadataRef) -> Self {
        Self {
            lambda_info: Default::default(),
            metadata,
        }
    }

    pub(crate) fn rewrite(&mut self, s_expr: &SExpr) -> Result<SExpr> {
        let mut s_expr = s_expr.clone();
        if !s_expr.children.is_empty() {
            let mut children = Vec::with_capacity(s_expr.children.len());
            for child in s_expr.children.iter() {
                children.push(Arc::new(self.rewrite(child)?));
            }
            s_expr.children = children;
        }

        // Rewrite Lambda and its arguments as derived column.
        match (*s_expr.plan).clone() {
            RelOperator::EvalScalar(mut plan) => {
                for item in &plan.items {
                    // The index of Lambda item can be reused.
                    if let ScalarExpr::LambdaFunction(lambda) = &item.scalar {
                        self.lambda_info
                            .lambda_functions_index_map
                            .insert(lambda.display_name.clone(), item.index);
                    }
                }
                for item in &mut plan.items {
                    self.visit(&mut item.scalar)?;
                }
                let child_expr = self.create_lambda_expr(s_expr.children[0].clone());
                let new_expr = SExpr::create_unary(Arc::new(plan.into()), child_expr);
                Ok(new_expr)
            }
            RelOperator::Filter(mut plan) => {
                for scalar in &mut plan.predicates {
                    self.visit(scalar)?;
                }
                let child_expr = self.create_lambda_expr(s_expr.children[0].clone());
                let new_expr = SExpr::create_unary(Arc::new(plan.into()), child_expr);
                Ok(new_expr)
            }
            _ => Ok(s_expr),
        }
    }

    fn create_lambda_expr(&mut self, mut child_expr: Arc<SExpr>) -> Arc<SExpr> {
        let lambda_info = &mut self.lambda_info;
        if !lambda_info.lambda_functions.is_empty() {
            if !lambda_info.lambda_arguments.is_empty() {
                // Add an EvalScalar for the arguments of Lambda.
                let mut scalar_items = mem::take(&mut lambda_info.lambda_arguments);
                scalar_items.sort_by_key(|item| item.index);
                let eval_scalar = EvalScalar {
                    items: scalar_items,
                };
                child_expr = Arc::new(SExpr::create_unary(
                    Arc::new(eval_scalar.into()),
                    child_expr,
                ));
            }

            let lambda_plan = Lambda {
                items: mem::take(&mut lambda_info.lambda_functions),
            };
            Arc::new(SExpr::create_unary(
                Arc::new(lambda_plan.into()),
                child_expr,
            ))
        } else {
            child_expr
        }
    }
}

impl<'a> VisitorMut<'a> for LambdaRewriter {
    fn visit(&mut self, expr: &'a mut ScalarExpr) -> Result<()> {
        walk_expr_mut(self, expr)?;
        // replace lambda with derived column
        if let ScalarExpr::LambdaFunction(lambda) = expr {
            if let Some(column_ref) = self
                .lambda_info
                .lambda_functions_map
                .get(&lambda.display_name)
            {
                *expr = ScalarExpr::BoundColumnRef(column_ref.clone());
            } else {
                return Err(ErrorCode::Internal("Rewrite lambda function failed"));
            }
        }
        Ok(())
    }

    fn visit_lambda_function(&mut self, lambda_func: &'a mut LambdaFunc) -> Result<()> {
        for (i, arg) in lambda_func.args.iter_mut().enumerate() {
            let arg_is_lambda = matches!(arg, ScalarExpr::LambdaFunction(_));
            self.visit(arg)?;

            let new_column_ref = if let ScalarExpr::BoundColumnRef(ref column_ref) = &arg {
                column_ref.clone()
            } else {
                let name = format!("{}_arg_{}", &lambda_func.display_name, i);
                let index = self
                    .metadata
                    .write()
                    .add_derived_column(name.clone(), arg.data_type()?);

                // Generate a ColumnBinding for each argument of lambda function
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

            if !arg_is_lambda {
                self.lambda_info.lambda_arguments.push(ScalarItem {
                    index: new_column_ref.column.index,
                    scalar: arg.clone(),
                });
            }
            *arg = new_column_ref.into();
        }

        let index = match self
            .lambda_info
            .lambda_functions_index_map
            .get(&lambda_func.display_name)
        {
            Some(index) => *index,
            None => self.metadata.write().add_derived_column(
                lambda_func.display_name.clone(),
                (*lambda_func.return_type).clone(),
            ),
        };

        // Generate a ColumnBinding for the lambda function
        let column = ColumnBindingBuilder::new(
            lambda_func.display_name.clone(),
            index,
            lambda_func.return_type.clone(),
            Visibility::Visible,
        )
        .build();

        let replaced_column = BoundColumnRef {
            span: lambda_func.span,
            column,
        };

        self.lambda_info
            .lambda_functions_map
            .insert(lambda_func.display_name.clone(), replaced_column);
        self.lambda_info.lambda_functions.push(ScalarItem {
            index,
            scalar: lambda_func.clone().into(),
        });

        Ok(())
    }
}
