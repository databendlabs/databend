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
use std::sync::Arc;

use common_exception::Result;

use super::select::SelectList;
use crate::binder::ColumnBindingBuilder;
use crate::optimizer::SExpr;
use crate::plans::BoundColumnRef;
use crate::plans::EvalScalar;
use crate::plans::Lambda;
use crate::plans::LambdaFunc;
use crate::plans::ScalarExpr;
use crate::plans::ScalarItem;
use crate::plans::VisitorMut;
use crate::BindContext;
use crate::Binder;
use crate::MetadataRef;
use crate::Visibility;

#[derive(Default, Clone, PartialEq, Eq, Debug)]
pub struct LambdaInfo {
    /// Arguments of lambda functions
    pub lambda_arguments: Vec<ScalarItem>,
    /// Lambda functions
    pub lambda_functions: Vec<ScalarItem>,
    /// Mapping: (lambda function display name) -> (derived column ref)
    /// This is used to generate column in projection.
    pub lambda_functions_map: HashMap<String, BoundColumnRef>,
}

pub(super) struct LambdaRewriter<'a> {
    pub bind_context: &'a mut BindContext,
    pub metadata: MetadataRef,
}

impl<'a> LambdaRewriter<'a> {
    pub fn new(bind_context: &'a mut BindContext, metadata: MetadataRef) -> Self {
        Self {
            bind_context,
            metadata,
        }
    }
}

impl<'a, 'b> VisitorMut<'a> for LambdaRewriter<'b> {
    fn visit_lambda_function(&mut self, lambda_func: &'a mut LambdaFunc) -> Result<()> {
        for (i, arg) in lambda_func.args.iter_mut().enumerate() {
            self.visit(arg)?;
            if let ScalarExpr::LambdaFunction(_) = arg {
                continue;
            }

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

            self.bind_context
                .lambda_info
                .lambda_arguments
                .push(ScalarItem {
                    index: new_column_ref.column.index,
                    scalar: arg.clone(),
                });

            *arg = new_column_ref.into();
        }

        let index = self.metadata.write().add_derived_column(
            lambda_func.display_name.clone(),
            (*lambda_func.return_type).clone(),
        );

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

        self.bind_context
            .lambda_info
            .lambda_functions_map
            .insert(lambda_func.display_name.clone(), replaced_column);
        self.bind_context
            .lambda_info
            .lambda_functions
            .push(ScalarItem {
                index,
                scalar: lambda_func.clone().into(),
            });

        Ok(())
    }
}

impl Binder {
    /// Analyze lambda functions in select clause, this will rewrite lambda functions.
    /// See [`LambdaRewriter`] for more details.
    pub(crate) fn analyze_lambda(
        &mut self,
        bind_context: &mut BindContext,
        select_list: &mut SelectList,
    ) -> Result<()> {
        for item in select_list.items.iter_mut() {
            let mut rewriter = LambdaRewriter::new(bind_context, self.metadata.clone());
            rewriter.visit(&mut item.scalar)?;
        }

        Ok(())
    }

    #[async_backtrace::framed]
    pub async fn bind_lambda(
        &mut self,
        bind_context: &mut BindContext,
        child: SExpr,
    ) -> Result<SExpr> {
        let lambda_info = &bind_context.lambda_info;
        if lambda_info.lambda_functions.is_empty() {
            return Ok(child);
        }

        let mut new_expr = child;
        if !lambda_info.lambda_arguments.is_empty() {
            let mut scalar_items = lambda_info.lambda_arguments.clone();
            scalar_items.sort_by_key(|item| item.index);
            let eval_scalar = EvalScalar {
                items: scalar_items,
            };
            new_expr = SExpr::create_unary(Arc::new(eval_scalar.into()), Arc::new(new_expr));
        }

        let lambda_plan = Lambda {
            items: lambda_info.lambda_functions.clone(),
        };
        new_expr = SExpr::create_unary(Arc::new(lambda_plan.into()), Arc::new(new_expr));

        Ok(new_expr)
    }
}
