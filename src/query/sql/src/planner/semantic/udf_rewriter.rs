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
use std::collections::VecDeque;
use std::sync::Arc;

use databend_common_exception::ErrorCode;
use databend_common_exception::Result;

use crate::optimizer::SExpr;
use crate::plans::walk_expr_mut;
use crate::plans::BoundColumnRef;
use crate::plans::EvalScalar;
use crate::plans::RelOperator;
use crate::plans::ScalarExpr;
use crate::plans::ScalarItem;
use crate::plans::UDFCall;
use crate::plans::Udf;
use crate::plans::VisitorMut;
use crate::ColumnBindingBuilder;
use crate::IndexType;
use crate::MetadataRef;
use crate::Visibility;

pub(crate) struct UdfRewriter {
    metadata: MetadataRef,
    /// Arguments of udf functions
    udf_arguments: VecDeque<Vec<ScalarItem>>,
    /// Udf functions
    udf_functions: VecDeque<Vec<ScalarItem>>,
    /// Mapping: (udf function display name) -> (derived column ref)
    /// This is used to replace udf with a derived column.
    udf_functions_map: HashMap<String, BoundColumnRef>,
    /// Mapping: (udf function display name) -> (derived index)
    /// This is used to reuse already generated derived columns
    udf_functions_index_map: HashMap<String, IndexType>,
    script_udf: bool,
}

impl UdfRewriter {
    pub(crate) fn new(metadata: MetadataRef, script_udf: bool) -> Self {
        Self {
            metadata,
            udf_arguments: Default::default(),
            udf_functions: Default::default(),
            udf_functions_map: Default::default(),
            udf_functions_index_map: Default::default(),
            script_udf,
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

        // Rewrite Udf and its arguments as derived column.
        match (*s_expr.plan).clone() {
            RelOperator::EvalScalar(mut plan) => {
                for item in &plan.items {
                    // The index of Udf item can be reused.
                    if let ScalarExpr::UDFCall(udf) = &item.scalar {
                        self.udf_functions_index_map
                            .insert(udf.display_name.clone(), item.index);
                    }
                }
                for item in &mut plan.items {
                    self.visit(&mut item.scalar)?;
                }
                let child_expr = self.create_udf_expr(s_expr.children[0].clone());
                let new_expr = SExpr::create_unary(Arc::new(plan.into()), child_expr);
                Ok(new_expr)
            }
            RelOperator::Filter(mut plan) => {
                for scalar in &mut plan.predicates {
                    self.visit(scalar)?;
                }
                let child_expr = self.create_udf_expr(s_expr.children[0].clone());
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
                let child_expr = self.create_udf_expr(s_expr.children[0].clone());
                let new_expr = SExpr::create_unary(Arc::new(plan.into()), child_expr);
                Ok(new_expr)
            }
            _ => Ok(s_expr),
        }
    }

    fn create_udf_expr(&mut self, mut child_expr: Arc<SExpr>) -> Arc<SExpr> {
        while !self.udf_functions.is_empty() {
            if !self.udf_arguments.is_empty() {
                // Add an EvalScalar for the arguments of Udf.
                let mut scalar_items = self.udf_arguments.pop_front().unwrap();
                scalar_items.sort_by_key(|item| item.index);
                let eval_scalar = EvalScalar {
                    items: scalar_items,
                };

                child_expr = Arc::new(SExpr::create_unary(
                    Arc::new(eval_scalar.into()),
                    child_expr,
                ));
            }

            let udf_functions = self.udf_functions.pop_front().unwrap();
            let udf_plan = Udf {
                items: udf_functions,
                script_udf: self.script_udf,
            };
            child_expr = Arc::new(SExpr::create_unary(Arc::new(udf_plan.into()), child_expr));
        }
        child_expr
    }
}

impl<'a> VisitorMut<'a> for UdfRewriter {
    fn visit(&mut self, expr: &'a mut ScalarExpr) -> Result<()> {
        walk_expr_mut(self, expr)?;
        // replace udf with derived column
        if let ScalarExpr::UDFCall(udf) = expr {
            if let Some(column_ref) = self.udf_functions_map.get(&udf.display_name) {
                *expr = ScalarExpr::BoundColumnRef(column_ref.clone());
            } else if udf.udf_type.match_type(self.script_udf) {
                return Err(ErrorCode::Internal("Rewrite udf function failed"));
            }
        }
        Ok(())
    }

    fn visit_udf_call(&mut self, udf: &'a mut UDFCall) -> Result<()> {
        if !udf.udf_type.match_type(self.script_udf) {
            return Ok(());
        }

        let mut udf_arguments = Vec::with_capacity(udf.arguments.len());

        for (i, arg) in udf.arguments.iter_mut().enumerate() {
            self.visit(arg)?;

            let new_column_ref = if let ScalarExpr::BoundColumnRef(ref column_ref) = &arg {
                column_ref.clone()
            } else {
                let name = format!("{}_arg_{}", &udf.display_name, i);
                let index = self.metadata.write().add_derived_column(
                    name.clone(),
                    arg.data_type()?,
                    Some(arg.clone()),
                );

                // Generate a ColumnBinding for each argument of udf function
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

            udf_arguments.push(ScalarItem {
                index: new_column_ref.column.index,
                scalar: arg.clone(),
            });
            *arg = new_column_ref.into();
        }

        self.udf_arguments.push_back(udf_arguments);

        let index = match self.udf_functions_index_map.get(&udf.display_name) {
            Some(index) => *index,
            None => self.metadata.write().add_derived_column(
                udf.display_name.clone(),
                (*udf.return_type).clone(),
                Some(ScalarExpr::UDFCall(udf.clone())),
            ),
        };

        // Generate a ColumnBinding for the udf function
        let column = ColumnBindingBuilder::new(
            udf.display_name.clone(),
            index,
            udf.return_type.clone(),
            Visibility::Visible,
        )
        .build();

        let replaced_column = BoundColumnRef {
            span: udf.span,
            column,
        };

        self.udf_functions_map
            .insert(udf.display_name.clone(), replaced_column);
        self.udf_functions.push_back(vec![ScalarItem {
            index,
            scalar: udf.clone().into(),
        }]);

        Ok(())
    }
}
