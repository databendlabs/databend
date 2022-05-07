// Copyright 2022 Datafuse Labs.
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

use async_recursion::async_recursion;
use common_ast::ast::Expr;
use common_ast::ast::Join;
use common_ast::ast::JoinCondition;
use common_ast::ast::JoinOperator;
use common_exception::ErrorCode;
use common_exception::Result;

use crate::sql::binder::scalar_common::split_conjunctions;
use crate::sql::binder::scalar_common::split_equivalent_predicate;
use crate::sql::optimizer::ColumnSet;
use crate::sql::optimizer::SExpr;
use crate::sql::planner::binder::scalar::ScalarBinder;
use crate::sql::planner::binder::Binder;
use crate::sql::plans::FilterPlan;
use crate::sql::plans::LogicalInnerJoin;
use crate::sql::plans::Scalar;
use crate::sql::plans::ScalarExpr;
use crate::sql::BindContext;

impl Binder {
    #[async_recursion]
    pub(super) async fn bind_join(
        &mut self,
        bind_context: &BindContext,
        join: &Join,
    ) -> Result<BindContext> {
        let left_child = self.bind_table_reference(&join.left, bind_context).await?;
        let right_child = self.bind_table_reference(&join.right, bind_context).await?;

        let mut bind_context = BindContext::new();
        for column in left_child.all_column_bindings() {
            bind_context.add_column_binding(column.clone());
        }
        for column in right_child.all_column_bindings() {
            bind_context.add_column_binding(column.clone());
        }

        let mut left_join_conditions: Vec<Scalar> = vec![];
        let mut right_join_conditions: Vec<Scalar> = vec![];
        let mut other_conditions: Vec<Scalar> = vec![];
        let join_condition_resolver =
            JoinConditionResolver::new(&left_child, &right_child, &bind_context, &join.condition);
        join_condition_resolver.resolve(
            &mut left_join_conditions,
            &mut right_join_conditions,
            &mut other_conditions,
        )?;

        match &join.op {
            JoinOperator::Inner => {
                bind_context = self.bind_inner_join(
                    left_join_conditions,
                    right_join_conditions,
                    bind_context,
                    left_child.expression.unwrap(),
                    right_child.expression.unwrap(),
                )?;
            }
            JoinOperator::LeftOuter => {
                return Err(ErrorCode::UnImplement(
                    "Unsupported join type: LEFT OUTER JOIN",
                ));
            }
            JoinOperator::RightOuter => {
                return Err(ErrorCode::UnImplement(
                    "Unsupported join type: RIGHT OUTER JOIN",
                ));
            }
            JoinOperator::FullOuter => {
                return Err(ErrorCode::UnImplement(
                    "Unsupported join type: FULL OUTER JOIN",
                ));
            }
            JoinOperator::CrossJoin => {
                return Err(ErrorCode::UnImplement("Unsupported join type: CROSS JOIN"));
            }
        }

        if !other_conditions.is_empty() {
            let filter_plan = FilterPlan {
                predicates: other_conditions,
                is_having: false,
            };
            let new_expr =
                SExpr::create_unary(filter_plan.into(), bind_context.expression.clone().unwrap());
            bind_context.expression = Some(new_expr);
        }

        Ok(bind_context)
    }

    fn bind_inner_join(
        &mut self,
        left_conditions: Vec<Scalar>,
        right_conditions: Vec<Scalar>,
        mut bind_context: BindContext,
        left_child: SExpr,
        right_child: SExpr,
    ) -> Result<BindContext> {
        let inner_join = LogicalInnerJoin {
            left_conditions,
            right_conditions,
        };
        let expr = SExpr::create_binary(inner_join.into(), left_child, right_child);
        bind_context.expression = Some(expr);

        Ok(bind_context)
    }
}

struct JoinConditionResolver<'a> {
    left_context: &'a BindContext,
    right_context: &'a BindContext,
    join_context: &'a BindContext,
    join_condition: &'a JoinCondition,
}

impl<'a> JoinConditionResolver<'a> {
    pub fn new(
        left_context: &'a BindContext,
        right_context: &'a BindContext,
        join_context: &'a BindContext,
        join_condition: &'a JoinCondition,
    ) -> Self {
        Self {
            left_context,
            right_context,
            join_context,
            join_condition,
        }
    }

    pub fn resolve(
        &self,
        left_join_conditions: &mut Vec<Scalar>,
        right_join_conditions: &mut Vec<Scalar>,
        other_join_conditions: &mut Vec<Scalar>,
    ) -> Result<()> {
        match &self.join_condition {
            JoinCondition::On(cond) => {
                self.resolve_on(
                    cond,
                    left_join_conditions,
                    right_join_conditions,
                    other_join_conditions,
                )?;
            }
            JoinCondition::Using(_) => {
                return Err(ErrorCode::UnImplement("USING clause is not supported yet. Please specify join condition with ON clause."));
            }
            JoinCondition::Natural => {
                return Err(ErrorCode::UnImplement("NATURAL JOIN is not supported yet. Please specify join condition with ON clause."));
            }
            JoinCondition::None => {
                return Err(ErrorCode::UnImplement("JOIN without condition is not supported yet. Please specify join condition with ON clause."));
            }
        }
        Ok(())
    }

    fn resolve_on(
        &self,
        condition: &Expr,
        left_join_conditions: &mut Vec<Scalar>,
        right_join_conditions: &mut Vec<Scalar>,
        other_join_conditions: &mut Vec<Scalar>,
    ) -> Result<()> {
        let scalar_binder = ScalarBinder::new(self.join_context);
        let (scalar, _) = scalar_binder.bind_expr(condition)?;
        let conjunctions = split_conjunctions(&scalar);

        for expr in conjunctions.iter() {
            self.resolve_predicate(
                expr,
                left_join_conditions,
                right_join_conditions,
                other_join_conditions,
            )?;
        }
        Ok(())
    }

    fn resolve_predicate(
        &self,
        predicate: &Scalar,
        left_join_conditions: &mut Vec<Scalar>,
        right_join_conditions: &mut Vec<Scalar>,
        other_join_conditions: &mut Vec<Scalar>,
    ) -> Result<()> {
        // Given two tables: t1(a, b), t2(a, b)
        // A predicate can be regarded as an equi-predicate iff:
        //
        //   - The predicate is literally an equivalence expression, e.g. `t1.a = t2.a`
        //   - Each side of `=` only contains columns from one table and the both sides are disjoint.
        //     For example, `t1.a + t1.b = t2.a` is a valid one while `t1.a + t2.a = t2.b` isn't.
        //
        // Only equi-predicate can be exploited by common join algorithms(e.g. sort-merge join, hash join).
        // For the predicates that aren't equi-predicate, we will lift them as a `Filter` operator.
        if let Some((left, right)) = split_equivalent_predicate(predicate) {
            let left_used_columns = left.used_columns();
            let right_used_columns = right.used_columns();
            let left_columns: ColumnSet = self.left_context.all_column_bindings().iter().fold(
                ColumnSet::new(),
                |mut acc, v| {
                    acc.insert(v.index);
                    acc
                },
            );
            let right_columns: ColumnSet = self.right_context.all_column_bindings().iter().fold(
                ColumnSet::new(),
                |mut acc, v| {
                    acc.insert(v.index);
                    acc
                },
            );

            // TODO(leiysky): bump types of left conditions and right conditions
            if left_used_columns.is_subset(&left_columns)
                && right_used_columns.is_subset(&right_columns)
            {
                left_join_conditions.push(left);
                right_join_conditions.push(right);
            } else if left_used_columns.is_subset(&right_columns)
                && right_used_columns.is_subset(&left_columns)
            {
                left_join_conditions.push(right);
                right_join_conditions.push(left);
            }
        } else {
            other_join_conditions.push(predicate.clone());
        }
        Ok(())
    }
}
