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

use std::sync::Arc;

use async_recursion::async_recursion;
use common_ast::ast::Expr;
use common_ast::ast::Join;
use common_ast::ast::JoinCondition;
use common_ast::ast::JoinOperator;
use common_datavalues::type_coercion::merge_types;
use common_exception::ErrorCode;
use common_exception::Result;

use crate::sessions::QueryContext;
use crate::sql::binder::scalar_common::split_conjunctions;
use crate::sql::binder::scalar_common::split_equivalent_predicate;
use crate::sql::binder::scalar_common::wrap_cast_if_needed;
use crate::sql::optimizer::ColumnSet;
use crate::sql::optimizer::SExpr;
use crate::sql::planner::binder::scalar::ScalarBinder;
use crate::sql::planner::binder::Binder;
use crate::sql::plans::FilterPlan;
use crate::sql::plans::LogicalInnerJoin;
use crate::sql::plans::Scalar;
use crate::sql::plans::ScalarExpr;
use crate::sql::BindContext;

impl<'a> Binder {
    #[async_recursion]
    pub(super) async fn bind_join(
        &mut self,
        bind_context: &BindContext,
        join: &Join<'a>,
    ) -> Result<(SExpr, BindContext)> {
        let (left_child, left_context) =
            self.bind_table_reference(bind_context, &join.left).await?;
        let (right_child, right_context) = self
            .bind_table_reference(&left_context, &join.right)
            .await?;

        check_join_table(&left_context, &right_context)?;

        let mut bind_context = BindContext::new();
        for column in left_context.all_column_bindings() {
            bind_context.add_column_binding(column.clone());
        }
        for column in right_context.all_column_bindings() {
            bind_context.add_column_binding(column.clone());
        }

        let mut left_join_conditions: Vec<Scalar> = vec![];
        let mut right_join_conditions: Vec<Scalar> = vec![];
        let mut other_conditions: Vec<Scalar> = vec![];
        let join_condition_resolver = JoinConditionResolver::new(
            self.ctx.clone(),
            &left_context,
            &right_context,
            &bind_context,
            &join.condition,
        );
        join_condition_resolver
            .resolve(
                &mut left_join_conditions,
                &mut right_join_conditions,
                &mut other_conditions,
            )
            .await?;

        let mut s_expr = match &join.op {
            JoinOperator::Inner => self.bind_inner_join(
                left_join_conditions,
                right_join_conditions,
                left_child,
                right_child,
            ),
            JoinOperator::LeftOuter => Err(ErrorCode::UnImplement(
                "Unsupported join type: LEFT OUTER JOIN",
            )),
            JoinOperator::RightOuter => Err(ErrorCode::UnImplement(
                "Unsupported join type: RIGHT OUTER JOIN",
            )),
            JoinOperator::FullOuter => Err(ErrorCode::UnImplement(
                "Unsupported join type: FULL OUTER JOIN",
            )),
            JoinOperator::CrossJoin => {
                Err(ErrorCode::UnImplement("Unsupported join type: CROSS JOIN"))
            }
        }?;

        if !other_conditions.is_empty() {
            let filter_plan = FilterPlan {
                predicates: other_conditions,
                is_having: false,
            };
            s_expr = SExpr::create_unary(filter_plan.into(), s_expr);
        }

        Ok((s_expr, bind_context))
    }

    fn bind_inner_join(
        &mut self,
        left_conditions: Vec<Scalar>,
        right_conditions: Vec<Scalar>,
        left_child: SExpr,
        right_child: SExpr,
    ) -> Result<SExpr> {
        let inner_join = LogicalInnerJoin {
            left_conditions,
            right_conditions,
        };
        let expr = SExpr::create_binary(inner_join.into(), left_child, right_child);

        Ok(expr)
    }
}

pub fn check_join_table(left_context: &BindContext, right_context: &BindContext) -> Result<()> {
    let left_column_bindings = left_context.all_column_bindings();
    let left_table_name = if left_column_bindings.is_empty() {
        None
    } else {
        left_column_bindings[0].table_name.as_ref()
    };

    let right_column_bindings = right_context.all_column_bindings();
    let right_table_name = if right_column_bindings.is_empty() {
        None
    } else {
        right_column_bindings[0].table_name.as_ref()
    };

    if let Some(left) = left_table_name {
        if let Some(right) = right_table_name {
            if left.eq(right) {
                return Err(ErrorCode::SemanticError(format!(
                    "Duplicated table name {} in the same FROM clause",
                    left
                )));
            }
        }
    }
    Ok(())
}

struct JoinConditionResolver<'a> {
    ctx: Arc<QueryContext>,

    left_context: &'a BindContext,
    right_context: &'a BindContext,
    join_context: &'a BindContext,
    join_condition: &'a JoinCondition<'a>,
}

impl<'a> JoinConditionResolver<'a> {
    pub fn new(
        ctx: Arc<QueryContext>,
        left_context: &'a BindContext,
        right_context: &'a BindContext,
        join_context: &'a BindContext,
        join_condition: &'a JoinCondition<'a>,
    ) -> Self {
        Self {
            ctx,
            left_context,
            right_context,
            join_context,
            join_condition,
        }
    }

    pub async fn resolve(
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
                )
                .await?;
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

    async fn resolve_on(
        &self,
        condition: &Expr<'a>,
        left_join_conditions: &mut Vec<Scalar>,
        right_join_conditions: &mut Vec<Scalar>,
        other_join_conditions: &mut Vec<Scalar>,
    ) -> Result<()> {
        let scalar_binder = ScalarBinder::new(self.join_context, self.ctx.clone());
        let (scalar, _) = scalar_binder.bind_expr(condition).await?;
        let conjunctions = split_conjunctions(&scalar);

        for expr in conjunctions.iter() {
            self.resolve_predicate(
                expr,
                left_join_conditions,
                right_join_conditions,
                other_join_conditions,
            )
            .await?;
        }
        Ok(())
    }

    async fn resolve_predicate(
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
        if let Some((mut left, mut right)) = split_equivalent_predicate(predicate) {
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

            // Bump types of left conditions and right conditions
            let left_type = left.data_type();
            let right_type = right.data_type();
            let least_super_type = merge_types(&left_type, &right_type)?;
            left = wrap_cast_if_needed(left, &least_super_type);
            right = wrap_cast_if_needed(right, &least_super_type);

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
