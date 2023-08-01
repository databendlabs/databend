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

use async_recursion::async_recursion;
use common_ast::ast::split_conjunctions_expr;
use common_ast::ast::split_equivalent_predicate_expr;
use common_ast::ast::Expr;
use common_ast::ast::JoinCondition;
use common_ast::ast::JoinOperator;
use common_catalog::table_context::TableContext;
use common_exception::ErrorCode;
use common_exception::Result;
use common_exception::Span;

use crate::binder::JoinPredicate;
use crate::binder::Visibility;
use crate::normalize_identifier;
use crate::optimizer::ColumnSet;
use crate::optimizer::RelExpr;
use crate::optimizer::SExpr;
use crate::planner::binder::scalar::ScalarBinder;
use crate::planner::binder::Binder;
use crate::planner::semantic::NameResolutionContext;
use crate::plans::BoundColumnRef;
use crate::plans::Filter;
use crate::plans::Join;
use crate::plans::JoinType;
use crate::plans::ScalarExpr;
use crate::BindContext;
use crate::IndexType;
use crate::MetadataRef;

pub struct JoinConditions {
    pub(crate) left_conditions: Vec<ScalarExpr>,
    pub(crate) right_conditions: Vec<ScalarExpr>,
    pub(crate) non_equi_conditions: Vec<ScalarExpr>,
    pub(crate) other_conditions: Vec<ScalarExpr>,
}

impl Binder {
    #[async_recursion]
    #[async_backtrace::framed]
    pub(crate) async fn bind_join(
        &mut self,
        bind_context: &BindContext,
        left_context: BindContext,
        right_context: BindContext,
        left_child: SExpr,
        right_child: SExpr,
        join: &common_ast::ast::Join,
    ) -> Result<(SExpr, BindContext)> {
        check_duplicate_join_tables(&left_context, &right_context)?;

        let mut bind_context = bind_context.replace();

        match &join.op {
            JoinOperator::LeftOuter | JoinOperator::RightOuter | JoinOperator::FullOuter
                if join.condition == JoinCondition::None =>
            {
                return Err(ErrorCode::SemanticError(
                    "outer join should contain join conditions".to_string(),
                ));
            }
            JoinOperator::CrossJoin if join.condition != JoinCondition::None => {
                return Err(ErrorCode::SemanticError(
                    "cross join should not contain join conditions".to_string(),
                ));
            }
            _ => (),
        };

        let mut left_join_conditions: Vec<ScalarExpr> = vec![];
        let mut right_join_conditions: Vec<ScalarExpr> = vec![];
        let mut non_equi_conditions: Vec<ScalarExpr> = vec![];
        let mut other_conditions: Vec<ScalarExpr> = vec![];
        let mut join_condition_resolver = JoinConditionResolver::new(
            self.ctx.clone(),
            &self.name_resolution_ctx,
            self.metadata.clone(),
            self.m_cte_bound_ctx.clone(),
            join.op.clone(),
            &left_context,
            &right_context,
            &mut bind_context,
            &join.condition,
        );
        join_condition_resolver
            .resolve(
                &mut left_join_conditions,
                &mut right_join_conditions,
                &mut non_equi_conditions,
                &mut other_conditions,
                &join.op,
            )
            .await?;

        let join_conditions = JoinConditions {
            left_conditions: left_join_conditions,
            right_conditions: right_join_conditions,
            non_equi_conditions,
            other_conditions,
        };
        let s_expr = match &join.op {
            JoinOperator::Inner => {
                self.bind_join_with_type(JoinType::Inner, join_conditions, left_child, right_child)
            }
            JoinOperator::LeftOuter => {
                self.bind_join_with_type(JoinType::Left, join_conditions, left_child, right_child)
            }
            JoinOperator::RightOuter => {
                self.bind_join_with_type(JoinType::Right, join_conditions, left_child, right_child)
            }
            JoinOperator::FullOuter => {
                self.bind_join_with_type(JoinType::Full, join_conditions, left_child, right_child)
            }
            JoinOperator::CrossJoin => {
                self.bind_join_with_type(JoinType::Cross, join_conditions, left_child, right_child)
            }
            JoinOperator::LeftSemi => {
                bind_context = left_context;
                self.bind_join_with_type(
                    JoinType::LeftSemi,
                    join_conditions,
                    left_child,
                    right_child,
                )
            }
            JoinOperator::RightSemi => {
                bind_context = right_context;
                self.bind_join_with_type(
                    JoinType::RightSemi,
                    join_conditions,
                    left_child,
                    right_child,
                )
            }
            JoinOperator::LeftAnti => {
                bind_context = left_context;
                self.bind_join_with_type(
                    JoinType::LeftAnti,
                    join_conditions,
                    left_child,
                    right_child,
                )
            }
            JoinOperator::RightAnti => {
                bind_context = right_context;
                self.bind_join_with_type(
                    JoinType::RightAnti,
                    join_conditions,
                    left_child,
                    right_child,
                )
            }
        }?;
        Ok((s_expr, bind_context))
    }

    pub fn bind_join_with_type(
        &mut self,
        join_type: JoinType,
        join_conditions: JoinConditions,
        mut left_child: SExpr,
        mut right_child: SExpr,
    ) -> Result<SExpr> {
        let left_conditions = join_conditions.left_conditions;
        let right_conditions = join_conditions.right_conditions;
        let mut non_equi_conditions = join_conditions.non_equi_conditions;
        let other_conditions = join_conditions.other_conditions;
        if join_type == JoinType::Cross
            && (!left_conditions.is_empty() || !right_conditions.is_empty())
        {
            return Err(ErrorCode::SemanticError(
                "Join conditions should be empty in cross join",
            ));
        }
        self.push_down_other_conditions(
            &mut left_child,
            &mut right_child,
            other_conditions,
            &mut non_equi_conditions,
        )?;
        let logical_join = Join {
            left_conditions,
            right_conditions,
            non_equi_conditions,
            join_type,
            marker_index: None,
            from_correlated_subquery: false,
            contain_runtime_filter: false,
        };
        Ok(SExpr::create_binary(
            Arc::new(logical_join.into()),
            Arc::new(left_child),
            Arc::new(right_child),
        ))
    }

    fn push_down_other_conditions(
        &self,
        left_child: &mut SExpr,
        right_child: &mut SExpr,
        other_conditions: Vec<ScalarExpr>,
        non_equi_conditions: &mut Vec<ScalarExpr>,
    ) -> Result<()> {
        if other_conditions.is_empty() {
            return Ok(());
        }
        let left_prop = RelExpr::with_s_expr(left_child).derive_relational_prop()?;
        let right_prop = RelExpr::with_s_expr(right_child).derive_relational_prop()?;

        let mut left_push_down = vec![];
        let mut right_push_down = vec![];
        let mut need_push_down = false;

        for predicate in other_conditions.iter() {
            let pred = JoinPredicate::new(predicate, &left_prop, &right_prop);
            match pred {
                JoinPredicate::Left(_) => {
                    need_push_down = true;
                    left_push_down.push(predicate.clone());
                }
                JoinPredicate::Right(_) => {
                    need_push_down = true;
                    right_push_down.push(predicate.clone());
                }
                _ => {
                    non_equi_conditions.push(predicate.clone());
                }
            }
        }

        if !need_push_down {
            return Ok(());
        }

        if !left_push_down.is_empty() {
            *left_child = SExpr::create_unary(
                Arc::new(
                    Filter {
                        predicates: left_push_down,
                        is_having: false,
                    }
                    .into(),
                ),
                Arc::new(left_child.clone()),
            );
        }

        if !right_push_down.is_empty() {
            *right_child = SExpr::create_unary(
                Arc::new(
                    Filter {
                        predicates: right_push_down,
                        is_having: false,
                    }
                    .into(),
                ),
                Arc::new(right_child.clone()),
            );
        }

        Ok(())
    }
}

// Wrap nullable for column binding depending on join type.
fn wrap_nullable_for_column(
    join_type: &JoinOperator,
    left_context: &BindContext,
    right_context: &BindContext,
    bind_context: &mut BindContext,
) {
    match join_type {
        JoinOperator::LeftOuter => {
            for column in left_context.all_column_bindings() {
                bind_context.add_column_binding(column.clone());
            }
            for column in right_context.all_column_bindings().iter() {
                let mut nullable_column = column.clone();
                nullable_column.data_type = Box::new(column.data_type.wrap_nullable());
                bind_context.add_column_binding(nullable_column);
            }
        }
        JoinOperator::RightOuter => {
            for column in left_context.all_column_bindings() {
                let mut nullable_column = column.clone();
                nullable_column.data_type = Box::new(column.data_type.wrap_nullable());
                bind_context.add_column_binding(nullable_column);
            }

            for column in right_context.all_column_bindings().iter() {
                bind_context.add_column_binding(column.clone());
            }
        }
        JoinOperator::FullOuter => {
            for column in left_context.all_column_bindings() {
                let mut nullable_column = column.clone();
                nullable_column.data_type = Box::new(column.data_type.wrap_nullable());
                bind_context.add_column_binding(nullable_column);
            }

            for column in right_context.all_column_bindings().iter() {
                let mut nullable_column = column.clone();
                nullable_column.data_type = Box::new(column.data_type.wrap_nullable());
                bind_context.add_column_binding(nullable_column);
            }
        }
        _ => {
            for column in left_context.all_column_bindings() {
                bind_context.add_column_binding(column.clone());
            }
            for column in right_context.all_column_bindings() {
                bind_context.add_column_binding(column.clone());
            }
        }
    }
}

pub fn check_duplicate_join_tables(
    left_context: &BindContext,
    right_context: &BindContext,
) -> Result<()> {
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
    ctx: Arc<dyn TableContext>,
    name_resolution_ctx: &'a NameResolutionContext,
    metadata: MetadataRef,
    m_cte_bound_ctx: HashMap<IndexType, BindContext>,
    join_op: JoinOperator,
    left_context: &'a BindContext,
    right_context: &'a BindContext,
    join_context: &'a mut BindContext,
    join_condition: &'a JoinCondition,
}

impl<'a> JoinConditionResolver<'a> {
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        ctx: Arc<dyn TableContext>,
        name_resolution_ctx: &'a NameResolutionContext,
        metadata: MetadataRef,
        m_cte_bound_ctx: HashMap<IndexType, BindContext>,
        join_op: JoinOperator,
        left_context: &'a BindContext,
        right_context: &'a BindContext,
        join_context: &'a mut BindContext,
        join_condition: &'a JoinCondition,
    ) -> Self {
        Self {
            ctx,
            name_resolution_ctx,
            metadata,
            m_cte_bound_ctx,
            join_op,
            left_context,
            right_context,
            join_context,
            join_condition,
        }
    }

    #[async_backtrace::framed]
    pub async fn resolve(
        &mut self,
        left_join_conditions: &mut Vec<ScalarExpr>,
        right_join_conditions: &mut Vec<ScalarExpr>,
        non_equi_conditions: &mut Vec<ScalarExpr>,
        other_join_conditions: &mut Vec<ScalarExpr>,
        join_op: &JoinOperator,
    ) -> Result<()> {
        match &self.join_condition {
            JoinCondition::On(cond) => {
                self.resolve_on(
                    cond,
                    left_join_conditions,
                    right_join_conditions,
                    non_equi_conditions,
                    other_join_conditions,
                )
                .await?;
            }
            JoinCondition::Using(identifiers) => {
                let using_columns = identifiers
                    .iter()
                    .map(|ident| {
                        (
                            ident.span,
                            normalize_identifier(ident, self.name_resolution_ctx).name,
                        )
                    })
                    .collect();
                self.resolve_using(
                    using_columns,
                    left_join_conditions,
                    right_join_conditions,
                    join_op,
                )
                .await?;
            }
            JoinCondition::Natural => {
                // NATURAL is a shorthand form of USING: it forms a USING list consisting of all column names that appear in both input tables
                // As with USING, these columns appear only once in the output table
                // Todo(xudong963) If there are no common column names, NATURAL JOIN behaves like JOIN ... ON TRUE, producing a cross-product join.
                let mut using_columns = vec![];
                // Find common columns in both input tables
                self.find_using_columns(&mut using_columns)?;
                self.resolve_using(
                    using_columns,
                    left_join_conditions,
                    right_join_conditions,
                    join_op,
                )
                .await?
            }
            JoinCondition::None => {
                wrap_nullable_for_column(
                    &self.join_op,
                    self.left_context,
                    self.right_context,
                    self.join_context,
                );
            }
        }
        Ok(())
    }

    #[async_backtrace::framed]
    async fn resolve_on(
        &mut self,
        condition: &Expr,
        left_join_conditions: &mut Vec<ScalarExpr>,
        right_join_conditions: &mut Vec<ScalarExpr>,
        non_equi_conditions: &mut Vec<ScalarExpr>,
        other_join_conditions: &mut Vec<ScalarExpr>,
    ) -> Result<()> {
        let conjunctions = split_conjunctions_expr(condition);
        for expr in conjunctions.iter() {
            self.resolve_predicate(
                expr,
                left_join_conditions,
                right_join_conditions,
                non_equi_conditions,
                other_join_conditions,
            )
            .await?;
        }
        wrap_nullable_for_column(
            &self.join_op,
            self.left_context,
            self.right_context,
            self.join_context,
        );
        Ok(())
    }

    #[async_backtrace::framed]
    async fn resolve_predicate(
        &self,
        predicate: &Expr,
        left_join_conditions: &mut Vec<ScalarExpr>,
        right_join_conditions: &mut Vec<ScalarExpr>,
        non_equi_conditions: &mut Vec<ScalarExpr>,
        other_join_conditions: &mut Vec<ScalarExpr>,
    ) -> Result<()> {
        let mut join_context = (*self.join_context).clone();
        wrap_nullable_for_column(
            &self.join_op,
            self.left_context,
            self.right_context,
            &mut join_context,
        );
        let mut scalar_binder = ScalarBinder::new(
            &mut join_context,
            self.ctx.clone(),
            self.name_resolution_ctx,
            self.metadata.clone(),
            &[],
            self.m_cte_bound_ctx.clone(),
        );
        // Given two tables: t1(a, b), t2(a, b)
        // A predicate can be regarded as an equi-predicate iff:
        //
        //   - The predicate is literally an equivalence expression, e.g. `t1.a = t2.a`
        //   - Each side of `=` only contains columns from one table and the both sides are disjoint.
        //     For example, `t1.a + t1.b = t2.a` is a valid one while `t1.a + t2.a = t2.b` isn't.
        //
        // Only equi-predicate can be exploited by common join algorithms(e.g. sort-merge join, hash join).

        let mut added = if let Some((left, right)) = split_equivalent_predicate_expr(predicate) {
            let (left, _) = scalar_binder.bind(&left).await?;
            let (right, _) = scalar_binder.bind(&right).await?;
            self.add_equi_conditions(left, right, left_join_conditions, right_join_conditions)?
        } else {
            false
        };
        if !added {
            added = self
                .add_other_conditions(predicate, other_join_conditions)
                .await?;
            if !added {
                let (predicate, _) = scalar_binder.bind(predicate).await?;
                non_equi_conditions.push(predicate);
            }
        }
        Ok(())
    }

    #[async_backtrace::framed]
    async fn resolve_using(
        &mut self,
        using_columns: Vec<(Span, String)>,
        left_join_conditions: &mut Vec<ScalarExpr>,
        right_join_conditions: &mut Vec<ScalarExpr>,
        join_op: &JoinOperator,
    ) -> Result<()> {
        wrap_nullable_for_column(
            &self.join_op,
            self.left_context,
            self.right_context,
            self.join_context,
        );
        let left_columns_len = self.left_context.columns.len();
        for (span, join_key) in using_columns.iter() {
            let join_key_name = join_key.as_str();
            let left_scalar = if let Some(col_binding) = self.join_context.columns
                [0..left_columns_len]
                .iter()
                .find(|col_binding| col_binding.column_name == join_key_name)
            {
                ScalarExpr::BoundColumnRef(BoundColumnRef {
                    span: *span,
                    column: col_binding.clone(),
                })
            } else {
                return Err(ErrorCode::SemanticError(format!(
                    "column {} specified in USING clause does not exist in left table",
                    join_key_name
                ))
                .set_span(*span));
            };

            let right_scalar = if let Some(col_binding) = self.join_context.columns
                [left_columns_len..]
                .iter()
                .find(|col_binding| col_binding.column_name == join_key_name)
            {
                ScalarExpr::BoundColumnRef(BoundColumnRef {
                    span: *span,
                    column: col_binding.clone(),
                })
            } else {
                return Err(ErrorCode::SemanticError(format!(
                    "column {} specified in USING clause does not exist in right table",
                    join_key_name
                ))
                .set_span(*span));
            };
            let idx = !matches!(join_op, JoinOperator::RightOuter) as usize;
            if let Some(col_binding) = self
                .join_context
                .columns
                .iter_mut()
                .filter(|col_binding| col_binding.column_name == join_key_name)
                .nth(idx)
            {
                // Always make the second using column in the join_context invisible in unqualified wildcard.
                col_binding.visibility = Visibility::UnqualifiedWildcardInVisible;
            }

            self.add_equi_conditions(
                left_scalar,
                right_scalar,
                left_join_conditions,
                right_join_conditions,
            )?;
        }
        Ok(())
    }

    fn add_equi_conditions(
        &self,
        left: ScalarExpr,
        right: ScalarExpr,
        left_join_conditions: &mut Vec<ScalarExpr>,
        right_join_conditions: &mut Vec<ScalarExpr>,
    ) -> Result<bool> {
        let left_used_columns = left.used_columns();
        let right_used_columns = right.used_columns();
        let (left_columns, right_columns) = self.left_right_columns()?;

        if !left_used_columns.is_empty() && !right_used_columns.is_empty() {
            if left_used_columns.is_subset(&left_columns)
                && right_used_columns.is_subset(&right_columns)
            {
                left_join_conditions.push(left);
                right_join_conditions.push(right);
                return Ok(true);
            } else if left_used_columns.is_subset(&right_columns)
                && right_used_columns.is_subset(&left_columns)
            {
                left_join_conditions.push(right);
                right_join_conditions.push(left);
                return Ok(true);
            }
        }

        Ok(false)
    }

    #[async_backtrace::framed]
    async fn add_other_conditions(
        &self,
        predicate: &Expr,
        other_join_conditions: &mut Vec<ScalarExpr>,
    ) -> Result<bool> {
        let mut join_context = (*self.join_context).clone();
        wrap_nullable_for_column(
            &JoinOperator::Inner,
            self.left_context,
            self.right_context,
            &mut join_context,
        );
        let mut scalar_binder = ScalarBinder::new(
            &mut join_context,
            self.ctx.clone(),
            self.name_resolution_ctx,
            self.metadata.clone(),
            &[],
            self.m_cte_bound_ctx.clone(),
        );
        let (predicate, _) = scalar_binder.bind(predicate).await?;
        let predicate_used_columns = predicate.used_columns();
        let (left_columns, right_columns) = self.left_right_columns()?;
        match self.join_op {
            JoinOperator::LeftOuter => {
                if predicate_used_columns.is_subset(&right_columns) {
                    other_join_conditions.push(predicate);
                    return Ok(true);
                }
            }
            JoinOperator::RightOuter => {
                if predicate_used_columns.is_subset(&left_columns) {
                    other_join_conditions.push(predicate);
                    return Ok(true);
                }
            }
            JoinOperator::Inner => {
                if predicate_used_columns.is_subset(&left_columns)
                    || predicate_used_columns.is_subset(&right_columns)
                {
                    other_join_conditions.push(predicate);
                    return Ok(true);
                }
            }
            _ => {
                return Ok(false);
            }
        }
        Ok(false)
    }

    fn left_right_columns(&self) -> Result<(ColumnSet, ColumnSet)> {
        let left_columns: ColumnSet =
            self.left_context
                .all_column_bindings()
                .iter()
                .fold(ColumnSet::new(), |mut acc, v| {
                    acc.insert(v.index);
                    acc
                });
        let right_columns: ColumnSet =
            self.right_context
                .all_column_bindings()
                .iter()
                .fold(ColumnSet::new(), |mut acc, v| {
                    acc.insert(v.index);
                    acc
                });
        Ok((left_columns, right_columns))
    }

    fn find_using_columns(&self, using_columns: &mut Vec<(Span, String)>) -> Result<()> {
        for left_column in self.left_context.all_column_bindings().iter() {
            for right_column in self.right_context.all_column_bindings().iter() {
                if left_column.column_name == right_column.column_name {
                    using_columns.push((None, left_column.column_name.clone()));
                }
            }
        }
        Ok(())
    }
}
