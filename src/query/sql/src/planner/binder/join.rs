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
use common_ast::ast::split_conjunctions_expr;
use common_ast::ast::Expr;
use common_ast::ast::JoinCondition;
use common_ast::ast::JoinOperator;
use common_catalog::table_context::TableContext;
use common_exception::ErrorCode;
use common_exception::Result;
use common_expression::types::DataType;

use crate::binder::Visibility;
use crate::normalize_identifier;
use crate::optimizer::SExpr;
use crate::planner::binder::scalar::ScalarBinder;
use crate::planner::binder::Binder;
use crate::planner::semantic::NameResolutionContext;
use crate::plans::BoundColumnRef;
use crate::plans::ComparisonExpr;
use crate::plans::ComparisonOp;
use crate::plans::Join;
use crate::plans::JoinType;
use crate::plans::ScalarExpr;
use crate::BindContext;
use crate::MetadataRef;

impl Binder {
    #[async_recursion]
    pub(super) async fn bind_join(
        &mut self,
        bind_context: &BindContext,
        join: &common_ast::ast::Join,
    ) -> Result<(SExpr, BindContext)> {
        let (left_child, left_context) =
            self.bind_table_reference(bind_context, &join.left).await?;
        let (right_child, right_context) =
            self.bind_table_reference(bind_context, &join.right).await?;

        check_duplicate_join_tables(&left_context, &right_context)?;

        // Build bind_context for join.
        let mut bind_context = bind_context.replace();
        for column in left_context.all_column_bindings() {
            bind_context.add_column_binding(column.clone());
        }
        for column in right_context.all_column_bindings() {
            bind_context.add_column_binding(column.clone());
        }

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

        let mut join_conditions = vec![];
        let mut join_condition_resolver = JoinConditionResolver::new(
            self.ctx.clone(),
            &self.name_resolution_ctx,
            self.metadata.clone(),
            join.op.clone(),
            &left_context,
            &right_context,
            &mut bind_context,
            &join.condition,
        );
        join_condition_resolver
            .resolve(&mut join_conditions, &join.op)
            .await?;

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
        join_conditions: Vec<ScalarExpr>,
        left_child: SExpr,
        right_child: SExpr,
    ) -> Result<SExpr> {
        let join = Join {
            join_type,
            conditions: join_conditions,
            marker_index: None,
            from_correlated_subquery: false,
            contain_runtime_filter: false,
        };
        Ok(SExpr::create_binary(join.into(), left_child, right_child))
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
    _join_op: JoinOperator,
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
        _join_op: JoinOperator,
        left_context: &'a BindContext,
        right_context: &'a BindContext,
        join_context: &'a mut BindContext,
        join_condition: &'a JoinCondition,
    ) -> Self {
        Self {
            ctx,
            name_resolution_ctx,
            metadata,
            _join_op,
            left_context,
            right_context,
            join_context,
            join_condition,
        }
    }

    pub async fn resolve(
        &mut self,
        conditions: &mut Vec<ScalarExpr>,
        join_op: &JoinOperator,
    ) -> Result<()> {
        match &self.join_condition {
            JoinCondition::On(cond) => {
                self.resolve_on(cond, conditions).await?;
            }
            JoinCondition::Using(identifiers) => {
                let using_columns = identifiers
                    .iter()
                    .map(|ident| normalize_identifier(ident, self.name_resolution_ctx).name)
                    .collect::<Vec<String>>();
                self.resolve_using(using_columns, conditions, join_op)
                    .await?;
            }
            JoinCondition::Natural => {
                // NATURAL is a shorthand form of USING: it forms a USING list consisting of all column names that appear in both input tables
                // As with USING, these columns appear only once in the output table
                // Todo(xudong963) If there are no common column names, NATURAL JOIN behaves like JOIN ... ON TRUE, producing a cross-product join.
                let mut using_columns = vec![];
                // Find common columns in both input tables
                self.find_using_columns(&mut using_columns)?;
                self.resolve_using(using_columns, conditions, join_op)
                    .await?
            }
            JoinCondition::None => {}
        }
        Ok(())
    }

    async fn resolve_on(
        &mut self,
        condition: &Expr,
        conditions: &mut Vec<ScalarExpr>,
    ) -> Result<()> {
        let conjunctions = split_conjunctions_expr(condition);
        for expr in conjunctions.iter() {
            self.resolve_predicate(expr, conditions).await?;
        }
        Ok(())
    }

    async fn resolve_predicate(
        &self,
        predicate: &Expr,
        conditions: &mut Vec<ScalarExpr>,
    ) -> Result<()> {
        let mut scalar_binder = ScalarBinder::new(
            self.join_context,
            self.ctx.clone(),
            self.name_resolution_ctx,
            self.metadata.clone(),
            &[],
        );
        let (predicate, _) = scalar_binder.bind(predicate).await?;
        conditions.push(predicate);
        Ok(())
    }

    async fn resolve_using(
        &mut self,
        using_columns: Vec<String>,
        conditions: &mut Vec<ScalarExpr>,
        join_op: &JoinOperator,
    ) -> Result<()> {
        let left_columns_len = self.left_context.columns.len();
        for join_key in using_columns.iter() {
            let join_key_name = join_key.as_str();
            let left_scalar = if let Some(col_binding) = self.join_context.columns
                [0..left_columns_len]
                .iter()
                .find(|col_binding| col_binding.column_name == join_key_name)
            {
                ScalarExpr::BoundColumnRef(BoundColumnRef {
                    column: col_binding.clone(),
                })
            } else {
                return Err(ErrorCode::SemanticError(format!(
                    "column {} specified in USING clause does not exist in left table",
                    join_key_name
                )));
            };

            let right_scalar = if let Some(col_binding) = self.join_context.columns
                [left_columns_len..]
                .iter()
                .find(|col_binding| col_binding.column_name == join_key_name)
            {
                ScalarExpr::BoundColumnRef(BoundColumnRef {
                    column: col_binding.clone(),
                })
            } else {
                return Err(ErrorCode::SemanticError(format!(
                    "column {} specified in USING clause does not exist in right table",
                    join_key_name
                )));
            };
            // According to the definition of USING clause and NATURAL JOIN, if
            // the join operator is left outer join or inner join, the column
            // referenced later without qualified table name should be the column
            // in the left table. If the join operator is right outer join, the
            // referenced column should be the column in the right table.
            let idx = !matches!(&join_op, JoinOperator::RightOuter) as usize;
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

            let condition = ScalarExpr::ComparisonExpr(ComparisonExpr {
                left: Box::new(left_scalar),
                op: ComparisonOp::Equal,
                right: Box::new(right_scalar),
                return_type: Box::new(DataType::Boolean),
            });
            conditions.push(condition);
        }
        Ok(())
    }

    fn find_using_columns(&self, using_columns: &mut Vec<String>) -> Result<()> {
        for left_column in self.left_context.all_column_bindings().iter() {
            for right_column in self.right_context.all_column_bindings().iter() {
                if left_column.column_name == right_column.column_name {
                    using_columns.push(left_column.column_name.clone());
                }
            }
        }
        Ok(())
    }
}
