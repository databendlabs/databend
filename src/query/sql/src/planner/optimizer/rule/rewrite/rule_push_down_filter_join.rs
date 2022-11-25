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

use common_datavalues::type_coercion::compare_coercion;
use common_exception::Result;

use crate::binder::wrap_cast;
use crate::binder::JoinPredicate;
use crate::optimizer::rule::Rule;
use crate::optimizer::rule::TransformResult;
use crate::optimizer::RelExpr;
use crate::optimizer::RuleID;
use crate::optimizer::SExpr;
use crate::plans::AndExpr;
use crate::plans::Filter;
use crate::plans::JoinType;
use crate::plans::LogicalJoin;
use crate::plans::OrExpr;
use crate::plans::PatternPlan;
use crate::plans::RelOp;
use crate::plans::Scalar;
use crate::plans::ScalarExpr;
use crate::ColumnSet;
use crate::IndexType;

pub struct RulePushDownFilterJoin {
    id: RuleID,
    pattern: SExpr,
}

impl RulePushDownFilterJoin {
    pub fn new() -> Self {
        Self {
            id: RuleID::PushDownFilterJoin,
            // Filter
            //  \
            //   InnerJoin
            //   | \
            //   |  *
            //   *
            pattern: SExpr::create_unary(
                PatternPlan {
                    plan_type: RelOp::Filter,
                }
                .into(),
                SExpr::create_binary(
                    PatternPlan {
                        plan_type: RelOp::LogicalJoin,
                    }
                    .into(),
                    SExpr::create_leaf(
                        PatternPlan {
                            plan_type: RelOp::Pattern,
                        }
                        .into(),
                    ),
                    SExpr::create_leaf(
                        PatternPlan {
                            plan_type: RelOp::Pattern,
                        }
                        .into(),
                    ),
                ),
            ),
        }
    }

    #[allow(clippy::only_used_in_recursion)]
    fn find_nullable_columns(
        &self,
        predicate: &Scalar,
        left_output_columns: &ColumnSet,
        right_output_columns: &ColumnSet,
        nullable_columns: &mut Vec<IndexType>,
    ) -> Result<()> {
        match predicate {
            Scalar::BoundColumnRef(column_binding) => {
                nullable_columns.push(column_binding.column.index);
            }
            Scalar::OrExpr(expr) => {
                let mut left_cols = vec![];
                let mut right_cols = vec![];
                self.find_nullable_columns(
                    &expr.left,
                    left_output_columns,
                    right_output_columns,
                    &mut left_cols,
                )?;
                self.find_nullable_columns(
                    &expr.right,
                    left_output_columns,
                    right_output_columns,
                    &mut right_cols,
                )?;
                if !left_cols.is_empty() && !right_cols.is_empty() {
                    for left_col in left_cols.iter() {
                        for right_col in right_cols.iter() {
                            if (left_output_columns.contains(left_col)
                                && left_output_columns.contains(right_col))
                                || (right_output_columns.contains(left_col)
                                    && right_output_columns.contains(right_col))
                            {
                                nullable_columns.push(*left_col);
                                break;
                            }
                        }
                    }
                }
            }
            Scalar::ComparisonExpr(expr) => {
                // For any comparison expr, if input is null, the compare result is false
                self.find_nullable_columns(
                    &expr.left,
                    left_output_columns,
                    right_output_columns,
                    nullable_columns,
                )?;
                self.find_nullable_columns(
                    &expr.right,
                    left_output_columns,
                    right_output_columns,
                    nullable_columns,
                )?;
            }
            Scalar::CastExpr(expr) => {
                self.find_nullable_columns(
                    &expr.argument,
                    left_output_columns,
                    right_output_columns,
                    nullable_columns,
                )?;
            }
            // `predicate` can't be `Scalar::AndExpr`
            // because `Scalar::AndExpr` had been split in binder
            _ => {}
        }
        Ok(())
    }

    fn convert_outer_to_inner_join(&self, s_expr: &SExpr) -> Result<SExpr> {
        let filter: Filter = s_expr.plan().clone().try_into()?;
        let mut join: LogicalJoin = s_expr.child(0)?.plan().clone().try_into()?;
        let origin_join_type = join.join_type.clone();
        if !origin_join_type.is_outer_join() {
            return Ok(s_expr.clone());
        }
        let s_join_expr = s_expr.child(0)?;
        let join_expr = RelExpr::with_s_expr(s_join_expr);
        let left_child_output_column = join_expr.derive_relational_prop_child(0)?.output_columns;
        let right_child_output_column = join_expr.derive_relational_prop_child(1)?.output_columns;
        let predicates = &filter.predicates;
        let mut nullable_columns: Vec<IndexType> = vec![];
        for predicate in predicates {
            self.find_nullable_columns(
                predicate,
                &left_child_output_column,
                &right_child_output_column,
                &mut nullable_columns,
            )?;
        }

        if join.join_type == JoinType::Left
            || join.join_type == JoinType::Right
            || join.join_type == JoinType::Full
        {
            let mut left_join = false;
            let mut right_join = false;
            for col in nullable_columns.iter() {
                if left_child_output_column.contains(col) {
                    right_join = true;
                }
                if right_child_output_column.contains(col) {
                    left_join = true;
                }
            }

            match join.join_type {
                JoinType::Left => {
                    if left_join {
                        join.join_type = JoinType::Inner
                    }
                }
                JoinType::Right => {
                    if right_join {
                        join.join_type = JoinType::Inner
                    }
                }
                JoinType::Full => {
                    if left_join && right_join {
                        join.join_type = JoinType::Inner
                    } else if left_join {
                        join.join_type = JoinType::Right
                    } else if right_join {
                        join.join_type = JoinType::Left
                    }
                }
                _ => unreachable!(),
            }
        }

        let changed_join_type = join.join_type.clone();
        if origin_join_type == changed_join_type {
            return Ok(s_expr.clone());
        }
        let mut result = SExpr::create_binary(
            join.into(),
            s_join_expr.child(0)?.clone(),
            s_join_expr.child(1)?.clone(),
        );
        // wrap filter s_expr
        result = SExpr::create_unary(filter.into(), result);
        Ok(result)
    }

    fn convert_mark_to_semi_join(&self, s_expr: &SExpr) -> Result<SExpr> {
        let mut filter: Filter = s_expr.plan().clone().try_into()?;
        let mut join: LogicalJoin = s_expr.child(0)?.plan().clone().try_into()?;
        let has_disjunction = filter
            .predicates
            .iter()
            .any(|predicate| matches!(predicate, Scalar::OrExpr(_)));
        if !join.join_type.is_mark_join() || has_disjunction {
            return Ok(s_expr.clone());
        }

        let mark_index = join.marker_index.unwrap();

        // remove mark index filter
        for (idx, predicate) in filter.predicates.iter().enumerate() {
            if let Scalar::BoundColumnRef(col) = predicate {
                if col.column.index == mark_index {
                    filter.predicates.remove(idx);
                    break;
                }
            }
            if let Scalar::FunctionCall(func) = predicate {
                if func.func_name == "not" && func.arguments.len() == 1 {
                    // Check if the argument is mark index, if so, we won't convert it to semi join
                    if let Scalar::BoundColumnRef(col) = &func.arguments[0] {
                        if col.column.index == mark_index {
                            return Ok(s_expr.clone());
                        }
                    }
                }
            }
        }

        join.join_type = match join.join_type {
            JoinType::LeftMark => JoinType::RightSemi,
            JoinType::RightMark => JoinType::LeftSemi,
            _ => unreachable!(),
        };

        let s_join_expr = s_expr.child(0)?;
        let mut result = SExpr::create_binary(
            join.into(),
            s_join_expr.child(0)?.clone(),
            s_join_expr.child(1)?.clone(),
        );

        result = SExpr::create_unary(filter.into(), result);
        Ok(result)
    }
}

impl Rule for RulePushDownFilterJoin {
    fn id(&self) -> RuleID {
        self.id
    }

    fn apply(&self, s_expr: &SExpr, state: &mut TransformResult) -> Result<()> {
        // First, try to convert outer join to inner join
        let mut s_expr = self.convert_outer_to_inner_join(s_expr)?;
        // Second, check if can convert mark join to semi join
        s_expr = self.convert_mark_to_semi_join(&s_expr)?;
        let filter: Filter = s_expr.plan().clone().try_into()?;
        if filter.predicates.is_empty() {
            state.add_result(s_expr);
            return Ok(());
        }
        // Third, extract or predicates from Filter to push down them to join.
        // For example: `select * from t1, t2 where (t1.a=1 and t2.b=2) or (t1.a=2 and t2.b=1)`
        // The predicate will be rewritten to `((t1.a=1 and t2.b=2) or (t1.a=2 and t2.b=1)) and (t1.a=1 or t1.a=2) and (t2.b=2 or t2.b=1)`
        // So `(t1.a=1 or t1.a=1), (t2.b=2 or t2.b=1)` may be pushed down join and reduce rows between join
        let predicates = rewrite_predicates(&s_expr)?;
        let (need_push, mut result) = try_push_down_filter_join(&s_expr, predicates)?;
        if !need_push {
            return Ok(());
        }
        result.apply_rule(&self.id);
        state.add_result(result);

        Ok(())
    }

    fn pattern(&self) -> &SExpr {
        &self.pattern
    }
}

fn rewrite_predicates(s_expr: &SExpr) -> Result<Vec<Scalar>> {
    let filter: Filter = s_expr.plan().clone().try_into()?;
    let join = s_expr.child(0)?;
    let mut new_predicates = Vec::new();
    let mut origin_predicates = filter.predicates.clone();
    for predicate in filter.predicates.iter() {
        if let Scalar::OrExpr(or_expr) = predicate {
            for join_child in join.children().iter() {
                let rel_expr = RelExpr::with_s_expr(join_child);
                let used_columns = rel_expr.derive_relational_prop()?.used_columns;
                if let Some(predicate) = extract_or_predicate(or_expr, &used_columns)? {
                    new_predicates.push(predicate)
                }
            }
        }
    }
    origin_predicates.extend(new_predicates);
    Ok(origin_predicates)
}

// Only need to be executed once
fn extract_or_predicate(or_expr: &OrExpr, required_columns: &ColumnSet) -> Result<Option<Scalar>> {
    let or_args = flatten_ors(or_expr.clone());
    let mut extracted_scalars = Vec::new();
    for or_arg in or_args.iter() {
        let mut sub_scalars = Vec::new();
        if let Scalar::AndExpr(and_expr) = or_arg {
            let and_args = flatten_ands(and_expr.clone());
            for and_arg in and_args.iter() {
                if let Scalar::OrExpr(or_expr) = and_arg {
                    if let Some(scalar) = extract_or_predicate(or_expr, required_columns)? {
                        sub_scalars.push(scalar);
                    }
                } else {
                    let used_columns = and_arg.used_columns();
                    if used_columns.is_subset(required_columns) {
                        sub_scalars.push(and_arg.clone());
                    }
                }
            }
        } else {
            let used_columns = or_arg.used_columns();
            if used_columns.is_subset(required_columns) {
                sub_scalars.push(or_arg.clone());
            }
        }
        if sub_scalars.is_empty() {
            return Ok(None);
        }

        extracted_scalars.push(make_and_expr(&sub_scalars));
    }

    if !extracted_scalars.is_empty() {
        return Ok(Some(make_or_expr(&extracted_scalars)));
    }

    Ok(None)
}

pub fn try_push_down_filter_join(s_expr: &SExpr, predicates: Vec<Scalar>) -> Result<(bool, SExpr)> {
    let join_expr = s_expr.child(0)?;
    let mut join: LogicalJoin = join_expr.plan().clone().try_into()?;

    let rel_expr = RelExpr::with_s_expr(join_expr);
    let left_prop = rel_expr.derive_relational_prop_child(0)?;
    let right_prop = rel_expr.derive_relational_prop_child(1)?;

    let mut left_push_down = vec![];
    let mut right_push_down = vec![];
    let mut original_predicates = vec![];

    let mut need_push = false;

    for predicate in predicates.into_iter() {
        let pred = JoinPredicate::new(&predicate, &left_prop, &right_prop);
        match pred {
            JoinPredicate::Left(_) => {
                if matches!(join.join_type, JoinType::Right) {
                    original_predicates.push(predicate);
                    continue;
                }
                need_push = true;
                left_push_down.push(predicate);
            }
            JoinPredicate::Right(_) => {
                if matches!(join.join_type, JoinType::Left) {
                    original_predicates.push(predicate);
                    continue;
                }
                need_push = true;
                right_push_down.push(predicate);
            }
            JoinPredicate::Other(_) => original_predicates.push(predicate),

            JoinPredicate::Both { left, right } => {
                let left_type = left.data_type();
                let right_type = right.data_type();
                let join_key_type = compare_coercion(&left_type, &right_type);

                // We have to check if left_type and right_type can be coerced to
                // a super type. If the coercion is failed, we cannot push the
                // predicate into join.
                if let Ok(join_key_type) = join_key_type {
                    if join.join_type == JoinType::Cross {
                        join.join_type = JoinType::Inner;
                    }
                    if left.data_type().ne(&right.data_type()) {
                        let left = wrap_cast(left.clone(), &join_key_type);
                        let right = wrap_cast(right.clone(), &join_key_type);
                        join.left_conditions.push(left);
                        join.right_conditions.push(right);
                    } else {
                        join.left_conditions.push(left.clone());
                        join.right_conditions.push(right.clone());
                    }
                    need_push = true;
                } else {
                    original_predicates.push(predicate);
                }
            }
        }
    }

    if !need_push {
        return Ok((false, s_expr.clone()));
    }

    let mut left_child = join_expr.child(0)?.clone();
    let mut right_child = join_expr.child(1)?.clone();

    if !left_push_down.is_empty() {
        left_child = SExpr::create_unary(
            Filter {
                predicates: left_push_down,
                is_having: false,
            }
            .into(),
            left_child,
        );
    }

    if !right_push_down.is_empty() {
        right_child = SExpr::create_unary(
            Filter {
                predicates: right_push_down,
                is_having: false,
            }
            .into(),
            right_child,
        );
    }

    let mut result = SExpr::create_binary(join.into(), left_child, right_child);

    if !original_predicates.is_empty() {
        result = SExpr::create_unary(
            Filter {
                predicates: original_predicates,
                is_having: false,
            }
            .into(),
            result,
        );
    }
    Ok((need_push, result))
}

// Flatten nested ORs, such as `a=1 or b=1 or c=1`
// It'll be flatten to [a=1, b=1, c=1]
fn flatten_ors(or_expr: OrExpr) -> Vec<Scalar> {
    let mut flattened_ors = Vec::new();
    let or_args = vec![*or_expr.left, *or_expr.right];
    for or_arg in or_args.iter() {
        match or_arg {
            Scalar::OrExpr(or_expr) => flattened_ors.extend(flatten_ors(or_expr.clone())),
            _ => flattened_ors.push(or_arg.clone()),
        }
    }
    flattened_ors
}

// Flatten nested ORs, such as `a=1 and b=1 and c=1`
// It'll be flatten to [a=1, b=1, c=1]
fn flatten_ands(and_expr: AndExpr) -> Vec<Scalar> {
    let mut flattened_ands = Vec::new();
    let and_args = vec![*and_expr.left, *and_expr.right];
    for and_arg in and_args.iter() {
        match and_arg {
            Scalar::AndExpr(and_expr) => flattened_ands.extend(flatten_ands(and_expr.clone())),
            _ => flattened_ands.push(and_arg.clone()),
        }
    }
    flattened_ands
}

// Merge predicates to AND scalar
fn make_and_expr(scalars: &[Scalar]) -> Scalar {
    if scalars.len() == 1 {
        return scalars[0].clone();
    }
    Scalar::AndExpr(AndExpr {
        left: Box::new(scalars[0].clone()),
        right: Box::new(make_and_expr(&scalars[1..])),
        return_type: Box::new(scalars[0].data_type()),
    })
}

// Merge predicates to OR scalar
fn make_or_expr(scalars: &[Scalar]) -> Scalar {
    if scalars.len() == 1 {
        return scalars[0].clone();
    }
    Scalar::OrExpr(OrExpr {
        left: Box::new(scalars[0].clone()),
        right: Box::new(make_or_expr(&scalars[1..])),
        return_type: Box::new(scalars[0].data_type()),
    })
}
