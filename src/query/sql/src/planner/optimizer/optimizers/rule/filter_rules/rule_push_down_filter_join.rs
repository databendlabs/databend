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

use databend_common_exception::Result;
use databend_common_expression::FunctionContext;
use databend_common_expression::types::DataType;

use crate::MetadataRef;
use crate::binder::JoinPredicate;
use crate::optimizer::ir::Matcher;
use crate::optimizer::ir::RelExpr;
use crate::optimizer::ir::RelationalProperty;
use crate::optimizer::ir::SExpr;
use crate::optimizer::ir::StatContext;
use crate::optimizer::optimizers::operator::EquivalentConstantsVisitor;
use crate::optimizer::optimizers::operator::InferFilterOptimizer;
use crate::optimizer::optimizers::operator::JoinCondition;
use crate::optimizer::optimizers::operator::JoinFilters;
use crate::optimizer::optimizers::operator::JoinProperty;
use crate::optimizer::optimizers::rule::Rule;
use crate::optimizer::optimizers::rule::RuleID;
use crate::optimizer::optimizers::rule::TransformResult;
use crate::optimizer::optimizers::rule::can_filter_null;
use crate::optimizer::optimizers::rule::constant::false_constant;
use crate::optimizer::optimizers::rule::constant::is_falsy;
use crate::optimizer::optimizers::rule::convert_mark_to_semi_join;
use crate::optimizer::optimizers::rule::outer_join_to_anti_join;
use crate::optimizer::optimizers::rule::outer_join_to_inner_join;
use crate::optimizer::optimizers::rule::rewrite_predicates;
use crate::plans::ComparisonOp;
use crate::plans::Filter;
use crate::plans::FunctionCall;
use crate::plans::Join;
use crate::plans::JoinType;
use crate::plans::Operator;
use crate::plans::RelOp;
use crate::plans::ScalarExpr;
use crate::plans::VisitorMut;

pub struct RulePushDownFilterJoin {
    matchers: Vec<Matcher>,
    metadata: MetadataRef,
    stat_context: StatContext,
}

impl RulePushDownFilterJoin {
    pub fn new(metadata: MetadataRef, stat_context: StatContext) -> Self {
        Self {
            // Filter
            //  \
            //   Join
            //   | \
            //   |  *
            //   *
            matchers: vec![Matcher::MatchOp {
                op_type: RelOp::Filter,
                children: vec![Matcher::MatchOp {
                    op_type: RelOp::Join,
                    children: vec![Matcher::Leaf, Matcher::Leaf],
                }],
            }],
            metadata,
            stat_context,
        }
    }
}

impl Rule for RulePushDownFilterJoin {
    fn id(&self) -> RuleID {
        RuleID::PushDownFilterJoin
    }

    fn apply(&self, s_expr: &SExpr, state: &mut TransformResult) -> Result<()> {
        // First, try to convert the outer join exclusion pattern to an anti join.
        if let Some(mut result) = outer_join_to_anti_join(s_expr, self.metadata.clone())? {
            result.set_applied_rule(&self.id());
            state.add_result(result);
            return Ok(());
        }

        // Second, try to convert outer join to inner join
        let (s_expr, outer_to_inner) =
            outer_join_to_inner_join(s_expr, self.metadata.clone(), &self.stat_context)?;

        // Third, check if can convert mark join to semi join
        let (s_expr, mark_to_semi) = convert_mark_to_semi_join(&s_expr, self.metadata.clone())?;
        if s_expr.plan().rel_op() != RelOp::Filter {
            state.add_result(s_expr);
            return Ok(());
        }
        let filter: Filter = s_expr.plan().clone().try_into()?;
        if filter.predicates.is_empty() {
            state.add_result(s_expr);
            return Ok(());
        }

        // Finally, push down filter to join.
        let (need_push, mut result) = try_push_down_filter_join(
            &s_expr,
            self.metadata.clone(),
            &self.stat_context.function_context,
        )?;
        if !need_push && !outer_to_inner && !mark_to_semi {
            return Ok(());
        }

        result.set_applied_rule(&self.id());
        state.add_result(result);

        Ok(())
    }

    fn matchers(&self) -> &[Matcher] {
        &self.matchers
    }
}

fn try_push_down_filter_join(
    s_expr: &SExpr,
    metadata: MetadataRef,
    func_ctx: &FunctionContext,
) -> Result<(bool, SExpr)> {
    // Extract or predicates from Filter to push down them to join.
    // For example: `select * from t1, t2 where (t1.a=1 and t2.b=2) or (t1.a=2 and t2.b=1)`
    // The predicate will be rewritten to `((t1.a=1 and t2.b=2) or (t1.a=2 and t2.b=1)) and (t1.a=1 or t1.a=2) and (t2.b=2 or t2.b=1)`
    // So `(t1.a=1 or t1.a=1), (t2.b=2 or t2.b=1)` may be pushed down join and reduce rows between join
    let mut predicates = rewrite_predicates(s_expr)?;
    let join_expr = s_expr.child(0)?;
    let mut join: Join = join_expr.plan().clone().try_into()?;

    let rel_expr = RelExpr::with_s_expr(join_expr);
    let left_prop = rel_expr.derive_relational_prop_child(0)?;
    let right_prop = rel_expr.derive_relational_prop_child(1)?;
    let mut visitor = EquivalentConstantsVisitor::default();

    for predicate in predicates.iter_mut() {
        if let JoinPredicate::Both {
            is_equal_op: true, ..
        } = JoinPredicate::new(predicate, &left_prop, &right_prop)
        {
            // skip join eq conditions
            visitor.visit(&mut predicate.clone())?;
        } else {
            visitor.visit(predicate)?;
        }
    }
    let original_predicates_count = predicates.len();
    let mut placement = classify_predicates(
        predicates,
        &mut join,
        &left_prop,
        &right_prop,
        metadata,
        func_ctx,
    )?;

    if placement.filters.residual.len() == original_predicates_count {
        return Ok((false, s_expr.clone()));
    }
    infer_join_predicates(&mut join, &left_prop, &right_prop, &mut placement)?;
    place_inferred_predicates(&mut join, &left_prop, &right_prop, &mut placement);
    for predicate in placement.non_equi_predicates {
        JoinCondition::NonEqui(&predicate).insert_into(&mut join);
    }
    let result = placement.filters.build(
        join,
        join_expr.child(0)?.clone(),
        join_expr.child(1)?.clone(),
    );
    Ok((true, result))
}

struct PredicatePlacement {
    filters: JoinFilters,
    // Equalities and constants, plus eligible single-side predicates, feed
    // inference. Non-equalities are kept out of that phase.
    push_down_predicates: Vec<ScalarExpr>,
    non_equi_predicates: Vec<ScalarExpr>,
}

/// Nullable-side and join-type checks apply before inference. The second
/// classification consumes only inference output, with different guarantees.
fn classify_predicates(
    predicates: Vec<ScalarExpr>,
    join: &mut Join,
    left_prop: &RelationalProperty,
    right_prop: &RelationalProperty,
    metadata: MetadataRef,
    func_ctx: &FunctionContext,
) -> Result<PredicatePlacement> {
    let mut original_predicates = vec![];
    let mut left_push_down = vec![];
    let mut right_push_down = vec![];
    let mut push_down_predicates = vec![];
    let mut non_equi_predicates = vec![];
    for predicate in predicates.into_iter() {
        if is_falsy(&predicate) {
            push_down_predicates = vec![false_constant()];
            break;
        }
        let pred = JoinPredicate::new(&predicate, left_prop, right_prop);
        match pred {
            JoinPredicate::ALL(_) => {
                push_down_predicates.push(predicate);
            }
            side @ (JoinPredicate::Left(_) | JoinPredicate::Right(_)) => {
                let (prop, push_down, nullable_side) = if matches!(side, JoinPredicate::Left(_)) {
                    (
                        left_prop,
                        &mut left_push_down,
                        matches!(
                            join.join_type,
                            JoinType::Right
                                | JoinType::RightSingle
                                | JoinType::Full
                                | JoinType::FullAsof
                        ),
                    )
                } else {
                    (
                        right_prop,
                        &mut right_push_down,
                        matches!(
                            join.join_type,
                            JoinType::Left
                                | JoinType::LeftSingle
                                | JoinType::Full
                                | JoinType::FullAsof
                        ),
                    )
                };
                if !nullable_side
                    || can_filter_null(
                        &predicate,
                        &prop.output_columns,
                        &join.join_type,
                        metadata.clone(),
                        func_ctx,
                    )?
                {
                    push_down.push(predicate);
                } else {
                    original_predicates.push(predicate);
                }
            }
            JoinPredicate::Other(_)
                if can_place_other_as_non_equi(&predicate, join, left_prop, right_prop) =>
            {
                non_equi_predicates.push(predicate);
                if join.join_type == JoinType::Cross {
                    join.join_type = JoinType::Inner;
                }
            }
            JoinPredicate::Other(_) => original_predicates.push(predicate),
            JoinPredicate::Both { is_equal_op, .. } => {
                if matches!(join.join_type, JoinType::Inner | JoinType::Cross)
                    || join.single_to_inner.is_some()
                {
                    if is_equal_op {
                        push_down_predicates.push(predicate);
                    } else {
                        non_equi_predicates.push(predicate);
                    }
                    if join.join_type == JoinType::Cross {
                        join.join_type = JoinType::Inner;
                    }
                } else {
                    original_predicates.push(predicate);
                }
            }
        }
    }

    Ok(PredicatePlacement {
        filters: JoinFilters {
            residual: original_predicates,
            left: left_push_down,
            right: right_push_down,
        },
        push_down_predicates,
        non_equi_predicates,
    })
}

/// `Other` can be a valid residual without having separate left/right
/// operands. Column availability alone does not permit moving WHERE into an
/// outer/mark join, or changing a volatile expression's evaluation location.
fn can_place_other_as_non_equi(
    predicate: &ScalarExpr,
    join: &Join,
    left_prop: &RelationalProperty,
    right_prop: &RelationalProperty,
) -> bool {
    if !(matches!(join.join_type, JoinType::Inner | JoinType::Cross)
        || join.single_to_inner.is_some())
        || !predicate.is_deterministic()
    {
        return false;
    }
    let columns = predicate.used_columns();
    !columns.is_empty()
        && columns.iter().all(|column| {
            left_prop.output_columns.contains(column) || right_prop.output_columns.contains(column)
        })
}

fn infer_join_predicates(
    join: &mut Join,
    left_prop: &RelationalProperty,
    right_prop: &RelationalProperty,
    placement: &mut PredicatePlacement,
) -> Result<()> {
    let push_down_predicates = &mut placement.push_down_predicates;
    let left_push_down = &mut placement.filters.left;
    let right_push_down = &mut placement.filters.right;
    if !matches!(join.join_type, JoinType::Full | JoinType::FullAsof)
        && !join.has_null_equi_condition()
    {
        // Infer new predicate and push down filter.
        for equi_condition in join.equi_conditions.iter() {
            let left = equi_condition.left.clone();
            let right = equi_condition.right.clone();
            let return_type =
                ScalarExpr::passthrough_nullable_type(DataType::Boolean, [&left, &right]);
            push_down_predicates.push(ScalarExpr::FunctionCall(FunctionCall {
                span: None,
                func_name: String::from(ComparisonOp::Equal.to_func_name()),
                params: vec![],
                arguments: vec![left, right],
                return_type: Box::new(return_type),
            }));
        }
        join.equi_conditions.clear();
        match join.join_type {
            JoinType::Left | JoinType::LeftSingle => push_down_predicates.append(left_push_down),
            JoinType::Right | JoinType::RightSingle => push_down_predicates.append(right_push_down),
            _ => {
                push_down_predicates.append(left_push_down);
                push_down_predicates.append(right_push_down);
            }
        }
        let join_prop = JoinProperty::new(&left_prop.output_columns, &right_prop.output_columns);
        let mut infer_filter = InferFilterOptimizer::new(Some(join_prop));
        *push_down_predicates = infer_filter.optimize(std::mem::take(push_down_predicates))?;
    }

    Ok(())
}

fn place_inferred_predicates(
    join: &mut Join,
    left_prop: &RelationalProperty,
    right_prop: &RelationalProperty,
    placement: &mut PredicatePlacement,
) {
    let left_push_down = &mut placement.filters.left;
    let right_push_down = &mut placement.filters.right;
    let original_predicates = &mut placement.filters.residual;
    let mut all_push_down = vec![];
    for predicate in std::mem::take(&mut placement.push_down_predicates) {
        if is_falsy(&predicate) {
            *left_push_down = vec![false_constant()];
            *right_push_down = vec![false_constant()];
            break;
        }
        let pred = JoinPredicate::new(&predicate, left_prop, right_prop);
        match pred {
            JoinPredicate::ALL(_) => {
                all_push_down.push(predicate);
            }
            JoinPredicate::Left(_) => {
                left_push_down.push(predicate);
            }
            JoinPredicate::Right(_) => {
                right_push_down.push(predicate);
            }
            JoinPredicate::Both { left, right, .. } => {
                JoinCondition::Equi {
                    left: &left,
                    right: &right,
                    is_null_equal: false,
                }
                .insert_into(join);
            }
            JoinPredicate::Other(_)
                if can_place_other_as_non_equi(&predicate, join, left_prop, right_prop) =>
            {
                JoinCondition::NonEqui(&predicate).insert_into(join);
                if join.join_type == JoinType::Cross {
                    join.join_type = JoinType::Inner;
                }
            }
            _ => original_predicates.push(predicate),
        }
    }
    if !all_push_down.is_empty() {
        left_push_down.extend(all_push_down.to_vec());
        right_push_down.extend(all_push_down);
    }
}

#[cfg(test)]
mod tests {
    use databend_common_expression::Scalar;

    use super::*;
    use crate::ColumnBindingBuilder;
    use crate::Symbol;
    use crate::Visibility;
    use crate::plans::BoundColumnRef;
    use crate::plans::ConstantExpr;

    fn column(index: usize) -> ScalarExpr {
        BoundColumnRef {
            span: None,
            column: ColumnBindingBuilder::new(
                format!("c{index}"),
                Symbol::new(index),
                Box::new(DataType::Boolean),
                Visibility::Visible,
            )
            .build(),
        }
        .into()
    }

    fn other_predicate(right_index: usize) -> ScalarExpr {
        FunctionCall {
            span: None,
            func_name: "and_filters".to_string(),
            params: vec![],
            arguments: vec![
                column(0),
                column(right_index),
                ConstantExpr {
                    span: None,
                    value: Scalar::Boolean(true),
                }
                .into(),
            ],
            return_type: Box::new(DataType::Boolean),
        }
        .into()
    }

    #[test]
    fn other_residual_requires_available_columns_and_legal_join_type() {
        let left = RelationalProperty {
            output_columns: [Symbol::new(0)].into_iter().collect(),
            ..Default::default()
        };
        let right = RelationalProperty {
            output_columns: [Symbol::new(1)].into_iter().collect(),
            ..Default::default()
        };
        let predicate = other_predicate(1);
        assert!(matches!(
            JoinPredicate::new(&predicate, &left, &right),
            JoinPredicate::Other(_)
        ));
        for join_type in [JoinType::Inner, JoinType::Cross] {
            let join = Join {
                join_type,
                ..Default::default()
            };
            assert!(can_place_other_as_non_equi(
                &predicate, &join, &left, &right
            ));
            // Column 2 belongs to neither input; it must remain outside this join.
            assert!(!can_place_other_as_non_equi(
                &other_predicate(2),
                &join,
                &left,
                &right
            ));
        }
        for join_type in [
            JoinType::Left,
            JoinType::Right,
            JoinType::Full,
            JoinType::LeftMark,
            JoinType::RightMark,
        ] {
            let join = Join {
                join_type,
                ..Default::default()
            };
            assert!(!can_place_other_as_non_equi(
                &predicate, &join, &left, &right
            ));
        }
    }
}
