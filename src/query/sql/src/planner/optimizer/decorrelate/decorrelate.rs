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

use std::collections::HashSet;
use std::sync::Arc;

use databend_common_ast::Span;
use databend_common_exception::Result;
use databend_common_expression::types::DataType;

use crate::binder::ColumnBindingBuilder;
use crate::binder::JoinPredicate;
use crate::binder::Visibility;
use crate::optimizer::decorrelate::subquery_rewriter::FlattenInfo;
use crate::optimizer::decorrelate::subquery_rewriter::SubqueryRewriter;
use crate::optimizer::decorrelate::subquery_rewriter::UnnestResult;
use crate::optimizer::extract::Matcher;
use crate::optimizer::ColumnSet;
use crate::optimizer::RelExpr;
use crate::optimizer::SExpr;
use crate::plans::BoundColumnRef;
use crate::plans::Filter;
use crate::plans::FunctionCall;
use crate::plans::Join;
use crate::plans::JoinEquiCondition;
use crate::plans::JoinType;
use crate::plans::RelOp;
use crate::plans::ScalarExpr;
use crate::plans::SubqueryExpr;
use crate::plans::SubqueryType;
use crate::IndexType;
use crate::MetadataRef;

/// Decorrelate subqueries inside `s_expr`.
///
/// We only need to process three kinds of join: Scalar Subquery, Any Subquery, and Exists Subquery.
/// Other kinds of subqueries have be converted to one of the above subqueries in `type_check`.
///
/// It will rewrite `s_expr` to all kinds of join.
/// Correlated scalar subquery -> Single join
/// Any subquery -> Marker join
/// Correlated exists subquery -> Marker join
///
/// More information can be found in the paper: Unnesting Arbitrary Queries
pub fn decorrelate_subquery(metadata: MetadataRef, s_expr: SExpr) -> Result<SExpr> {
    let mut rewriter = SubqueryRewriter::new(metadata, None);
    rewriter.rewrite(&s_expr)
}

impl SubqueryRewriter {
    // Try to decorrelate a `CrossApply` into `SemiJoin` or `AntiJoin`.
    // We only do simple decorrelation here, the scheme is:
    // 1. If the subquery is correlated, we will try to decorrelate it into `SemiJoin`
    pub fn try_decorrelate_simple_subquery(
        &self,
        input: &SExpr,
        subquery: &SubqueryExpr,
    ) -> Result<Option<SExpr>> {
        if subquery.outer_columns.is_empty() {
            return Ok(None);
        }

        // TODO(leiysky): this is the canonical plan generated by Binder, we should find a proper
        // way to address such a pattern.
        //
        // (1) EvalScalar
        //      \
        //       Filter
        //        \
        //         Get
        //
        // (2) EvalScalar
        //      \
        //       Filter
        //        \
        //         EvalScalar
        //          \
        //           Get
        let matchers = vec![
            Matcher::MatchOp {
                op_type: RelOp::EvalScalar,
                children: vec![Matcher::MatchOp {
                    op_type: RelOp::Filter,
                    children: vec![Matcher::MatchOp {
                        op_type: RelOp::Scan,
                        children: vec![],
                    }],
                }],
            },
            Matcher::MatchOp {
                op_type: RelOp::EvalScalar,
                children: vec![Matcher::MatchOp {
                    op_type: RelOp::Filter,
                    children: vec![Matcher::MatchOp {
                        op_type: RelOp::EvalScalar,
                        children: vec![Matcher::MatchOp {
                            op_type: RelOp::Scan,
                            children: vec![],
                        }],
                    }],
                }],
            },
        ];
        let mut matched = false;
        for matcher in matchers {
            if matcher.matches(&subquery.subquery) {
                matched = true;
                break;
            }
        }
        if !matched {
            return Ok(None);
        }

        let filter_tree = subquery
            .subquery // EvalScalar
            .child(0)?; // Filter
        let filter_expr = RelExpr::with_s_expr(filter_tree);
        let filter: Filter = subquery
            .subquery // EvalScalar
            .child(0)? // Filter
            .plan()
            .clone()
            .try_into()?;
        let filter_prop = filter_expr.derive_relational_prop()?;
        let filter_child_prop = filter_expr.derive_relational_prop_child(0)?;

        let input_expr = RelExpr::with_s_expr(input);
        let input_prop = input_expr.derive_relational_prop()?;

        // First, we will check if all the outer columns are in the filter.
        if !filter_child_prop.outer_columns.is_empty() {
            return Ok(None);
        }

        // Second, we will check if the filter only contains equi-predicates.
        // This is not necessary, but it is a good heuristic for most cases.
        let mut left_conditions = vec![];
        let mut right_conditions = vec![];
        let mut non_equi_conditions = vec![];
        let mut left_filters = vec![];
        let mut right_filters = vec![];
        for pred in filter.predicates.iter() {
            let join_condition = JoinPredicate::new(pred, &input_prop, &filter_prop);
            match join_condition {
                JoinPredicate::Left(filter) | JoinPredicate::ALL(filter) => {
                    left_filters.push(filter.clone());
                }
                JoinPredicate::Right(filter) => {
                    right_filters.push(filter.clone());
                }

                JoinPredicate::Other(pred) => {
                    non_equi_conditions.push(pred.clone());
                }

                JoinPredicate::Both {
                    left,
                    right,
                    is_equal_op,
                    ..
                } => {
                    if is_equal_op {
                        left_conditions.push(left.clone());
                        right_conditions.push(right.clone());
                    } else {
                        non_equi_conditions.push(pred.clone());
                    }
                }
            }
        }

        let join = Join {
            equi_conditions: JoinEquiCondition::new_conditions(
                left_conditions,
                right_conditions,
                vec![],
            ),
            non_equi_conditions,
            join_type: match &subquery.typ {
                SubqueryType::Any | SubqueryType::All | SubqueryType::Scalar => {
                    return Ok(None);
                }
                SubqueryType::Exists => JoinType::LeftSemi,
                SubqueryType::NotExists => JoinType::LeftAnti,
            },
            marker_index: None,
            from_correlated_subquery: true,
            need_hold_hash_table: false,
            is_lateral: false,
            single_to_inner: None,
            build_side_cache_info: None,
        };

        // Rewrite plan to semi-join.
        let mut left_child = input.clone();
        if !left_filters.is_empty() {
            left_child = SExpr::create_unary(
                Arc::new(
                    Filter {
                        predicates: left_filters,
                    }
                    .into(),
                ),
                Arc::new(left_child),
            );
        }

        // Remove `Filter` from subquery.
        let mut right_child = subquery
            .subquery
            .replace_children(vec![Arc::new(filter_tree.child(0)?.clone())]);
        if !right_filters.is_empty() {
            right_child = SExpr::create_unary(
                Arc::new(
                    Filter {
                        predicates: right_filters,
                    }
                    .into(),
                ),
                Arc::new(right_child),
            );
        }

        let result = SExpr::create_binary(
            Arc::new(join.into()),
            Arc::new(left_child),
            Arc::new(right_child),
        );

        Ok(Some(result))
    }

    pub fn try_decorrelate_subquery(
        &mut self,
        left: &SExpr,
        subquery: &SubqueryExpr,
        flatten_info: &mut FlattenInfo,
        is_conjunctive_predicate: bool,
    ) -> Result<(SExpr, UnnestResult)> {
        match subquery.typ {
            SubqueryType::Scalar => {
                let correlated_columns = subquery.outer_columns.clone();
                let flatten_plan = self.flatten_plan(
                    &subquery.subquery,
                    &correlated_columns,
                    flatten_info,
                    false,
                )?;
                // Construct single join
                let mut left_conditions = Vec::with_capacity(correlated_columns.len());
                let mut right_conditions = Vec::with_capacity(correlated_columns.len());
                self.add_equi_conditions(
                    subquery.span,
                    &correlated_columns,
                    &mut right_conditions,
                    &mut left_conditions,
                )?;

                let mut join_type = JoinType::LeftSingle;
                if subquery.contain_agg.unwrap() {
                    let rel_expr = RelExpr::with_s_expr(&subquery.subquery);
                    let card = rel_expr
                        .derive_cardinality()?
                        .statistics
                        .precise_cardinality;

                    if card.is_some() {
                        join_type = JoinType::Left;
                    }
                }

                let join_plan = Join {
                    equi_conditions: JoinEquiCondition::new_conditions(
                        left_conditions,
                        right_conditions,
                        vec![],
                    ),
                    non_equi_conditions: vec![],
                    join_type,
                    marker_index: None,
                    from_correlated_subquery: true,
                    need_hold_hash_table: false,
                    is_lateral: false,
                    single_to_inner: None,
                    build_side_cache_info: None,
                };
                let s_expr = SExpr::create_binary(
                    Arc::new(join_plan.into()),
                    Arc::new(left.clone()),
                    Arc::new(flatten_plan),
                );
                Ok((s_expr, UnnestResult::SingleJoin))
            }
            SubqueryType::Exists | SubqueryType::NotExists => {
                if is_conjunctive_predicate {
                    if let Some(result) = self.try_decorrelate_simple_subquery(left, subquery)? {
                        return Ok((result, UnnestResult::SimpleJoin { output_index: None }));
                    }
                }
                let correlated_columns = subquery.outer_columns.clone();
                let flatten_plan = self.flatten_plan(
                    &subquery.subquery,
                    &correlated_columns,
                    flatten_info,
                    false,
                )?;
                // Construct mark join
                let mut left_conditions = Vec::with_capacity(correlated_columns.len());
                let mut right_conditions = Vec::with_capacity(correlated_columns.len());
                self.add_equi_conditions(
                    subquery.span,
                    &correlated_columns,
                    &mut left_conditions,
                    &mut right_conditions,
                )?;

                let marker_index = if let Some(idx) = subquery.projection_index {
                    idx
                } else {
                    self.metadata.write().add_derived_column(
                        "marker".to_string(),
                        DataType::Nullable(Box::new(DataType::Boolean)),
                        None,
                    )
                };
                let join_plan = Join {
                    equi_conditions: JoinEquiCondition::new_conditions(
                        right_conditions,
                        left_conditions,
                        vec![],
                    ),
                    non_equi_conditions: vec![],
                    join_type: JoinType::RightMark,
                    marker_index: Some(marker_index),
                    from_correlated_subquery: true,
                    need_hold_hash_table: false,
                    is_lateral: false,
                    single_to_inner: None,
                    build_side_cache_info: None,
                };
                let s_expr = SExpr::create_binary(
                    Arc::new(join_plan.into()),
                    Arc::new(left.clone()),
                    Arc::new(flatten_plan),
                );
                Ok((s_expr, UnnestResult::MarkJoin { marker_index }))
            }
            SubqueryType::Any => {
                let correlated_columns = subquery.outer_columns.clone();
                let flatten_plan = self.flatten_plan(
                    &subquery.subquery,
                    &correlated_columns,
                    flatten_info,
                    false,
                )?;
                let mut left_conditions = Vec::with_capacity(correlated_columns.len());
                let mut right_conditions = Vec::with_capacity(correlated_columns.len());
                self.add_equi_conditions(
                    subquery.span,
                    &correlated_columns,
                    &mut left_conditions,
                    &mut right_conditions,
                )?;
                let output_column = subquery.output_column.clone();
                let column_name = format!("subquery_{}", output_column.index);
                let right_condition = ScalarExpr::BoundColumnRef(BoundColumnRef {
                    span: subquery.span,
                    column: ColumnBindingBuilder::new(
                        column_name,
                        output_column.index,
                        output_column.data_type,
                        Visibility::Visible,
                    )
                    .build(),
                });
                let child_expr = *subquery.child_expr.as_ref().unwrap().clone();
                let op = *subquery.compare_op.as_ref().unwrap();
                // Make <child_expr op right_condition> as non_equi_conditions even if op is equal operator.
                // Because it's not null-safe.
                let non_equi_conditions = vec![ScalarExpr::FunctionCall(FunctionCall {
                    span: subquery.span,
                    func_name: op.to_func_name().to_string(),
                    params: vec![],
                    arguments: vec![child_expr, right_condition],
                })];
                let marker_index = if let Some(idx) = subquery.projection_index {
                    idx
                } else {
                    self.metadata.write().add_derived_column(
                        "marker".to_string(),
                        DataType::Nullable(Box::new(DataType::Boolean)),
                        None,
                    )
                };
                let mark_join = Join {
                    equi_conditions: JoinEquiCondition::new_conditions(
                        right_conditions,
                        left_conditions,
                        vec![],
                    ),
                    non_equi_conditions,
                    join_type: JoinType::RightMark,
                    marker_index: Some(marker_index),
                    from_correlated_subquery: true,
                    need_hold_hash_table: false,
                    is_lateral: false,
                    single_to_inner: None,
                    build_side_cache_info: None,
                }
                .into();
                Ok((
                    SExpr::create_binary(
                        Arc::new(mark_join),
                        Arc::new(left.clone()),
                        Arc::new(flatten_plan),
                    ),
                    UnnestResult::MarkJoin { marker_index },
                ))
            }
            _ => unreachable!(),
        }
    }

    pub fn add_equi_conditions(
        &self,
        span: Span,
        correlated_columns: &HashSet<IndexType>,
        left_conditions: &mut Vec<ScalarExpr>,
        right_conditions: &mut Vec<ScalarExpr>,
    ) -> Result<()> {
        let mut correlated_columns = correlated_columns.clone().into_iter().collect::<Vec<_>>();
        correlated_columns.sort();
        for correlated_column in correlated_columns.iter() {
            let metadata = self.metadata.read();
            let column_entry = metadata.column(*correlated_column);
            let right_column = ScalarExpr::BoundColumnRef(BoundColumnRef {
                span,
                column: ColumnBindingBuilder::new(
                    column_entry.name(),
                    *correlated_column,
                    Box::from(column_entry.data_type()),
                    Visibility::Visible,
                )
                .table_index(column_entry.table_index())
                .build(),
            });
            let derive_column = self.derived_columns.get(correlated_column).unwrap();
            let column_entry = metadata.column(*derive_column);
            let left_column = ScalarExpr::BoundColumnRef(BoundColumnRef {
                span,
                column: ColumnBindingBuilder::new(
                    column_entry.name(),
                    *derive_column,
                    Box::from(column_entry.data_type()),
                    Visibility::Visible,
                )
                .table_index(column_entry.table_index())
                .build(),
            });
            left_conditions.push(left_column);
            right_conditions.push(right_column);
        }
        Ok(())
    }

    // Check if need to join outer and inner table
    // If correlated_columns only occur in equi-conditions, such as `where t1.a = t.a and t1.b = t.b`(t1 is outer table)
    // Then we won't join outer and inner table.
    pub(crate) fn join_outer_inner_table(
        &mut self,
        filter: &Filter,
        correlated_columns: &ColumnSet,
    ) -> Result<bool> {
        Ok(!filter.predicates.iter().all(|predicate| {
            if predicate
                .used_columns()
                .iter()
                .any(|column| correlated_columns.contains(column))
            {
                if let ScalarExpr::FunctionCall(func) = predicate {
                    if func.func_name == "eq" {
                        if let (
                            ScalarExpr::BoundColumnRef(left),
                            ScalarExpr::BoundColumnRef(right),
                        ) = (&func.arguments[0], &func.arguments[1])
                        {
                            if correlated_columns.contains(&left.column.index)
                                && !correlated_columns.contains(&right.column.index)
                            {
                                self.derived_columns
                                    .insert(left.column.index, right.column.index);
                            }
                            if !correlated_columns.contains(&left.column.index)
                                && correlated_columns.contains(&right.column.index)
                            {
                                self.derived_columns
                                    .insert(right.column.index, left.column.index);
                            }
                            return true;
                        }
                    }
                }
                return false;
            }
            true
        }))
    }
}
