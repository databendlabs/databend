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

use std::collections::HashMap;

use common_exception::ErrorCode;
use common_exception::Result;
use common_expression::types::DataType;
use common_expression::types::NumberDataType;
use common_expression::types::NumberScalar;
use common_expression::Scalar;
use common_functions::aggregates::AggregateCountFunction;

use crate::binder::wrap_cast;
use crate::binder::ColumnBinding;
use crate::binder::Visibility;
use crate::optimizer::RelExpr;
use crate::optimizer::SExpr;
use crate::plans::Aggregate;
use crate::plans::AggregateFunction;
use crate::plans::AggregateMode;
use crate::plans::AndExpr;
use crate::plans::BoundColumnRef;
use crate::plans::CastExpr;
use crate::plans::ComparisonExpr;
use crate::plans::ComparisonOp;
use crate::plans::ConstantExpr;
use crate::plans::Filter;
use crate::plans::FunctionCall;
use crate::plans::Join;
use crate::plans::JoinType;
use crate::plans::Limit;
use crate::plans::NotExpr;
use crate::plans::OrExpr;
use crate::plans::RelOperator;
use crate::plans::ScalarExpr;
use crate::plans::ScalarItem;
use crate::plans::SubqueryExpr;
use crate::plans::SubqueryType;
use crate::plans::WindowFuncType;
use crate::IndexType;
use crate::MetadataRef;

#[allow(clippy::enum_variant_names)]
pub enum UnnestResult {
    // Semi/Anti Join, Cross join for EXISTS
    SimpleJoin,
    MarkJoin { marker_index: IndexType },
    SingleJoin,
}

pub struct FlattenInfo {
    pub from_count_func: bool,
}

/// Rewrite subquery into `Apply` operator
pub struct SubqueryRewriter {
    pub(crate) metadata: MetadataRef,
    pub(crate) derived_columns: HashMap<IndexType, IndexType>,
}

impl SubqueryRewriter {
    pub fn new(metadata: MetadataRef) -> Self {
        Self {
            metadata,
            derived_columns: Default::default(),
        }
    }

    pub fn rewrite(&mut self, s_expr: &SExpr) -> Result<SExpr> {
        match s_expr.plan().clone() {
            RelOperator::EvalScalar(mut plan) => {
                let mut input = self.rewrite(s_expr.child(0)?)?;

                for item in plan.items.iter_mut() {
                    let res = self.try_rewrite_subquery(&item.scalar, &input, false)?;
                    input = res.1;
                    item.scalar = res.0;
                }

                Ok(SExpr::create_unary(plan.into(), input))
            }
            RelOperator::Filter(mut plan) => {
                let mut input = self.rewrite(s_expr.child(0)?)?;
                for pred in plan.predicates.iter_mut() {
                    let res = self.try_rewrite_subquery(pred, &input, true)?;
                    input = res.1;
                    *pred = res.0;
                }

                Ok(SExpr::create_unary(plan.into(), input))
            }
            RelOperator::Aggregate(mut plan) => {
                let mut input = self.rewrite(s_expr.child(0)?)?;

                for item in plan.group_items.iter_mut() {
                    let res = self.try_rewrite_subquery(&item.scalar, &input, false)?;
                    input = res.1;
                    item.scalar = res.0;
                }

                for item in plan.aggregate_functions.iter_mut() {
                    let res = self.try_rewrite_subquery(&item.scalar, &input, false)?;
                    input = res.1;
                    item.scalar = res.0;
                }

                Ok(SExpr::create_unary(plan.into(), input))
            }

            RelOperator::Window(mut plan) => {
                let mut input = self.rewrite(s_expr.child(0)?)?;

                for item in plan.partition_by.iter_mut() {
                    let res = self.try_rewrite_subquery(&item.scalar, &input, false)?;
                    input = res.1;
                    item.scalar = res.0;
                }

                for item in plan.order_by.iter_mut() {
                    let res =
                        self.try_rewrite_subquery(&item.order_by_item.scalar, &input, false)?;
                    input = res.1;
                    item.order_by_item.scalar = res.0;
                }

                if let WindowFuncType::Aggregate(agg) = &mut plan.function {
                    for item in agg.args.iter_mut() {
                        let res = self.try_rewrite_subquery(item, &input, false)?;
                        input = res.1;
                        *item = res.0;
                    }
                }

                Ok(SExpr::create_unary(plan.into(), input))
            }

            RelOperator::Join(_) | RelOperator::UnionAll(_) => Ok(SExpr::create_binary(
                s_expr.plan().clone(),
                self.rewrite(s_expr.child(0)?)?,
                self.rewrite(s_expr.child(1)?)?,
            )),

            RelOperator::Limit(_) | RelOperator::Sort(_) => Ok(SExpr::create_unary(
                s_expr.plan().clone(),
                self.rewrite(s_expr.child(0)?)?,
            )),

            RelOperator::DummyTableScan(_) | RelOperator::Scan(_) => Ok(s_expr.clone()),

            _ => Err(ErrorCode::Internal("Invalid plan type")),
        }
    }

    /// Try to extract subquery from a scalar expression. Returns replaced scalar expression
    /// and the subqueries.
    fn try_rewrite_subquery(
        &mut self,
        scalar: &ScalarExpr,
        s_expr: &SExpr,
        is_conjunctive_predicate: bool,
    ) -> Result<(ScalarExpr, SExpr)> {
        match scalar {
            ScalarExpr::BoundColumnRef(_) => Ok((scalar.clone(), s_expr.clone())),
            ScalarExpr::BoundInternalColumnRef(_) => Ok((scalar.clone(), s_expr.clone())),

            ScalarExpr::ConstantExpr(_) => Ok((scalar.clone(), s_expr.clone())),

            ScalarExpr::AndExpr(expr) => {
                // Notice that the conjunctions has been flattened in binder, if we encounter
                // a AND here, we can't treat it as a conjunction.
                let (left, s_expr) = self.try_rewrite_subquery(&expr.left, s_expr, false)?;
                let (right, s_expr) = self.try_rewrite_subquery(&expr.right, &s_expr, false)?;
                Ok((
                    AndExpr {
                        left: Box::new(left),
                        right: Box::new(right),
                    }
                    .into(),
                    s_expr,
                ))
            }

            ScalarExpr::OrExpr(expr) => {
                let (left, s_expr) = self.try_rewrite_subquery(&expr.left, s_expr, false)?;
                let (right, s_expr) = self.try_rewrite_subquery(&expr.right, &s_expr, false)?;
                Ok((
                    OrExpr {
                        left: Box::new(left),
                        right: Box::new(right),
                    }
                    .into(),
                    s_expr,
                ))
            }

            ScalarExpr::NotExpr(expr) => {
                let (argument, s_expr) =
                    self.try_rewrite_subquery(&expr.argument, s_expr, false)?;
                Ok((
                    NotExpr {
                        argument: Box::new(argument),
                    }
                    .into(),
                    s_expr,
                ))
            }

            ScalarExpr::ComparisonExpr(expr) => {
                let (left, s_expr) = self.try_rewrite_subquery(&expr.left, s_expr, false)?;
                let (right, s_expr) = self.try_rewrite_subquery(&expr.right, &s_expr, false)?;
                Ok((
                    ComparisonExpr {
                        op: expr.op.clone(),
                        left: Box::new(left),
                        right: Box::new(right),
                    }
                    .into(),
                    s_expr,
                ))
            }

            ScalarExpr::WindowFunction(_) => Ok((scalar.clone(), s_expr.clone())),

            ScalarExpr::AggregateFunction(_) => Ok((scalar.clone(), s_expr.clone())),

            ScalarExpr::FunctionCall(func) => {
                let mut args = vec![];
                let mut s_expr = s_expr.clone();
                for arg in func.arguments.iter() {
                    let res = self.try_rewrite_subquery(arg, &s_expr, false)?;
                    s_expr = res.1;
                    args.push(res.0);
                }

                let expr: ScalarExpr = FunctionCall {
                    span: func.span,
                    params: func.params.clone(),
                    arguments: args,
                    func_name: func.func_name.clone(),
                }
                .into();

                Ok((expr, s_expr))
            }

            ScalarExpr::CastExpr(cast) => {
                let (scalar, s_expr) = self.try_rewrite_subquery(&cast.argument, s_expr, false)?;
                Ok((
                    CastExpr {
                        span: cast.span,
                        is_try: cast.is_try,
                        argument: Box::new(scalar),
                        target_type: cast.target_type.clone(),
                    }
                    .into(),
                    s_expr,
                ))
            }

            ScalarExpr::SubqueryExpr(subquery) => {
                // Rewrite subquery recursively
                let mut subquery = subquery.clone();
                subquery.subquery = Box::new(self.rewrite(&subquery.subquery)?);

                // Check if the subquery is a correlated subquery.
                // If it is, we'll try to flatten it and rewrite to join.
                // If it is not, we'll just rewrite it to join
                let rel_expr = RelExpr::with_s_expr(&subquery.subquery);
                let prop = rel_expr.derive_relational_prop()?;
                let mut flatten_info = FlattenInfo {
                    from_count_func: false,
                };
                let (s_expr, result) = if prop.outer_columns.is_empty() {
                    self.try_rewrite_uncorrelated_subquery(s_expr, &subquery)?
                } else {
                    self.try_decorrelate_subquery(
                        s_expr,
                        &subquery,
                        &mut flatten_info,
                        is_conjunctive_predicate,
                    )?
                };

                // If we unnest the subquery into a simple join, then we can replace the
                // original predicate with a `TRUE` literal to eliminate the conjunction.
                if matches!(result, UnnestResult::SimpleJoin) {
                    return Ok((
                        ScalarExpr::ConstantExpr(ConstantExpr {
                            span: subquery.span,
                            value: Scalar::Boolean(true),
                        }),
                        s_expr,
                    ));
                }
                let (index, name) = if let UnnestResult::MarkJoin { marker_index } = result {
                    (marker_index, marker_index.to_string())
                } else if let UnnestResult::SingleJoin = result {
                    let mut output_column = subquery.output_column;
                    if let Some(index) = self.derived_columns.get(&output_column.index) {
                        output_column.index = *index;
                    }
                    (
                        output_column.index,
                        format!("scalar_subquery_{:?}", output_column.index),
                    )
                } else {
                    let index = subquery.output_column.index;
                    (index, format!("subquery_{}", index))
                };

                let data_type = if subquery.typ == SubqueryType::Scalar {
                    Box::new(subquery.data_type.wrap_nullable())
                } else if matches! {result, UnnestResult::MarkJoin {..}} {
                    Box::new(DataType::Nullable(Box::new(DataType::Boolean)))
                } else {
                    subquery.data_type.clone()
                };

                let column_ref = ScalarExpr::BoundColumnRef(BoundColumnRef {
                    span: subquery.span,
                    column: ColumnBinding {
                        database_name: None,
                        table_name: None,
                        table_index: None,
                        column_name: name,
                        index,
                        data_type,
                        visibility: Visibility::Visible,
                    },
                });

                let scalar = if flatten_info.from_count_func {
                    // convert count aggregate function to `if(count() is not null, count(), 0)`
                    let is_not_null = ScalarExpr::FunctionCall(FunctionCall {
                        span: subquery.span,
                        func_name: "is_not_null".to_string(),
                        params: vec![],
                        arguments: vec![column_ref.clone()],
                    });
                    let cast_column_ref_to_uint64 = ScalarExpr::CastExpr(CastExpr {
                        span: subquery.span,
                        is_try: true,
                        argument: Box::new(column_ref),
                        target_type: Box::new(
                            DataType::Number(NumberDataType::UInt64).wrap_nullable(),
                        ),
                    });
                    let zero = ScalarExpr::ConstantExpr(ConstantExpr {
                        span: subquery.span,
                        value: Scalar::Number(NumberScalar::UInt8(0)),
                    });
                    ScalarExpr::CastExpr(CastExpr {
                        span: subquery.span,
                        is_try: true,
                        argument: Box::new(ScalarExpr::FunctionCall(FunctionCall {
                            span: subquery.span,
                            params: vec![],
                            arguments: vec![is_not_null, cast_column_ref_to_uint64, zero],
                            func_name: "if".to_string(),
                        })),
                        target_type: Box::new(
                            DataType::Number(NumberDataType::UInt64).wrap_nullable(),
                        ),
                    })
                } else if subquery.typ == SubqueryType::NotExists {
                    ScalarExpr::NotExpr(NotExpr {
                        argument: Box::new(column_ref),
                    })
                } else {
                    column_ref
                };

                Ok((scalar, s_expr))
            }
        }
    }

    fn try_rewrite_uncorrelated_subquery(
        &mut self,
        left: &SExpr,
        subquery: &SubqueryExpr,
    ) -> Result<(SExpr, UnnestResult)> {
        match subquery.typ {
            SubqueryType::Scalar => {
                let join_plan = Join {
                    left_conditions: vec![],
                    right_conditions: vec![],
                    non_equi_conditions: vec![],
                    join_type: JoinType::Single,
                    marker_index: None,
                    from_correlated_subquery: false,
                    contain_runtime_filter: false,
                }
                .into();
                let s_expr =
                    SExpr::create_binary(join_plan, left.clone(), *subquery.subquery.clone());
                Ok((s_expr, UnnestResult::SingleJoin))
            }
            SubqueryType::Exists | SubqueryType::NotExists => {
                let mut subquery_expr = *subquery.subquery.clone();
                // Wrap Limit to current subquery
                let limit = Limit {
                    limit: Some(1),
                    offset: 0,
                };
                subquery_expr = SExpr::create_unary(limit.into(), subquery_expr.clone());

                // We will rewrite EXISTS subquery into the form `COUNT(*) = 1`.
                // For example, `EXISTS(SELECT a FROM t WHERE a > 1)` will be rewritten into
                // `(SELECT COUNT(*) = 1 FROM t WHERE a > 1 LIMIT 1)`.
                let agg_func = AggregateCountFunction::try_create("", vec![], vec![])?;
                let agg_func_index = self
                    .metadata
                    .write()
                    .add_derived_column("count(*)".to_string(), agg_func.return_type()?);

                let agg = Aggregate {
                    group_items: vec![],
                    aggregate_functions: vec![ScalarItem {
                        scalar: AggregateFunction {
                            display_name: "count(*)".to_string(),
                            func_name: "count".to_string(),
                            distinct: false,
                            params: vec![],
                            args: vec![],
                            return_type: Box::new(agg_func.return_type()?),
                        }
                        .into(),
                        index: agg_func_index,
                    }],
                    from_distinct: false,
                    mode: AggregateMode::Initial,
                    limit: None,
                    grouping_id_index: 0,
                    grouping_sets: vec![],
                };

                let compare = ComparisonExpr {
                    op: if subquery.typ == SubqueryType::Exists {
                        ComparisonOp::Equal
                    } else {
                        ComparisonOp::NotEqual
                    },
                    left: Box::new(
                        BoundColumnRef {
                            span: subquery.span,
                            column: ColumnBinding {
                                database_name: None,
                                table_name: None,
                                table_index: None,
                                column_name: "count(*)".to_string(),
                                index: agg_func_index,
                                data_type: Box::new(agg_func.return_type()?),
                                visibility: Visibility::Visible,
                            },
                        }
                        .into(),
                    ),
                    right: Box::new(
                        ConstantExpr {
                            span: subquery.span,
                            value: common_expression::Scalar::Number(NumberScalar::UInt64(1)),
                        }
                        .into(),
                    ),
                };
                let filter = Filter {
                    predicates: vec![compare.into()],
                    is_having: false,
                };

                // Filter: COUNT(*) = 1 or COUNT(*) != 1
                //     Aggregate: COUNT(*)
                let rewritten_subquery = SExpr::create_unary(
                    filter.into(),
                    SExpr::create_unary(agg.into(), subquery_expr),
                );
                let cross_join = Join {
                    left_conditions: vec![],
                    right_conditions: vec![],
                    non_equi_conditions: vec![],
                    join_type: JoinType::Cross,
                    marker_index: None,
                    from_correlated_subquery: false,
                    contain_runtime_filter: false,
                }
                .into();
                Ok((
                    SExpr::create_binary(cross_join, left.clone(), rewritten_subquery),
                    UnnestResult::SimpleJoin,
                ))
            }
            SubqueryType::Any => {
                let output_column = subquery.output_column.clone();
                let column_name = format!("subquery_{}", output_column.index);
                let left_condition = wrap_cast(
                    &ScalarExpr::BoundColumnRef(BoundColumnRef {
                        span: subquery.span,
                        column: ColumnBinding {
                            database_name: None,
                            table_name: None,
                            table_index: None,
                            column_name,
                            index: output_column.index,
                            data_type: output_column.data_type,
                            visibility: Visibility::Visible,
                        },
                    }),
                    &subquery.data_type,
                );
                let child_expr = *subquery.child_expr.as_ref().unwrap().clone();
                let op = subquery.compare_op.as_ref().unwrap().clone();
                let (right_condition, is_non_equi_condition) =
                    check_child_expr_in_subquery(&child_expr, &op)?;
                let (left_conditions, right_conditions, non_equi_conditions) =
                    if !is_non_equi_condition {
                        (vec![left_condition], vec![right_condition], vec![])
                    } else {
                        let other_condition = ScalarExpr::ComparisonExpr(ComparisonExpr {
                            op,
                            left: Box::new(right_condition),
                            right: Box::new(left_condition),
                        });
                        (vec![], vec![], vec![other_condition])
                    };
                // Add a marker column to save comparison result.
                // The column is Nullable(Boolean), the data value is TRUE, FALSE, or NULL.
                // If subquery contains NULL, the comparison result is TRUE or NULL.
                // Such as t1.a => {1, 3, 4}, select t1.a in (1, 2, NULL) from t1; The sql will return {true, null, null}.
                // If subquery doesn't contain NULL, the comparison result is FALSE, TRUE, or NULL.
                let marker_index = if let Some(idx) = subquery.projection_index {
                    idx
                } else {
                    self.metadata.write().add_derived_column(
                        "marker".to_string(),
                        DataType::Nullable(Box::new(DataType::Boolean)),
                    )
                };
                // Consider the sql: select * from t1 where t1.a = any(select t2.a from t2);
                // Will be transferred to:select t1.a, t2.a, marker_index from t1, t2 where t2.a = t1.a;
                // Note that subquery is the right table, and it'll be the build side.
                let mark_join = Join {
                    left_conditions: right_conditions,
                    right_conditions: left_conditions,
                    non_equi_conditions,
                    join_type: JoinType::RightMark,
                    marker_index: Some(marker_index),
                    from_correlated_subquery: false,
                    contain_runtime_filter: false,
                }
                .into();
                let s_expr =
                    SExpr::create_binary(mark_join, left.clone(), *subquery.subquery.clone());
                Ok((s_expr, UnnestResult::MarkJoin { marker_index }))
            }
            _ => unreachable!(),
        }
    }
}

pub fn check_child_expr_in_subquery(
    child_expr: &ScalarExpr,
    op: &ComparisonOp,
) -> Result<(ScalarExpr, bool)> {
    match child_expr {
        ScalarExpr::BoundColumnRef(_) => Ok((child_expr.clone(), op != &ComparisonOp::Equal)),
        ScalarExpr::ConstantExpr(_) => Ok((child_expr.clone(), true)),
        ScalarExpr::CastExpr(cast) => {
            let arg = &cast.argument;
            let (_, is_non_equi_condition) = check_child_expr_in_subquery(arg, op)?;
            Ok((child_expr.clone(), is_non_equi_condition))
        }
        other => Err(ErrorCode::Internal(format!(
            "Invalid child expr in subquery: {:?}",
            other
        ))),
    }
}
