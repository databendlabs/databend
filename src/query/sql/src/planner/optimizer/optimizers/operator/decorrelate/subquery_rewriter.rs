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
use std::vec;

use databend_common_catalog::table_context::TableContext;
use databend_common_exception::ErrorCode;
use databend_common_exception::Result;
use databend_common_expression::types::DataType;
use databend_common_expression::types::NumberDataType;
use databend_common_expression::types::NumberScalar;
use databend_common_expression::Scalar;
use databend_common_functions::aggregates::AggregateCountFunction;

use crate::binder::wrap_cast;
use crate::binder::ColumnBindingBuilder;
use crate::binder::Visibility;
use crate::optimizer::ir::SExpr;
use crate::optimizer::OptimizerContext;
use crate::plans::Aggregate;
use crate::plans::AggregateFunction;
use crate::plans::BoundColumnRef;
use crate::plans::CastExpr;
use crate::plans::ComparisonOp;
use crate::plans::ConstantExpr;
use crate::plans::EvalScalar;
use crate::plans::Filter;
use crate::plans::FunctionCall;
use crate::plans::Join;
use crate::plans::JoinEquiCondition;
use crate::plans::JoinType;
use crate::plans::Limit;
use crate::plans::RelOperator;
use crate::plans::ScalarExpr;
use crate::plans::ScalarItem;
use crate::plans::SubqueryExpr;
use crate::plans::SubqueryType;
use crate::plans::UDAFCall;
use crate::plans::UDFCall;
use crate::plans::UDFLambdaCall;
use crate::plans::WindowFuncType;
use crate::Binder;
use crate::IndexType;
use crate::MetadataRef;

#[allow(clippy::enum_variant_names)]
#[derive(Debug)]
pub enum UnnestResult {
    // Semi/Anti Join, Cross join for EXISTS
    SimpleJoin { output_index: Option<IndexType> },
    MarkJoin { marker_index: IndexType },
    SingleJoin,
}

pub struct FlattenInfo {
    pub from_count_func: bool,
}

/// Rewrite subquery into `Apply` operator
pub struct SubqueryRewriter {
    pub(crate) ctx: Arc<dyn TableContext>,
    pub(crate) metadata: MetadataRef,
    pub(crate) derived_columns: HashMap<IndexType, IndexType>,
    pub(crate) binder: Option<Binder>,
}

impl SubqueryRewriter {
    pub fn new(opt_ctx: Arc<OptimizerContext>, binder: Option<Binder>) -> Self {
        Self {
            ctx: opt_ctx.get_table_ctx(),
            metadata: opt_ctx.get_metadata(),
            derived_columns: Default::default(),
            binder,
        }
    }

    /// Decorrelate subqueries inside `s_expr`.
    ///
    /// We only need to process three kinds of join: Scalar Subquery, Any Subquery, and Exists Subquery.
    /// Other kinds of subqueries have been converted to one of the above subqueries in `type_check`.
    ///
    /// It will rewrite `s_expr` to all kinds of join.
    /// Correlated scalar subquery -> Single join
    /// Any subquery -> Marker join
    /// Correlated exists subquery -> Marker join
    ///
    /// More information can be found in the paper: Unnesting Arbitrary Queries
    #[recursive::recursive]
    pub fn optimize(&mut self, s_expr: &SExpr) -> Result<SExpr> {
        // If there is no subquery, return directly
        if !s_expr.contain_subquery() {
            return Ok(s_expr.clone());
        }

        match s_expr.plan().clone() {
            RelOperator::EvalScalar(mut plan) => {
                let mut input = self.optimize(s_expr.child(0)?)?;

                for item in plan.items.iter_mut() {
                    let res = self.try_rewrite_subquery(&item.scalar, &input, false)?;
                    input = res.1;
                    item.scalar = res.0;
                }

                Ok(SExpr::create_unary(Arc::new(plan.into()), Arc::new(input)))
            }
            RelOperator::Filter(mut plan) => {
                let mut input = self.optimize(s_expr.child(0)?)?;
                for pred in plan.predicates.iter_mut() {
                    let res = self.try_rewrite_subquery(pred, &input, true)?;
                    input = res.1;
                    *pred = res.0;
                }

                Ok(SExpr::create_unary(Arc::new(plan.into()), Arc::new(input)))
            }
            RelOperator::ProjectSet(mut plan) => {
                let mut input = self.optimize(s_expr.child(0)?)?;
                for item in plan.srfs.iter_mut() {
                    let res = self.try_rewrite_subquery(&item.scalar, &input, false)?;
                    input = res.1;
                    item.scalar = res.0
                }

                Ok(SExpr::create_unary(Arc::new(plan.into()), Arc::new(input)))
            }
            RelOperator::Aggregate(mut plan) => {
                let mut input = self.optimize(s_expr.child(0)?)?;

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

                Ok(SExpr::create_unary(Arc::new(plan.into()), Arc::new(input)))
            }

            RelOperator::Window(mut plan) => {
                let mut input = self.optimize(s_expr.child(0)?)?;

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
                    for item in agg.exprs_mut() {
                        let res = self.try_rewrite_subquery(item, &input, false)?;
                        input = res.1;
                        *item = res.0;
                    }
                }

                Ok(SExpr::create_unary(Arc::new(plan.into()), Arc::new(input)))
            }

            RelOperator::Sort(mut sort) => {
                let mut input = self.optimize(s_expr.child(0)?)?;

                if let Some(window) = &mut sort.window_partition {
                    for item in window.partition_by.iter_mut() {
                        let res = self.try_rewrite_subquery(&item.scalar, &input, false)?;
                        input = res.1;
                        item.scalar = res.0;
                    }
                }

                Ok(SExpr::create_unary(Arc::new(sort.into()), Arc::new(input)))
            }

            RelOperator::Join(_) | RelOperator::UnionAll(_) => Ok(SExpr::create_binary(
                Arc::new(s_expr.plan().clone()),
                Arc::new(self.optimize(s_expr.child(0)?)?),
                Arc::new(self.optimize(s_expr.child(1)?)?),
            )),

            RelOperator::Limit(_) | RelOperator::Udf(_) | RelOperator::AsyncFunction(_) => {
                Ok(SExpr::create_unary(
                    Arc::new(s_expr.plan().clone()),
                    Arc::new(self.optimize(s_expr.child(0)?)?),
                ))
            }

            RelOperator::DummyTableScan(_)
            | RelOperator::Scan(_)
            | RelOperator::ConstantTableScan(_)
            | RelOperator::ExpressionScan(_)
            | RelOperator::CacheScan(_)
            | RelOperator::Exchange(_)
            | RelOperator::RecursiveCteScan(_)
            | RelOperator::Mutation(_)
            | RelOperator::MutationSource(_)
            | RelOperator::CompactBlock(_) => Ok(s_expr.clone()),
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
            ScalarExpr::AsyncFunctionCall(_) => Ok((scalar.clone(), s_expr.clone())),
            ScalarExpr::BoundColumnRef(_) => Ok((scalar.clone(), s_expr.clone())),
            ScalarExpr::ConstantExpr(_) => Ok((scalar.clone(), s_expr.clone())),
            ScalarExpr::TypedConstantExpr(_, _) => Ok((scalar.clone(), s_expr.clone())),
            ScalarExpr::WindowFunction(_) => Ok((scalar.clone(), s_expr.clone())),
            ScalarExpr::AggregateFunction(_) => Ok((scalar.clone(), s_expr.clone())),
            ScalarExpr::LambdaFunction(_) => Ok((scalar.clone(), s_expr.clone())),
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
                subquery.subquery = Box::new(self.optimize(&subquery.subquery)?);

                if let Some(constant_subquery) = self.try_fold_constant_subquery(&subquery)? {
                    return Ok((constant_subquery, s_expr.clone()));
                }

                // Check if the subquery is a correlated subquery.
                // If it is, we'll try to flatten it and rewrite to join.
                // If it is not, we'll just rewrite it to join
                let prop = subquery.subquery.derive_relational_prop()?;
                let mut flatten_info = FlattenInfo {
                    from_count_func: false,
                };
                let (s_expr, result) = if prop.outer_columns.is_empty() {
                    self.try_rewrite_uncorrelated_subquery(
                        s_expr,
                        &subquery,
                        is_conjunctive_predicate,
                    )?
                } else {
                    // todo: optimize outer before decorrelate subquery
                    self.try_decorrelate_subquery(
                        s_expr,
                        &subquery,
                        &mut flatten_info,
                        is_conjunctive_predicate,
                    )?
                };

                // If we unnest the subquery into a simple join, then we can replace the
                // original predicate with a `TRUE` literal to eliminate the conjunction.
                if let UnnestResult::SimpleJoin { output_index } = result {
                    let scalar_expr = if let Some(output_index) = output_index {
                        ScalarExpr::BoundColumnRef(BoundColumnRef {
                            span: subquery.span,
                            column: ColumnBindingBuilder::new(
                                "exists_scalar".to_string(),
                                output_index,
                                Box::new(DataType::Boolean),
                                Visibility::Visible,
                            )
                            .build(),
                        })
                    } else {
                        ScalarExpr::ConstantExpr(ConstantExpr {
                            span: subquery.span,
                            value: Scalar::Boolean(true),
                        })
                    };
                    return Ok((scalar_expr, s_expr));
                }

                let data_type = if subquery.typ == SubqueryType::Scalar {
                    Box::new(subquery.data_type.wrap_nullable())
                } else if matches! {result, UnnestResult::MarkJoin {..}} {
                    Box::new(DataType::Nullable(Box::new(DataType::Boolean)))
                } else {
                    subquery.data_type.clone()
                };

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

                let column_ref = ScalarExpr::BoundColumnRef(BoundColumnRef {
                    span: subquery.span,
                    column: ColumnBindingBuilder::new(name, index, data_type, Visibility::Visible)
                        .build(),
                });

                let scalar = if flatten_info.from_count_func && subquery.typ == SubqueryType::Scalar
                {
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
                            func_name: "if".to_string(),
                            params: vec![],
                            arguments: vec![is_not_null, cast_column_ref_to_uint64, zero],
                        })),
                        target_type: Box::new(
                            DataType::Number(NumberDataType::UInt64).wrap_nullable(),
                        ),
                    })
                } else if subquery.typ == SubqueryType::NotExists {
                    ScalarExpr::FunctionCall(FunctionCall {
                        span: subquery.span,
                        func_name: "not".to_string(),
                        params: vec![],
                        arguments: vec![ScalarExpr::FunctionCall(FunctionCall {
                            span: subquery.span,
                            func_name: "is_true".to_string(),
                            params: vec![],
                            arguments: vec![column_ref],
                        })],
                    })
                } else if subquery.typ == SubqueryType::Exists {
                    ScalarExpr::FunctionCall(FunctionCall {
                        span: subquery.span,
                        func_name: "is_true".to_string(),
                        params: vec![],
                        arguments: vec![column_ref],
                    })
                } else {
                    column_ref
                };
                // After finishing rewriting subquery, we should clear the derived columns.
                self.derived_columns.clear();
                Ok((scalar, s_expr))
            }
            ScalarExpr::UDFCall(udf) => {
                let mut args = vec![];
                let mut s_expr = s_expr.clone();
                for arg in udf.arguments.iter() {
                    let res = self.try_rewrite_subquery(arg, &s_expr, false)?;
                    s_expr = res.1;
                    args.push(res.0);
                }

                let expr: ScalarExpr = UDFCall {
                    span: udf.span,
                    name: udf.name.clone(),
                    handler: udf.handler.clone(),
                    headers: udf.headers.clone(),
                    display_name: udf.display_name.clone(),
                    udf_type: udf.udf_type.clone(),
                    arg_types: udf.arg_types.clone(),
                    return_type: udf.return_type.clone(),
                    arguments: args,
                }
                .into();

                Ok((expr, s_expr))
            }
            ScalarExpr::UDFLambdaCall(udf) => {
                let mut s_expr = s_expr.clone();
                let res = self.try_rewrite_subquery(&udf.scalar, &s_expr, false)?;
                s_expr = res.1;

                let expr: ScalarExpr = UDFLambdaCall {
                    span: udf.span,
                    func_name: udf.func_name.clone(),
                    scalar: Box::new(res.0),
                }
                .into();

                Ok((expr, s_expr))
            }
            ScalarExpr::UDAFCall(udaf) => {
                let mut args = vec![];
                let mut s_expr = s_expr.clone();
                for arg in udaf.arguments.iter() {
                    let res = self.try_rewrite_subquery(arg, &s_expr, false)?;
                    s_expr = res.1;
                    args.push(res.0);
                }

                let expr: ScalarExpr = UDAFCall {
                    span: udaf.span,
                    name: udaf.name.clone(),
                    display_name: udaf.display_name.clone(),
                    udf_type: udaf.udf_type.clone(),
                    arg_types: udaf.arg_types.clone(),
                    state_fields: udaf.state_fields.clone(),
                    return_type: udaf.return_type.clone(),
                    arguments: args,
                }
                .into();

                Ok((expr, s_expr))
            }
        }
    }

    fn try_rewrite_uncorrelated_subquery(
        &mut self,
        left: &SExpr,
        subquery: &SubqueryExpr,
        is_conjunctive_predicate: bool,
    ) -> Result<(SExpr, UnnestResult)> {
        match subquery.typ {
            SubqueryType::Scalar => {
                let join_plan = Join {
                    non_equi_conditions: vec![],
                    join_type: JoinType::LeftSingle,
                    marker_index: None,
                    from_correlated_subquery: false,
                    equi_conditions: vec![],
                    need_hold_hash_table: false,
                    is_lateral: false,
                    single_to_inner: None,
                    build_side_cache_info: None,
                }
                .into();
                let s_expr = SExpr::create_binary(
                    Arc::new(join_plan),
                    Arc::new(left.clone()),
                    Arc::new(*subquery.subquery.clone()),
                );
                Ok((s_expr, UnnestResult::SingleJoin))
            }
            SubqueryType::Exists | SubqueryType::NotExists => {
                let mut subquery_expr = *subquery.subquery.clone();
                // Wrap Limit to current subquery
                let limit = Limit {
                    limit: Some(1),
                    offset: 0,
                    before_exchange: false,
                };
                subquery_expr =
                    SExpr::create_unary(Arc::new(limit.into()), Arc::new(subquery_expr.clone()));

                // We will rewrite EXISTS subquery into the form `COUNT(*) = 1`.
                // For example, `EXISTS(SELECT a FROM t WHERE a > 1)` will be rewritten into
                // `(SELECT COUNT(*) = 1 FROM t WHERE a > 1 LIMIT 1)`.
                let agg_func = AggregateCountFunction::try_create("", vec![], vec![], vec![])?;
                let agg_func_index = self.metadata.write().add_derived_column(
                    "count(*)".to_string(),
                    agg_func.return_type()?,
                    None,
                );

                let agg = Aggregate {
                    group_items: vec![],
                    aggregate_functions: vec![ScalarItem {
                        scalar: AggregateFunction {
                            span: subquery.span,
                            display_name: "count(*)".to_string(),
                            func_name: "count".to_string(),
                            distinct: false,
                            params: vec![],
                            args: vec![],
                            return_type: Box::new(agg_func.return_type()?),
                            sort_descs: vec![],
                        }
                        .into(),
                        index: agg_func_index,
                    }],
                    ..Default::default()
                };

                let compare = FunctionCall {
                    span: subquery.span,
                    func_name: if subquery.typ == SubqueryType::Exists {
                        "eq".to_string()
                    } else {
                        "noteq".to_string()
                    },
                    params: vec![],
                    arguments: vec![
                        BoundColumnRef {
                            span: subquery.span,
                            column: ColumnBindingBuilder::new(
                                "count(*)".to_string(),
                                agg_func_index,
                                Box::new(agg_func.return_type()?),
                                Visibility::Visible,
                            )
                            .build(),
                        }
                        .into(),
                        ScalarExpr::ConstantExpr(ConstantExpr {
                            span: subquery.span,
                            value: Scalar::Number(NumberScalar::UInt64(1)),
                        }),
                    ],
                };

                let agg_s_expr = Arc::new(SExpr::create_unary(
                    Arc::new(agg.into()),
                    Arc::new(subquery_expr),
                ));

                let mut output_index = None;
                let rewritten_subquery = if is_conjunctive_predicate {
                    let filter = Filter {
                        predicates: vec![compare.into()],
                    };
                    // Filter: COUNT(*) = 1 or COUNT(*) != 1
                    // └── Aggregate: COUNT(*)
                    SExpr::create_unary(Arc::new(filter.into()), agg_s_expr)
                } else {
                    let column_index = self.metadata.write().add_derived_column(
                        "_exists_scalar_subquery".to_string(),
                        DataType::Boolean,
                        None,
                    );
                    output_index = Some(column_index);
                    let eval_scalar = EvalScalar {
                        items: vec![ScalarItem {
                            scalar: compare.into(),
                            index: column_index,
                        }],
                    };
                    SExpr::create_unary(Arc::new(eval_scalar.into()), agg_s_expr)
                };

                let cross_join = Join {
                    equi_conditions: JoinEquiCondition::new_conditions(vec![], vec![], vec![]),
                    non_equi_conditions: vec![],
                    join_type: JoinType::Cross,
                    marker_index: None,
                    from_correlated_subquery: false,
                    need_hold_hash_table: false,
                    is_lateral: false,
                    single_to_inner: None,
                    build_side_cache_info: None,
                }
                .into();
                Ok((
                    SExpr::create_binary(
                        Arc::new(cross_join),
                        Arc::new(left.clone()),
                        Arc::new(rewritten_subquery),
                    ),
                    UnnestResult::SimpleJoin { output_index },
                ))
            }
            SubqueryType::Any => {
                let output_column = subquery.output_column.clone();
                let column_name = format!("subquery_{}", output_column.index);
                let left_condition = wrap_cast(
                    &ScalarExpr::BoundColumnRef(BoundColumnRef {
                        span: subquery.span,
                        column: ColumnBindingBuilder::new(
                            column_name,
                            output_column.index,
                            output_column.data_type,
                            Visibility::Visible,
                        )
                        .table_index(output_column.table_index)
                        .build(),
                    }),
                    &subquery.data_type,
                );
                let child_expr = *subquery.child_expr.as_ref().unwrap().clone();
                let op = *subquery.compare_op.as_ref().unwrap();
                let (right_condition, is_non_equi_condition) =
                    check_child_expr_in_subquery(&child_expr, &op)?;
                let (left_conditions, right_conditions, non_equi_conditions) =
                    if !is_non_equi_condition {
                        (vec![left_condition], vec![right_condition], vec![])
                    } else {
                        let other_condition = ScalarExpr::FunctionCall(FunctionCall {
                            span: subquery.span,
                            func_name: op.to_func_name().to_string(),
                            params: vec![],
                            arguments: vec![right_condition, left_condition],
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
                        None,
                    )
                };
                // Consider the sql: select * from t1 where t1.a = any(select t2.a from t2);
                // Will be transferred to:select t1.a, t2.a, marker_index from t1, t2 where t2.a = t1.a;
                // Note that subquery is the right table, and it'll be the build side.
                let mark_join = Join {
                    equi_conditions: JoinEquiCondition::new_conditions(
                        right_conditions,
                        left_conditions,
                        vec![],
                    ),
                    non_equi_conditions,
                    join_type: JoinType::RightMark,
                    marker_index: Some(marker_index),
                    from_correlated_subquery: false,
                    need_hold_hash_table: false,
                    is_lateral: false,
                    single_to_inner: None,
                    build_side_cache_info: None,
                }
                .into();
                let s_expr = SExpr::create_binary(
                    Arc::new(mark_join),
                    Arc::new(left.clone()),
                    Arc::new(*subquery.subquery.clone()),
                );
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
        ScalarExpr::FunctionCall(func) => {
            for arg in &func.arguments {
                let _ = check_child_expr_in_subquery(arg, op)?;
            }
            Ok((child_expr.clone(), op != &ComparisonOp::Equal))
        }
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
