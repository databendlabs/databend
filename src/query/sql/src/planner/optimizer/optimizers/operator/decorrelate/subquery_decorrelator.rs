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
use crate::optimizer::Optimizer;
use crate::optimizer::OptimizerContext;
use crate::plans::Aggregate;
use crate::plans::AggregateFunction;
use crate::plans::BoundColumnRef;
use crate::plans::CastExpr;
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
use crate::plans::Sort;
use crate::plans::SubqueryComparisonOp;
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

/// Transforms SQL subqueries into join operations for more efficient execution.
/// This optimizer handles several types of subqueries and converts them into
/// appropriate join operations based on their characteristics.
///
/// ## Transformation Examples Implemented in This File
///
/// ### 1. Uncorrelated Scalar Subquery
///
/// SQL Example:
/// ```sql
/// SELECT o.id, o.total / (SELECT AVG(total) FROM orders) as ratio
/// FROM orders o
/// ```
///
/// Plan Tree Transformation:
///
/// Before:
/// Project: o.id, o.total / (SELECT AVG(total) FROM orders)
/// └── TableScan: orders AS o
///
/// After:
/// Project: o.id, o.total / scalar_subquery_column
/// └── LeftSingleJoin
///     ├── TableScan: orders AS o
///     └── Aggregate: AVG(total) AS scalar_subquery_column
///         └── TableScan: orders
///
/// ### 2. Uncorrelated EXISTS Subquery
///
/// SQL Example:
/// ```sql
/// SELECT c.id, c.name
/// FROM customers c
/// WHERE EXISTS (SELECT 1 FROM orders)
/// ```
///
/// Plan Tree Transformation:
///
/// Before:
/// Filter: EXISTS (SELECT 1 FROM orders)
/// └── TableScan: customers AS c
///
/// After:
/// Filter: is_true(exists_scalar)
/// └── CrossJoin
///     ├── TableScan: customers AS c
///     └── Limit: 1
///         └── Aggregate: COUNT(*) = 1
///             └── TableScan: orders
///
/// ### 3. Uncorrelated ANY/IN Subquery
///
/// SQL Example:
/// ```sql
/// SELECT e.id, e.name
/// FROM employees e
/// WHERE e.department_id = ANY (SELECT d.id FROM departments d)
/// ```
///
/// Plan Tree Transformation:
///
/// Before:
/// Filter: e.department_id = ANY (SELECT d.id FROM departments d)
/// └── TableScan: employees AS e
///
/// After:
/// Filter: is_true(marker_column)
/// └── RightMarkJoin: (d.id = e.department_id)
///     ├── TableScan: employees AS e
///     └── TableScan: departments AS d
pub struct SubqueryDecorrelatorOptimizer {
    pub(crate) ctx: Arc<dyn TableContext>,
    pub(crate) metadata: MetadataRef,
    pub(crate) derived_columns: HashMap<IndexType, IndexType>,
    pub(crate) binder: Option<Binder>,
}

impl SubqueryDecorrelatorOptimizer {
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
    pub fn optimize_sync(&mut self, s_expr: &SExpr) -> Result<SExpr> {
        // If there is no subquery, return directly
        if !s_expr.has_subquery() {
            return Ok(s_expr.clone());
        }

        match s_expr.plan() {
            RelOperator::EvalScalar(eval) => {
                let mut outer = self.optimize_sync(s_expr.unary_child())?;
                let mut eval = eval.clone();
                for item in eval.items.iter_mut() {
                    (item.scalar, outer) = self.try_rewrite_subquery(&item.scalar, outer, false)?;
                }
                Ok(SExpr::create_unary(Arc::new(eval.into()), Arc::new(outer)))
            }

            RelOperator::Filter(plan) => {
                let mut plan = plan.clone();
                let mut outer = self.optimize_sync(s_expr.unary_child())?;
                for pred in plan.predicates.iter_mut() {
                    (*pred, outer) = self.try_rewrite_subquery(pred, outer, true)?;
                }
                Ok(SExpr::create_unary(Arc::new(plan.into()), Arc::new(outer)))
            }

            RelOperator::ProjectSet(plan) => {
                let mut plan = plan.clone();
                let mut outer = self.optimize_sync(s_expr.unary_child())?;
                for item in plan.srfs.iter_mut() {
                    (item.scalar, outer) = self.try_rewrite_subquery(&item.scalar, outer, false)?;
                }
                Ok(SExpr::create_unary(Arc::new(plan.into()), Arc::new(outer)))
            }

            RelOperator::Aggregate(plan) => {
                let mut plan = plan.clone();
                let mut outer = self.optimize_sync(s_expr.unary_child())?;
                for item in plan.group_items.iter_mut() {
                    (item.scalar, outer) = self.try_rewrite_subquery(&item.scalar, outer, false)?;
                }
                for item in plan.aggregate_functions.iter_mut() {
                    (item.scalar, outer) = self.try_rewrite_subquery(&item.scalar, outer, false)?;
                }
                Ok(SExpr::create_unary(Arc::new(plan.into()), Arc::new(outer)))
            }

            RelOperator::Window(plan) => {
                let mut plan = plan.clone();
                let mut outer = self.optimize_sync(s_expr.unary_child())?;

                for item in plan.partition_by.iter_mut() {
                    (item.scalar, outer) = self.try_rewrite_subquery(&item.scalar, outer, false)?;
                }

                for item in plan.order_by.iter_mut() {
                    (item.order_by_item.scalar, outer) =
                        self.try_rewrite_subquery(&item.order_by_item.scalar, outer, false)?;
                }

                if let WindowFuncType::Aggregate(agg) = &mut plan.function {
                    for item in agg.exprs_mut() {
                        (*item, outer) = self.try_rewrite_subquery(item, outer, false)?;
                    }
                }

                Ok(SExpr::create_unary(Arc::new(plan.into()), Arc::new(outer)))
            }

            RelOperator::Sort(sort) => {
                let mut outer = self.optimize_sync(s_expr.unary_child())?;

                let Some(mut window) = sort.window_partition.clone() else {
                    return Ok(SExpr::create_unary(s_expr.plan.clone(), Arc::new(outer)));
                };

                for item in window.partition_by.iter_mut() {
                    (item.scalar, outer) = self.try_rewrite_subquery(&item.scalar, outer, false)?;
                }
                let sort = Sort {
                    window_partition: Some(window),
                    ..sort.clone()
                };

                Ok(SExpr::create_unary(Arc::new(sort.into()), Arc::new(outer)))
            }

            RelOperator::Join(join) => {
                let mut left = self.optimize_sync(s_expr.left_child())?;
                let mut right = self.optimize_sync(s_expr.right_child())?;
                if !join.has_subquery() {
                    return Ok(SExpr::create_binary(
                        s_expr.plan.clone(),
                        Arc::new(left),
                        Arc::new(right),
                    ));
                }

                let mut equi_conditions = join.equi_conditions.clone();
                for condition in equi_conditions.iter_mut() {
                    (condition.left, left) =
                        self.try_rewrite_subquery(&condition.left, left, false)?;
                    (condition.right, right) =
                        self.try_rewrite_subquery(&condition.right, right, false)?;
                }

                // todo: non_equi_conditions for other join_type
                if matches!(join.join_type, JoinType::Inner(_))
                    || join
                        .non_equi_conditions
                        .iter()
                        .all(|condition| !condition.has_subquery())
                {
                    let join = Join {
                        equi_conditions,
                        ..join.clone()
                    };
                    return Ok(SExpr::create_binary(
                        Arc::new(join.into()),
                        Arc::new(left),
                        Arc::new(right),
                    ));
                }

                let mut predicates = join.non_equi_conditions.clone();
                let join = Join {
                    equi_conditions,
                    non_equi_conditions: vec![],
                    ..join.clone()
                };
                let mut outer =
                    SExpr::create_binary(Arc::new(join.into()), Arc::new(left), Arc::new(right));

                for pred in predicates.iter_mut() {
                    (*pred, outer) = self.try_rewrite_subquery(pred, outer, true)?;
                }
                let filter = Filter { predicates };
                return Ok(SExpr::create_unary(
                    Arc::new(filter.into()),
                    Arc::new(outer),
                ));
            }

            RelOperator::UnionAll(_) | RelOperator::Sequence(_) => Ok(SExpr::create_binary(
                s_expr.plan.clone(),
                Arc::new(self.optimize_sync(s_expr.left_child())?),
                Arc::new(self.optimize_sync(s_expr.right_child())?),
            )),

            RelOperator::Limit(_)
            | RelOperator::Udf(_)
            | RelOperator::AsyncFunction(_)
            | RelOperator::MaterializedCTE(_) => Ok(SExpr::create_unary(
                s_expr.plan.clone(),
                Arc::new(self.optimize_sync(s_expr.unary_child())?),
            )),

            RelOperator::DummyTableScan(_)
            | RelOperator::Scan(_)
            | RelOperator::ConstantTableScan(_)
            | RelOperator::ExpressionScan(_)
            | RelOperator::SecureFilter(_)
            | RelOperator::CacheScan(_)
            | RelOperator::Exchange(_)
            | RelOperator::RecursiveCteScan(_)
            | RelOperator::Mutation(_)
            | RelOperator::MutationSource(_)
            | RelOperator::MaterializedCTERef(_)
            | RelOperator::CompactBlock(_) => Ok(s_expr.clone()),
        }
    }

    /// Try to extract subquery from a scalar expression. Returns replaced scalar expression
    /// and the outer s_expr.
    fn try_rewrite_subquery(
        &mut self,
        scalar: &ScalarExpr,
        mut outer: SExpr,
        is_conjunctive_predicate: bool,
    ) -> Result<(ScalarExpr, SExpr)> {
        match scalar {
            ScalarExpr::AsyncFunctionCall(_)
            | ScalarExpr::BoundColumnRef(_)
            | ScalarExpr::ConstantExpr(_)
            | ScalarExpr::TypedConstantExpr(_, _)
            | ScalarExpr::WindowFunction(_)
            | ScalarExpr::AggregateFunction(_)
            | ScalarExpr::LambdaFunction(_) => Ok((scalar.clone(), outer)),

            ScalarExpr::CastExpr(cast) => {
                let (argument, outer) = self.try_rewrite_subquery(&cast.argument, outer, false)?;
                let cast = CastExpr {
                    argument: Box::new(argument),
                    ..cast.clone()
                };
                Ok((cast.into(), outer))
            }
            ScalarExpr::UDFLambdaCall(udf) => {
                let (scalar, outer) = self.try_rewrite_subquery(&udf.scalar, outer, false)?;
                let expr = UDFLambdaCall {
                    scalar: Box::new(scalar),
                    ..udf.clone()
                };
                Ok((expr.into(), outer))
            }

            ScalarExpr::FunctionCall(func) => {
                let mut arguments = func.arguments.clone();
                for arg in arguments.iter_mut() {
                    (*arg, outer) = self.try_rewrite_subquery(arg, outer, false)?;
                }
                let expr = FunctionCall {
                    arguments,
                    ..func.clone()
                };
                Ok((expr.into(), outer))
            }
            ScalarExpr::UDFCall(udf) => {
                let mut arguments = udf.arguments.clone();
                for arg in arguments.iter_mut() {
                    (*arg, outer) = self.try_rewrite_subquery(arg, outer, false)?;
                }
                let expr = UDFCall {
                    arguments,
                    ..udf.clone()
                };
                Ok((expr.into(), outer))
            }
            ScalarExpr::UDAFCall(udaf) => {
                let mut arguments = udaf.arguments.clone();
                for arg in arguments.iter_mut() {
                    (*arg, outer) = self.try_rewrite_subquery(arg, outer, false)?;
                }
                let expr = UDAFCall {
                    arguments,
                    ..udaf.clone()
                };

                Ok((expr.into(), outer))
            }

            ScalarExpr::SubqueryExpr(subquery) => {
                // Rewrite subquery recursively
                let mut subquery = subquery.clone();
                subquery.subquery = Box::new(self.optimize_sync(&subquery.subquery)?);

                if let Some(constant_subquery) = self.try_fold_constant_subquery(&subquery)? {
                    return Ok((constant_subquery, outer));
                }

                // Check if the subquery is a correlated subquery.
                // If it is, we'll try to flatten it and rewrite to join.
                // If it is not, we'll just rewrite it to join
                let prop = subquery.subquery.derive_relational_prop()?;
                let mut flatten_info = FlattenInfo {
                    from_count_func: false,
                };
                let (outer, result) = if prop.outer_columns.is_empty() {
                    self.try_rewrite_uncorrelated_subquery(
                        outer,
                        &subquery,
                        is_conjunctive_predicate,
                    )?
                } else {
                    // todo: optimize outer before decorrelate subquery
                    self.try_decorrelate_subquery(
                        &outer,
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
                    return Ok((scalar_expr, outer));
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
                    // Not exists subquery should be rewritten to `not(is_true(column_ref))`
                    // not null mark value will consider as:  not [null] ---> not [false] ---> true
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
                    // null value will consider as false
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
                Ok((scalar, outer))
            }
        }
    }

    fn try_rewrite_uncorrelated_subquery(
        &mut self,
        outer: SExpr,
        subquery: &SubqueryExpr,
        is_conjunctive_predicate: bool,
    ) -> Result<(SExpr, UnnestResult)> {
        match subquery.typ {
            SubqueryType::Scalar => {
                let join_plan = Join {
                    join_type: JoinType::LeftSingle,
                    ..Join::default()
                };
                let s_expr = SExpr::create_binary(
                    Arc::new(join_plan.into()),
                    Arc::new(outer),
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
                    lazy_columns: Default::default(),
                };
                subquery_expr =
                    SExpr::create_unary(Arc::new(limit.into()), Arc::new(subquery_expr));

                // We will rewrite EXISTS subquery into the form `COUNT(*) = 1`.
                // For example, `EXISTS(SELECT a FROM t WHERE a > 1)` will be rewritten into
                // `(SELECT COUNT(*) = 1 FROM t WHERE a > 1 LIMIT 1)`.
                let count_type = AggregateCountFunction::try_create("", vec![], vec![], vec![])?
                    .return_type()?;
                let count_func_index = self.metadata.write().add_derived_column(
                    "count(*)".to_string(),
                    count_type.clone(),
                    None,
                );

                let agg = Aggregate {
                    aggregate_functions: vec![ScalarItem {
                        scalar: AggregateFunction {
                            span: subquery.span,
                            display_name: "count(*)".to_string(),
                            func_name: "count".to_string(),
                            distinct: false,
                            params: vec![],
                            args: vec![],
                            return_type: Box::new(count_type.clone()),
                            sort_descs: vec![],
                        }
                        .into(),
                        index: count_func_index,
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
                                count_func_index,
                                Box::new(count_type),
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
                    join_type: JoinType::Cross(false),
                    equi_conditions: JoinEquiCondition::new_conditions(vec![], vec![], vec![]),
                    ..Join::default()
                };
                Ok((
                    SExpr::create_binary(
                        Arc::new(cross_join.into()),
                        Arc::new(outer),
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
                let op = subquery.compare_op.as_ref().unwrap().clone();
                let (right_condition, is_non_equi_condition) =
                    check_child_expr_in_subquery(&child_expr, &op)?;
                let (left_conditions, right_conditions, non_equi_conditions) =
                    if !is_non_equi_condition {
                        (vec![left_condition], vec![right_condition], vec![])
                    } else {
                        let other_condition = ScalarExpr::FunctionCall(op.to_func_call(
                            subquery.span,
                            right_condition,
                            left_condition,
                        ));
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

                let mut is_null_equal = Vec::new();
                for (i, (l, r)) in left_conditions
                    .iter()
                    .zip(right_conditions.iter())
                    .enumerate()
                {
                    if l.data_type()?.is_nullable() || r.data_type()?.is_nullable() {
                        is_null_equal.push(i);
                    }
                }

                // Consider the sql: select * from t1 where t1.a = any(select t2.a from t2);
                // Will be transferred to:select t1.a, t2.a, marker_index from t1, t2 where t2.a = t1.a;
                // Note that subquery is the right table, and it'll be the build side.
                let mark_join = Join {
                    equi_conditions: JoinEquiCondition::new_conditions(
                        right_conditions,
                        left_conditions,
                        is_null_equal,
                    ),
                    non_equi_conditions,
                    join_type: JoinType::RightMark,
                    marker_index: Some(marker_index),
                    ..Join::default()
                };
                let s_expr = SExpr::create_binary(
                    Arc::new(mark_join.into()),
                    Arc::new(outer),
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
    op: &SubqueryComparisonOp,
) -> Result<(ScalarExpr, bool)> {
    match child_expr {
        ScalarExpr::BoundColumnRef(_) => {
            Ok((child_expr.clone(), op != &SubqueryComparisonOp::Equal))
        }
        ScalarExpr::FunctionCall(func) => {
            for arg in &func.arguments {
                let _ = check_child_expr_in_subquery(arg, op)?;
            }
            Ok((child_expr.clone(), op != &SubqueryComparisonOp::Equal))
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

#[async_trait::async_trait]
impl Optimizer for SubqueryDecorrelatorOptimizer {
    /// Returns the name of this optimizer
    fn name(&self) -> String {
        "SubqueryDecorrelatorOptimizer".to_string()
    }

    /// Optimize the expression by rewriting subqueries
    async fn optimize(&mut self, expr: &SExpr) -> Result<SExpr> {
        self.optimize_sync(expr)
    }
}
