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

use common_exception::Result;
use common_expression::types::DataType;
use common_expression::TableDataType;

use crate::binder::satisfied_by;
use crate::optimizer::RelExpr;
use crate::optimizer::RelationalProperty;
use crate::optimizer::SExpr;
use crate::plans::AggregateFunction;
use crate::plans::AndExpr;
use crate::plans::BoundColumnRef;
use crate::plans::CastExpr;
use crate::plans::ComparisonExpr;
use crate::plans::Filter;
use crate::plans::FunctionCall;
use crate::plans::Join;
use crate::plans::JoinType;
use crate::plans::NotExpr;
use crate::plans::OrExpr;
use crate::plans::WindowFunc;
use crate::plans::WindowFuncType;
use crate::plans::WindowOrderBy;
use crate::ColumnBinding;
use crate::ColumnEntry;
use crate::ColumnSet;
use crate::IndexType;
use crate::MetadataRef;
use crate::ScalarExpr;

pub fn convert_outer_to_inner_join(s_expr: &SExpr) -> Result<(SExpr, bool)> {
    let filter: Filter = s_expr.plan().clone().try_into()?;
    let mut join: Join = s_expr.child(0)?.plan().clone().try_into()?;
    let origin_join_type = join.join_type.clone();
    if !origin_join_type.is_outer_join() {
        return Ok((s_expr.clone(), false));
    }
    let s_join_expr = s_expr.child(0)?;
    let join_expr = RelExpr::with_s_expr(s_join_expr);
    let left_child_output_column = join_expr.derive_relational_prop_child(0)?.output_columns;
    let right_child_output_column = join_expr.derive_relational_prop_child(1)?.output_columns;
    let predicates = &filter.predicates;
    let mut nullable_columns: Vec<IndexType> = vec![];
    for predicate in predicates {
        find_nullable_columns(
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
        return Ok((s_expr.clone(), false));
    }
    let mut result = SExpr::create_binary(
        join.into(),
        s_join_expr.child(0)?.clone(),
        s_join_expr.child(1)?.clone(),
    );
    // wrap filter s_expr
    result = SExpr::create_unary(filter.into(), result);
    Ok((result, true))
}

#[allow(clippy::only_used_in_recursion)]
fn find_nullable_columns(
    predicate: &ScalarExpr,
    left_output_columns: &ColumnSet,
    right_output_columns: &ColumnSet,
    nullable_columns: &mut Vec<IndexType>,
) -> Result<()> {
    match predicate {
        ScalarExpr::BoundColumnRef(column_binding) => {
            nullable_columns.push(column_binding.column.index);
        }
        ScalarExpr::AndExpr(expr) => {
            let mut left_cols = vec![];
            let mut right_cols = vec![];
            find_nullable_columns(
                &expr.left,
                left_output_columns,
                right_output_columns,
                &mut left_cols,
            )?;
            find_nullable_columns(
                &expr.right,
                left_output_columns,
                right_output_columns,
                &mut right_cols,
            )?;
        }
        ScalarExpr::OrExpr(expr) => {
            let mut left_cols = vec![];
            let mut right_cols = vec![];
            find_nullable_columns(
                &expr.left,
                left_output_columns,
                right_output_columns,
                &mut left_cols,
            )?;
            find_nullable_columns(
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
        ScalarExpr::NotExpr(expr) => {
            find_nullable_columns(
                &expr.argument,
                left_output_columns,
                right_output_columns,
                nullable_columns,
            )?;
        }
        ScalarExpr::ComparisonExpr(expr) => {
            // For any comparison expr, if input is null, the compare result is false
            find_nullable_columns(
                &expr.left,
                left_output_columns,
                right_output_columns,
                nullable_columns,
            )?;
            find_nullable_columns(
                &expr.right,
                left_output_columns,
                right_output_columns,
                nullable_columns,
            )?;
        }
        ScalarExpr::CastExpr(expr) => {
            find_nullable_columns(
                &expr.argument,
                left_output_columns,
                right_output_columns,
                nullable_columns,
            )?;
        }
        _ => {}
    }
    Ok(())
}

// If outer join is converted to inner join, we need to change datatype of filter predicate
pub fn remove_nullable(
    s_expr: &SExpr,
    predicate: &ScalarExpr,
    join_type: &JoinType,
    metadata: MetadataRef,
) -> Result<ScalarExpr> {
    let join_expr = s_expr.child(0)?;

    let rel_expr = RelExpr::with_s_expr(join_expr);
    let left_prop = rel_expr.derive_relational_prop_child(0)?;
    let right_prop = rel_expr.derive_relational_prop_child(1)?;

    remove_column_nullable(predicate, &left_prop, &right_prop, join_type, metadata)
}

fn remove_column_nullable(
    scalar_expr: &ScalarExpr,
    left_prop: &RelationalProperty,
    right_prop: &RelationalProperty,
    join_type: &JoinType,
    metadata: MetadataRef,
) -> Result<ScalarExpr> {
    Ok(match scalar_expr {
        ScalarExpr::BoundColumnRef(column) => {
            let mut data_type = column.column.data_type.clone();
            let metadata = metadata.read();
            let column_entry = metadata.column(column.column.index);
            let mut need_remove = true;
            // If the column type is nullable when the table is created
            // Do not need to remove nullable.
            match column_entry {
                ColumnEntry::BaseTableColumn(base) => {
                    if let TableDataType::Nullable(_) = base.data_type {
                        need_remove = false;
                    }
                }
                ColumnEntry::DerivedColumn(derived) => {
                    if let DataType::Nullable(_) = derived.data_type {
                        need_remove = false;
                    }
                }
                // None of internal columns will be nullable, so just ignore internal column type entry
                ColumnEntry::InternalColumn(..) => {}
                ColumnEntry::VirtualColumn(virtual_column) => {
                    if let TableDataType::Nullable(_) = virtual_column.data_type {
                        need_remove = false;
                    }
                }
            }
            match join_type {
                JoinType::Left => {
                    if satisfied_by(scalar_expr, right_prop) && need_remove {
                        data_type = Box::new(column.column.data_type.remove_nullable());
                    }
                }
                JoinType::Right => {
                    if satisfied_by(scalar_expr, left_prop) && need_remove {
                        data_type = Box::new(column.column.data_type.remove_nullable());
                    }
                }
                JoinType::Full => {
                    if need_remove {
                        data_type = Box::new(column.column.data_type.remove_nullable())
                    }
                }
                _ => {}
            };
            ScalarExpr::BoundColumnRef(BoundColumnRef {
                span: column.span,
                column: ColumnBinding {
                    database_name: column.column.database_name.clone(),
                    table_name: column.column.table_name.clone(),
                    table_index: column.column.table_index,
                    column_name: column.column.column_name.clone(),
                    index: column.column.index,
                    data_type,
                    visibility: column.column.visibility.clone(),
                },
            })
        }
        ScalarExpr::BoundInternalColumnRef(_) => {
            // internal column will never be null
            unreachable!()
        }
        ScalarExpr::AndExpr(expr) => {
            let left_expr = remove_column_nullable(
                &expr.left,
                left_prop,
                right_prop,
                join_type,
                metadata.clone(),
            )?;
            let right_expr =
                remove_column_nullable(&expr.right, left_prop, right_prop, join_type, metadata)?;
            ScalarExpr::AndExpr(AndExpr {
                left: Box::new(left_expr),
                right: Box::new(right_expr),
            })
        }
        ScalarExpr::OrExpr(expr) => {
            let left_expr = remove_column_nullable(
                &expr.left,
                left_prop,
                right_prop,
                join_type,
                metadata.clone(),
            )?;
            let right_expr =
                remove_column_nullable(&expr.right, left_prop, right_prop, join_type, metadata)?;
            ScalarExpr::OrExpr(OrExpr {
                left: Box::new(left_expr),
                right: Box::new(right_expr),
            })
        }
        ScalarExpr::NotExpr(expr) => {
            let new_expr =
                remove_column_nullable(&expr.argument, left_prop, right_prop, join_type, metadata)?;
            ScalarExpr::NotExpr(NotExpr {
                argument: Box::new(new_expr),
            })
        }
        ScalarExpr::ComparisonExpr(expr) => {
            let left_expr = remove_column_nullable(
                &expr.left,
                left_prop,
                right_prop,
                join_type,
                metadata.clone(),
            )?;
            let right_expr =
                remove_column_nullable(&expr.right, left_prop, right_prop, join_type, metadata)?;
            ScalarExpr::ComparisonExpr(ComparisonExpr {
                op: expr.op.clone(),
                left: Box::new(left_expr),
                right: Box::new(right_expr),
            })
        }
        ScalarExpr::WindowFunction(expr) => {
            let func = match &expr.func {
                WindowFuncType::Aggregate(agg) => {
                    let args = agg
                        .args
                        .iter()
                        .map(|arg| {
                            remove_column_nullable(
                                arg,
                                left_prop,
                                right_prop,
                                join_type,
                                metadata.clone(),
                            )
                        })
                        .collect::<Result<Vec<_>>>()?;

                    WindowFuncType::Aggregate(AggregateFunction {
                        display_name: agg.display_name.clone(),
                        func_name: agg.func_name.clone(),
                        distinct: agg.distinct,
                        params: agg.params.clone(),
                        args,
                        return_type: agg.return_type.clone(),
                    })
                }
                func => func.clone(),
            };
            let mut partition_by = Vec::with_capacity(expr.partition_by.len());
            for arg in expr.partition_by.iter() {
                partition_by.push(remove_column_nullable(
                    arg,
                    left_prop,
                    right_prop,
                    join_type,
                    metadata.clone(),
                )?);
            }
            let mut order_by = Vec::with_capacity(expr.order_by.len());
            for arg in expr.order_by.iter() {
                let new_expr = remove_column_nullable(
                    &arg.expr,
                    left_prop,
                    right_prop,
                    join_type,
                    metadata.clone(),
                )?;
                order_by.push(WindowOrderBy {
                    expr: new_expr,
                    nulls_first: arg.nulls_first,
                    asc: arg.asc,
                });
            }
            ScalarExpr::WindowFunction(WindowFunc {
                display_name: expr.display_name.clone(),
                func,
                partition_by,
                order_by,
                frame: expr.frame.clone(),
            })
        }
        ScalarExpr::AggregateFunction(expr) => {
            let mut args = Vec::with_capacity(expr.args.len());
            for arg in expr.args.iter() {
                args.push(remove_column_nullable(
                    arg,
                    left_prop,
                    right_prop,
                    join_type,
                    metadata.clone(),
                )?);
            }
            ScalarExpr::AggregateFunction(AggregateFunction {
                display_name: expr.display_name.clone(),
                func_name: expr.func_name.clone(),
                distinct: expr.distinct,
                params: expr.params.clone(),
                args,
                return_type: expr.return_type.clone(),
            })
        }
        ScalarExpr::FunctionCall(expr) => {
            let mut args = Vec::with_capacity(expr.arguments.len());
            for arg in expr.arguments.iter() {
                args.push(remove_column_nullable(
                    arg,
                    left_prop,
                    right_prop,
                    join_type,
                    metadata.clone(),
                )?);
            }
            ScalarExpr::FunctionCall(FunctionCall {
                span: expr.span,
                params: expr.params.clone(),
                arguments: args,
                func_name: expr.func_name.clone(),
            })
        }
        ScalarExpr::CastExpr(expr) => {
            let new_expr =
                remove_column_nullable(&expr.argument, left_prop, right_prop, join_type, metadata)?;
            ScalarExpr::CastExpr(CastExpr {
                span: expr.span,
                is_try: expr.is_try,
                argument: Box::new(new_expr),
                target_type: expr.target_type.clone(),
            })
        }
        ScalarExpr::ConstantExpr(_) | ScalarExpr::SubqueryExpr(_) => scalar_expr.clone(),
    })
}
