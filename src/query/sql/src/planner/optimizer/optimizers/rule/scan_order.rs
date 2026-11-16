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

//! Push a sort order down to the scans that feed an operator, so the physical
//! plan can register runtime TopN filters on them. The order is a hint for the
//! scans: it changes neither the rows they return nor the result.

use std::sync::Arc;

use databend_common_expression::types::DataType;

use crate::Symbol;
use crate::optimizer::ir::SExpr;
use crate::plans::RelOperator;
use crate::plans::ScalarExpr;
use crate::plans::SortItem;
use crate::plans::UnionAll;

/// Install `order_by` on the scans reached from `s_expr`, or return `None` if
/// no scan changed.
///
/// The traversal crosses the row-preserving `Filter`/`EvalScalar` and
/// non-recursive `UNION ALL`, translating the keys into the columns of each
/// branch. Every row above a union comes from one of its branches, so a
/// runtime boundary over the union output is valid for each branch scan. A
/// branch is skipped when a key is coerced to a type with other values than
/// the branch column, since boundary values could not be compared with the
/// branch statistics. Other operators stop the traversal of their subtree.
#[recursive::recursive]
pub(crate) fn push_down_scan_order(s_expr: &SExpr, order_by: &[SortItem]) -> Option<SExpr> {
    match s_expr.plan() {
        RelOperator::Scan(scan) => {
            // Keep an ordering installed by another rule, and only order by
            // columns the scan itself produces.
            if scan.order_by.is_some()
                || !order_by
                    .iter()
                    .all(|item| scan.columns.contains(&item.index))
            {
                return None;
            }
            let mut scan = scan.clone();
            scan.order_by = Some(order_by.to_vec());
            Some(s_expr.replace_plan(RelOperator::Scan(scan)))
        }
        RelOperator::Filter(_) | RelOperator::EvalScalar(_) => {
            let child = push_down_scan_order(s_expr.unary_child(), order_by)?;
            Some(s_expr.replace_children([Arc::new(child)]))
        }
        RelOperator::UnionAll(union_all) if union_all.cte_scan_names.is_empty() => {
            let left = union_branch_order(union_all, &union_all.left_outputs, order_by)
                .and_then(|order_by| push_down_scan_order(s_expr.left_child(), &order_by));
            let right = union_branch_order(union_all, &union_all.right_outputs, order_by)
                .and_then(|order_by| push_down_scan_order(s_expr.right_child(), &order_by));
            if left.is_none() && right.is_none() {
                return None;
            }
            Some(s_expr.replace_children([
                left.map_or_else(|| s_expr.left_child_arc(), Arc::new),
                right.map_or_else(|| s_expr.right_child_arc(), Arc::new),
            ]))
        }
        _ => None,
    }
}

/// Translate `order_by` on union output columns into the columns of one
/// branch, if every key keeps the branch values.
fn union_branch_order(
    union_all: &UnionAll,
    outputs: &[(Symbol, Option<ScalarExpr>)],
    order_by: &[SortItem],
) -> Option<Vec<SortItem>> {
    order_by
        .iter()
        .map(|item| {
            let position = union_all
                .output_indexes
                .iter()
                .position(|index| *index == item.index)?;
            let (symbol, coercion) = outputs.get(position)?;
            if let Some(coercion) = coercion
                && !is_nullable_cast_of(coercion, *symbol)
            {
                return None;
            }
            Some(SortItem {
                index: *symbol,
                asc: item.asc,
                nulls_first: item.nulls_first,
            })
        })
        .collect()
}

/// Whether `expr` casts column `symbol` to its nullable type, which keeps the
/// values and their order. The binder coerces union branches with such casts.
fn is_nullable_cast_of(expr: &ScalarExpr, symbol: Symbol) -> bool {
    let ScalarExpr::CastExpr(cast) = expr else {
        return false;
    };
    let ScalarExpr::BoundColumnRef(column_ref) = cast.argument.as_ref() else {
        return false;
    };
    !cast.is_try
        && column_ref.column.index == symbol
        && matches!(cast.target_type.as_ref(), DataType::Nullable(_))
        && column_ref.column.data_type.remove_nullable() == cast.target_type.remove_nullable()
}
