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
use databend_common_expression::Scalar;

use crate::MetadataRef;
use crate::optimizer::ir::SExpr;
use crate::plans::ConstantExpr;
use crate::plans::EvalScalar;
use crate::plans::Join;
use crate::plans::JoinType;
use crate::plans::ScalarExpr;
use crate::plans::ScalarItem;

/// Convert an outer-join exclusion pattern to the corresponding anti join when
/// the null-tested expression is a regular equi-key on the null-extended side:
///
/// ```text
/// Filter(right_key IS NULL)
///   LeftOuterJoin(left_key = right_key)
///
/// Filter(left_key IS NULL)
///   RightOuterJoin(left_key = right_key)
/// ```
///
/// A matched row cannot have a NULL regular equi-join key, even when the source
/// column itself is nullable. Therefore the filter keeps exactly the preserved
/// side's unmatched rows. Null-equal join conditions are deliberately excluded.
pub fn outer_join_to_anti_join(s_expr: &SExpr, metadata: MetadataRef) -> Result<Option<SExpr>> {
    let filter = s_expr.plan().as_filter().unwrap();
    let join_expr = s_expr.unary_child();
    let join = join_expr.plan().as_join().unwrap();
    let anti_join_type = match join.join_type {
        JoinType::Left => JoinType::LeftAnti,
        JoinType::Right => JoinType::RightAnti,
        _ => return Ok(None),
    };

    let Some(predicate_index) = filter.predicates.iter().position(|predicate| {
        let Some(null_tested_expr) = null_tested_expr(predicate) else {
            return false;
        };

        join.equi_conditions.iter().any(|condition| {
            !condition.is_null_equal
                && match join.join_type {
                    JoinType::Left => condition.right == *null_tested_expr,
                    JoinType::Right => condition.left == *null_tested_expr,
                    _ => unreachable!(),
                }
        })
    }) else {
        return Ok(None);
    };

    // An anti join outputs only its preserved side, while the original
    // outer-join/filter shape still exposes the other side as NULL. Recreate
    // those symbols so upper operators preserve their schema.
    let null_extended_prop = match join.join_type {
        JoinType::Left => join_expr.right_child().derive_relational_prop()?,
        JoinType::Right => join_expr.left_child().derive_relational_prop()?,
        _ => unreachable!(),
    };
    let metadata = metadata.read();
    let null_items = null_extended_prop
        .output_columns
        .iter()
        .map(|index| ScalarItem {
            scalar: ScalarExpr::TypedConstantExpr(
                ConstantExpr {
                    span: None,
                    value: Scalar::Null,
                },
                metadata.column(*index).data_type().wrap_nullable(),
            ),
            index: *index,
        })
        .collect();
    drop(metadata);

    let result = SExpr::create_binary(
        Join {
            join_type: anti_join_type,
            ..join.clone()
        },
        join_expr.left_child_arc(),
        join_expr.right_child_arc(),
    )
    .build_unary(EvalScalar { items: null_items });

    Ok(Some(if filter.predicates.len() > 1 {
        let mut filter = filter.clone();
        filter.predicates.remove(predicate_index);
        result.build_unary(filter)
    } else {
        result
    }))
}

fn null_tested_expr(predicate: &ScalarExpr) -> Option<&ScalarExpr> {
    if let ScalarExpr::FunctionCall(not) = predicate
        && not.func_name == "not"
        && let [ScalarExpr::FunctionCall(is_not_null)] = not.arguments.as_slice()
        && is_not_null.func_name == "is_not_null"
        && let [expr] = is_not_null.arguments.as_slice()
    {
        Some(expr)
    } else {
        None
    }
}
