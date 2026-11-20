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

use crate::Symbol;
use crate::planner::binder::is_grouping_id_item;
use crate::plans::Aggregate;
use crate::plans::BoundColumnRef;
use crate::plans::EvalScalar;
use crate::plans::ScalarItem;

/// Bit `i` identifies an absent grouping key in group-item order, not an input
/// column position. Set only absent bits so all 32 bits of `_grouping_id` fit
/// without constructing an overflowing `(1 << 32) - 1` mask.
pub(super) fn grouping_id(
    group_items: &[ScalarItem],
    grouping_id_index: Symbol,
    set: &[Symbol],
) -> u32 {
    group_items
        .iter()
        .filter(|item| !is_grouping_id_item(item, grouping_id_index))
        .enumerate()
        .filter(|(_, item)| !set.contains(&item.index))
        .fold(0, |id, (i, _)| id | (1 << i))
}

pub(super) fn ensure_group_items_are_projected(
    eval_scalar: &mut EvalScalar,
    agg: &Aggregate,
    grouping_id_index: Symbol,
) -> Result<()> {
    for group_item in agg
        .group_items
        .iter()
        .filter(|item| !is_grouping_id_item(item, grouping_id_index))
    {
        if eval_scalar
            .items
            .iter()
            .any(|item| item.index == group_item.index)
        {
            continue;
        }

        eval_scalar.items.push(ScalarItem {
            scalar: BoundColumnRef {
                span: None,
                column: group_item.column_binding(format!("group_item_{}", group_item.index))?,
            }
            .into(),
            index: group_item.index,
        });
    }
    Ok(())
}

pub(super) fn union_output_indexes(
    eval_scalar: &EvalScalar,
    agg: &Aggregate,
    grouping_id_index: Symbol,
) -> Vec<Symbol> {
    let mut output_indexes: Vec<_> = eval_scalar.items.iter().map(|item| item.index).collect();

    for item in agg.aggregate_functions.iter().chain(agg.group_items.iter()) {
        if !output_indexes.contains(&item.index) {
            output_indexes.push(item.index);
        }
    }

    if !output_indexes.contains(&grouping_id_index) {
        output_indexes.push(grouping_id_index);
    }
    output_indexes
}
