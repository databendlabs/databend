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

use crate::ColumnBindingBuilder;
use crate::ColumnSet;
use crate::ScalarExpr;
use crate::Symbol;
use crate::Visibility;
use crate::optimizer::ir::SExpr;
use crate::plans::BoundColumnRef;
use crate::plans::RelOperator;
use crate::plans::VisitorMut;
use crate::plans::Window;
use crate::plans::walk_expr_mut;

/// Decides which filter predicates can move below a Window or WindowGroup. A
/// predicate can move when it has a single value within each partition of
/// every window, so the moved filter discards whole partitions only.
///
/// A partition expression counts as a partition key, a column that it reads
/// does not: with `PARTITION BY k % 2`, `k % 2 = 1` can move, but `k = 3`
/// cannot, because rows with different `k` share a partition.
pub(super) struct WindowFilterPushDown {
    // Per window: the partition expressions with their columns, and the
    // partition key columns.
    partitions: Vec<(Vec<(ScalarExpr, Symbol)>, ColumnSet)>,
    child_columns: ColumnSet,
    // The existing push-down restriction on order and argument columns.
    rejected: ColumnSet,
}

impl WindowFilterPushDown {
    pub(super) fn try_new(expr: &SExpr) -> Result<Option<Self>> {
        let Some(windows) = partition_windows(expr.plan()) else {
            return Ok(None);
        };
        let mut rejected = ColumnSet::new();
        let mut partitions = Vec::with_capacity(windows.len());
        for window in windows {
            rejected.extend(window.order_by_columns()?);
            rejected.extend(window.function.used_columns());
            let exprs = window
                .partition_by
                .iter()
                .map(|item| (item.scalar.clone(), item.index))
                .collect();
            partitions.push((exprs, window.partition_key_columns()));
        }
        let child_columns = expr
            .unary_child()
            .derive_relational_prop()?
            .output_columns
            .clone();
        Ok(Some(Self {
            partitions,
            child_columns,
            rejected,
        }))
    }

    pub(super) fn can_push_down(&self, predicate: &ScalarExpr) -> Result<bool> {
        // The original predicate is evaluated below the window, so it can only
        // read child columns, not scalars evaluated by a WindowGroup.
        let used = predicate.used_columns();
        if !used.is_subset(&self.child_columns) || !used.is_disjoint(&self.rejected) {
            return Ok(false);
        }
        for (exprs, columns) in &self.partitions {
            let mut rewritten = predicate.clone();
            ReplacePartitionExprs { exprs }.visit(&mut rewritten)?;
            if !rewritten.used_columns().is_subset(columns) {
                return Ok(false);
            }
        }
        Ok(true)
    }
}

/// Windows whose partitions a filter below the operator could discard. A
/// WindowGroup scalar that reads other rows would see fewer rows.
fn partition_windows(plan: &RelOperator) -> Option<&[Window]> {
    let windows = match plan {
        RelOperator::Window(window) => std::slice::from_ref(window),
        RelOperator::WindowGroup(group) if !group.depends_on_other_rows() => {
            group.windows.as_slice()
        }
        _ => return None,
    };
    (!windows.is_empty()).then_some(windows)
}

/// Replace each occurrence of a partition expression by its partition column.
/// An expression that differs only in column binding metadata does not match,
/// which keeps the predicate above the window.
struct ReplacePartitionExprs<'a> {
    exprs: &'a [(ScalarExpr, Symbol)],
}

impl<'a> VisitorMut<'a> for ReplacePartitionExprs<'_> {
    fn visit(&mut self, expr: &'a mut ScalarExpr) -> Result<()> {
        let Some((_, index)) = self.exprs.iter().find(|(partition, _)| *partition == *expr) else {
            return walk_expr_mut(self, expr);
        };
        let data_type = Box::new(expr.data_type().into_owned());
        let column =
            ColumnBindingBuilder::new(String::new(), *index, data_type, Visibility::Visible)
                .build();
        *expr = ScalarExpr::BoundColumnRef(BoundColumnRef { span: None, column });
        Ok(())
    }
}
