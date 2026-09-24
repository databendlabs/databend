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

use std::sync::Arc;

use databend_common_exception::Result;

use crate::MetadataRef;
use crate::ScalarExpr;
use crate::optimizer::Optimizer;
use crate::optimizer::ir::SExpr;
use crate::plans::BoundColumnRef;
use crate::plans::EvalScalar;
use crate::plans::Exchange;
use crate::plans::JoinType;
use crate::plans::RelOperator;
use crate::plans::ScalarItem;
use crate::plans::VisitorMut;
use crate::plans::Window;
use crate::plans::walk_expr_mut;

/// Materialize mandatory, row-local operator keys at their input boundary.
/// Callers must not use this to hoist residual predicates or output expressions:
/// those can have a different evaluation domain (including outer-join NULL rows).
pub struct KeyMaterializer {
    metadata: MetadataRef,
    items: Vec<ScalarItem>,
}

impl KeyMaterializer {
    pub fn new(metadata: MetadataRef) -> Self {
        Self {
            metadata,
            items: Vec::new(),
        }
    }

    pub fn materialize(&mut self, scalar: &mut ScalarExpr, input: &SExpr) -> Result<()> {
        if !matches!(
            scalar,
            ScalarExpr::FunctionCall(_) | ScalarExpr::CastExpr(_)
        ) || !scalar.is_deterministic()
            || !scalar
                .used_columns()
                .is_subset(&input.derive_relational_prop()?.output_columns)
        {
            return Ok(());
        }

        // Only reuse definitions in the immediate input scope. In particular,
        // an expression below an outer join is not equivalent above that join.
        let source_columns = scalar.used_columns();
        let existing = match input.plan() {
            RelOperator::EvalScalar(eval)
                if !eval
                    .items
                    .iter()
                    .any(|item| source_columns.contains(&item.index)) =>
            {
                eval.items.iter().find(|item| item.scalar == *scalar)
            }
            _ => None,
        };
        if let Some(item) =
            existing.or_else(|| self.items.iter().find(|item| item.scalar == *scalar))
        {
            *scalar = key_column(item)?;
            return Ok(());
        }

        let index = self
            .metadata
            .write()
            .add_derived_column("operator_key".to_string(), scalar.data_type().into_owned());
        let item = ScalarItem {
            index,
            scalar: scalar.clone(),
        };
        *scalar = key_column(&item)?;
        self.items.push(item);
        Ok(())
    }

    #[recursive::recursive]
    pub fn finish(self, input: SExpr) -> SExpr {
        if self.items.is_empty() {
            input
        } else if matches!(
            input.plan(),
            RelOperator::Exchange(Exchange::Merge | Exchange::Broadcast)
        ) {
            // These exchanges preserve rows and values. Evaluate before transfer,
            // while keeping filters and other row-changing operators in place.
            let child = self.finish(input.child(0).unwrap().clone());
            input.replace_children([Arc::new(child)])
        } else {
            SExpr::create_unary(
                Arc::new(EvalScalar { items: self.items }.into()),
                Arc::new(input),
            )
        }
    }
}

fn key_column(item: &ScalarItem) -> Result<ScalarExpr> {
    // Preserve an existing reference, including its binding metadata. Distribution
    // properties compare column and table IDs, so losing the table binding can
    // make an already partitioned input appear to need another exchange.
    if let ScalarExpr::BoundColumnRef(column) = &item.scalar
        && column.column.index == item.index
    {
        return Ok(item.scalar.clone());
    }
    // Unlike bound_column_expr, preserve the defined index even for aliases.
    Ok(BoundColumnRef {
        span: item.scalar.span(),
        column: item.column_binding("operator_key".to_string())?,
    }
    .into())
}

fn reference_window_inputs(window: &mut Window) -> Result<()> {
    for item in window
        .arguments
        .iter_mut()
        .chain(&mut window.partition_by)
        .chain(
            window
                .order_by
                .iter_mut()
                .map(|order| &mut order.order_by_item),
        )
    {
        item.scalar = key_column(item)?;
    }
    Ok(())
}

pub struct MaterializeKeysOptimizer {
    metadata: MetadataRef,
}

impl MaterializeKeysOptimizer {
    pub fn new(metadata: MetadataRef) -> Self {
        Self { metadata }
    }

    #[recursive::recursive]
    fn rewrite(&self, mut expr: SExpr) -> Result<SExpr> {
        let mut children = Vec::with_capacity(expr.children.len());
        for child in std::mem::take(&mut expr.children) {
            children.push(Arc::new(self.rewrite(Arc::unwrap_or_clone(child))?));
        }
        let mut expr = expr.replace_children(children);
        let mut plan = expr.plan().clone();
        let mut children = std::mem::take(&mut expr.children);
        match &mut plan {
            RelOperator::Join(join) => {
                let mut left = KeyMaterializer::new(self.metadata.clone());
                let mut right = KeyMaterializer::new(self.metadata.clone());
                for condition in &mut join.equi_conditions {
                    // Preserve the equality-preserving coercions used by execution
                    // before hiding a key's expression behind a column reference.
                    let (l, r) = condition.canonical_keys();
                    let (mut l, mut r) = (l.clone(), r.clone());
                    left.materialize(&mut l, &children[0])?;
                    right.materialize(&mut r, &children[1])?;
                    condition.left = l;
                    condition.right = r;
                }
                let right_child = children.pop().unwrap();
                let left_child = children.pop().unwrap();
                children.push(Arc::new(left.finish(Arc::unwrap_or_clone(left_child))));
                children.push(Arc::new(right.finish(Arc::unwrap_or_clone(right_child))));
            }
            RelOperator::Window(window) => reference_window_inputs(window)?,
            RelOperator::Sort(sort) => {
                if let Some(partition) = &mut sort.window_partition {
                    for item in &mut partition.partition_by {
                        item.scalar = key_column(item)?;
                    }
                }
            }
            RelOperator::WindowGroup(group)
                if group
                    .scalar_items
                    .iter()
                    .all(|item| item.scalar.is_deterministic()) =>
            {
                // Expose the existing evaluation before distribution is planned,
                // so the exchange hashes and transports the result columns.
                if !group.scalar_items.is_empty() {
                    children[0] = Arc::new(SExpr::create_unary(
                        Arc::new(
                            EvalScalar {
                                items: std::mem::take(&mut group.scalar_items),
                            }
                            .into(),
                        ),
                        children[0].clone(),
                    ));
                }
                for window in &mut group.windows {
                    reference_window_inputs(window)?;
                }
            }
            _ => {}
        }
        Ok(expr.replace_plan(Arc::new(plan)).replace_children(children))
    }
}

#[async_trait::async_trait]
impl Optimizer for MaterializeKeysOptimizer {
    fn name(&self) -> String {
        "MaterializeKeysOptimizer".to_string()
    }

    async fn optimize(&mut self, s_expr: SExpr) -> Result<SExpr> {
        self.rewrite(s_expr)
    }
}

/// Recover input definitions for scan-level runtime filters only. Execution and
/// column liveness must continue to use the materialized key, not this lineage.
pub fn expand_input_keys(mut scalar: ScalarExpr, mut input: &SExpr) -> Result<Option<ScalarExpr>> {
    struct Expand<'a> {
        items: &'a [ScalarItem],
        deterministic: bool,
    }
    impl VisitorMut<'_> for Expand<'_> {
        fn visit(&mut self, scalar: &mut ScalarExpr) -> Result<()> {
            if let ScalarExpr::BoundColumnRef(column) = scalar {
                if let Some(item) = self
                    .items
                    .iter()
                    .find(|item| item.index == column.column.index)
                {
                    if item.scalar.is_deterministic() {
                        *scalar = item.scalar.clone();
                    } else {
                        self.deterministic = false;
                    }
                    // Definitions in one EvalScalar all refer to its child.
                    return Ok(());
                }
            }
            walk_expr_mut(self, scalar)
        }
    }
    loop {
        match input.plan() {
            RelOperator::EvalScalar(eval) => {
                let mut expand = Expand {
                    items: &eval.items,
                    deterministic: true,
                };
                expand.visit(&mut scalar)?;
                if !expand.deterministic {
                    return Ok(None);
                }
            }
            RelOperator::Exchange(_) => {}
            RelOperator::Join(join) => {
                // Reuse can reference a key produced below an earlier join.
                // Trace only an input whose values survive this join unchanged;
                // expressions on a NULL-extended side are not interchangeable.
                let preserved: &[usize] = match join.join_type {
                    JoinType::Inner | JoinType::InnerAny | JoinType::Cross => &[0, 1],
                    JoinType::Left
                    | JoinType::LeftAny
                    | JoinType::LeftSingle
                    | JoinType::LeftSemi
                    | JoinType::LeftAnti => &[0],
                    JoinType::Right
                    | JoinType::RightAny
                    | JoinType::RightSingle
                    | JoinType::RightSemi
                    | JoinType::RightAnti => &[1],
                    _ => &[],
                };
                let used = scalar.used_columns();
                let mut source = None;
                if !used.is_empty() {
                    for &side in preserved {
                        let child = input.child(side)?;
                        if used.is_subset(&child.derive_relational_prop()?.output_columns) {
                            source = Some(child);
                            break;
                        }
                    }
                }
                if let Some(child) = source {
                    input = child;
                    continue;
                }
                break;
            }
            _ => break,
        }
        input = input.child(0)?;
    }
    Ok(Some(scalar))
}
