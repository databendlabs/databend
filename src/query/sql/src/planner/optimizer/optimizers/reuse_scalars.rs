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

use databend_common_exception::Result;

use crate::ColumnSet;
use crate::ScalarExpr;
use crate::optimizer::Optimizer;
use crate::optimizer::ir::SExpr;
use crate::plans::BoundColumnRef;
use crate::plans::JoinType;
use crate::plans::RelOperator;
use crate::plans::ScalarItem;
use crate::plans::VisitorMut;

/// Expressions whose result columns contain the same value on every surviving
/// row in this scope. This is value reuse, not expression hoisting: a definition
/// must already have executed before any consumer can reference it.
#[derive(Default)]
struct AvailableScalars {
    expressions: HashMap<ScalarExpr, BoundColumnRef>,
}

impl AvailableScalars {
    fn rewrite(&self, scalar: &mut ScalarExpr) -> Result<()> {
        struct Rewriter<'a>(&'a AvailableScalars);
        impl VisitorMut<'_> for Rewriter<'_> {
            fn visit(&mut self, scalar: &mut ScalarExpr) -> Result<()> {
                if let Some(column) = self.0.expressions.get(scalar)
                    && column.column.data_type.as_ref() == scalar.data_type().as_ref()
                {
                    *scalar = column.clone().into();
                    return Ok(());
                }
                // Subqueries, lambdas, aggregates and window functions have
                // their own binding/evaluation scopes. Do not descend into them.
                match scalar {
                    ScalarExpr::FunctionCall(func) => {
                        for argument in &mut func.arguments {
                            self.visit(argument)?;
                        }
                    }
                    ScalarExpr::CastExpr(cast) => self.visit(&mut cast.argument)?,
                    _ => {}
                }
                Ok(())
            }
        }
        Rewriter(self).visit(scalar)
    }

    fn invalidate(&mut self, definitions: &ColumnSet) {
        self.expressions.retain(|expression, column| {
            !definitions.contains(&column.column.index)
                && expression.used_columns().is_disjoint(definitions)
        });
    }

    fn record(
        &mut self,
        expression: &ScalarExpr,
        item: &ScalarItem,
        definitions: &ColumnSet,
    ) -> Result<()> {
        if matches!(
            expression,
            ScalarExpr::FunctionCall(_) | ScalarExpr::CastExpr(_)
        ) && expression.is_deterministic()
            && expression.used_columns().is_disjoint(definitions)
        {
            self.expressions.insert(expression.clone(), BoundColumnRef {
                span: expression.span(),
                column: item.column_binding("scalar_result".to_string())?,
            });
        }
        Ok(())
    }

    fn retain_outputs(&mut self, output: &ColumnSet) {
        self.expressions
            .retain(|_, column| output.contains(&column.column.index));
    }
}

fn is_identity(item: &ScalarItem) -> bool {
    matches!(&item.scalar, ScalarExpr::BoundColumnRef(column) if column.column.index == item.index)
}

pub struct ReuseScalarsOptimizer;

impl ReuseScalarsOptimizer {
    #[recursive::recursive]
    fn rewrite(mut expr: SExpr) -> Result<(SExpr, AvailableScalars)> {
        let mut children = Vec::with_capacity(expr.children.len());
        let mut inputs = Vec::with_capacity(expr.children.len());
        for child in std::mem::take(&mut expr.children) {
            let (child, available) = Self::rewrite(Arc::unwrap_or_clone(child))?;
            children.push(Arc::new(child));
            inputs.push(available);
        }
        let expr = expr.replace_children(children);
        let mut plan = expr.plan().clone();
        let mut available = match &mut plan {
            RelOperator::EvalScalar(eval) => {
                let mut available = inputs.remove(0);
                // A passthrough projection preserves the source value and must
                // not invalidate expressions already evaluated from that source.
                let definitions = eval
                    .items
                    .iter()
                    .filter(|item| !is_identity(item))
                    .map(|item| item.index)
                    .collect();
                // All sibling definitions read the same child scope. Do not
                // expose one sibling's result while rewriting another sibling.
                let originals = eval
                    .items
                    .iter()
                    .map(|item| item.scalar.clone())
                    .collect::<Vec<_>>();
                for item in &mut eval.items {
                    available.rewrite(&mut item.scalar)?;
                }
                available.invalidate(&definitions);
                for (original, item) in originals.iter().zip(&eval.items) {
                    available.record(original, item, &definitions)?;
                    available.record(&item.scalar, item, &definitions)?;
                }
                available
            }
            RelOperator::Filter(filter) => {
                let available = inputs.remove(0);
                for predicate in &mut filter.predicates {
                    available.rewrite(predicate)?;
                }
                available
            }
            RelOperator::Join(join) => {
                let mut left = inputs.remove(0);
                let right = inputs.remove(0);
                for condition in &mut join.equi_conditions {
                    left.rewrite(&mut condition.left)?;
                    right.rewrite(&mut condition.right)?;
                }
                // ON predicates consume input values, before this join creates
                // NULL-extended rows. Each side's symbols have distinct IDs.
                for predicate in &mut join.non_equi_conditions {
                    left.rewrite(predicate)?;
                    right.rewrite(predicate)?;
                }
                match join.join_type {
                    JoinType::Inner | JoinType::InnerAny | JoinType::Cross => {
                        left.expressions.extend(right.expressions);
                        left
                    }
                    JoinType::Left
                    | JoinType::LeftAny
                    | JoinType::LeftSingle
                    | JoinType::LeftSemi
                    | JoinType::LeftAnti => left,
                    JoinType::Right
                    | JoinType::RightAny
                    | JoinType::RightSingle
                    | JoinType::RightSemi
                    | JoinType::RightAnti => right,
                    // Values from a null-supplying side cannot represent an
                    // expression recomputed after NULL extension (e.g. coalesce).
                    _ => AvailableScalars::default(),
                }
            }
            RelOperator::Window(window) => {
                let mut available = inputs.remove(0);
                available.invalidate(&ColumnSet::from([window.index]));
                available
            }
            RelOperator::WindowGroup(group) => {
                let mut available = inputs.remove(0);
                let definitions = group
                    .scalar_items
                    .iter()
                    .filter(|item| !is_identity(item))
                    .map(|item| item.index)
                    .chain(group.windows.iter().map(|window| window.index))
                    .collect();
                available.invalidate(&definitions);
                // Groups containing volatile inputs can still evaluate their
                // scalars internally. Only their deterministic results are reusable.
                for item in &group.scalar_items {
                    available.record(&item.scalar, item, &definitions)?;
                }
                available
            }
            RelOperator::ProjectSet(project) => {
                let mut available = inputs.remove(0);
                available.invalidate(&project.srfs.iter().map(|item| item.index).collect());
                available
            }
            RelOperator::Sort(_) | RelOperator::Limit(_) | RelOperator::Exchange(_) => {
                inputs.remove(0)
            }
            // Aggregation/grouping sets, unions and CTE references change the
            // value scope. No definitions flow across these boundaries implicitly.
            _ => AvailableScalars::default(),
        };
        let expr = expr.replace_plan(Arc::new(plan));
        available.retain_outputs(&expr.derive_relational_prop()?.output_columns);
        Ok((expr, available))
    }
}

#[async_trait::async_trait]
impl Optimizer for ReuseScalarsOptimizer {
    fn name(&self) -> String {
        "ReuseScalarsOptimizer".to_string()
    }

    async fn optimize(&mut self, s_expr: SExpr) -> Result<SExpr> {
        Self::rewrite(s_expr).map(|(expr, _)| expr)
    }
}

#[cfg(test)]
mod tests {
    use databend_common_expression::types::DataType;
    use databend_common_expression::types::NumberDataType;

    use super::*;
    use crate::ColumnBindingBuilder;
    use crate::Symbol;
    use crate::Visibility;
    use crate::plans::FunctionCall;

    fn abs_column(index: usize) -> ScalarExpr {
        let data_type = DataType::Number(NumberDataType::Int64);
        let column = BoundColumnRef {
            span: None,
            column: ColumnBindingBuilder::new(
                "input".to_string(),
                Symbol::new(index),
                Box::new(data_type.clone()),
                Visibility::Visible,
            )
            .build(),
        };
        FunctionCall {
            span: None,
            func_name: "abs".to_string(),
            params: vec![],
            arguments: vec![column.into()],
            return_type: Box::new(data_type),
        }
        .into()
    }

    #[test]
    fn overwritten_sources_and_results_invalidate_available_values() -> Result<()> {
        let item = ScalarItem {
            scalar: abs_column(0),
            index: Symbol::new(1),
        };
        for overwritten in [0, 1] {
            let mut available = AvailableScalars::default();
            available.record(&item.scalar, &item, &ColumnSet::from([item.index]))?;
            let mut consumer = item.scalar.clone();
            available.rewrite(&mut consumer)?;
            assert!(
                matches!(consumer, ScalarExpr::BoundColumnRef(ref column) if column.column.index == item.index)
            );

            available.invalidate(&ColumnSet::from([Symbol::new(overwritten)]));
            let mut consumer = item.scalar.clone();
            available.rewrite(&mut consumer)?;
            assert_eq!(consumer, item.scalar);
        }
        Ok(())
    }

    #[test]
    fn sibling_shadowing_does_not_export_an_old_source_definition() -> Result<()> {
        let item = ScalarItem {
            scalar: abs_column(0),
            index: Symbol::new(1),
        };
        let mut available = AvailableScalars::default();
        available.record(
            &item.scalar,
            &item,
            &ColumnSet::from([Symbol::new(0), Symbol::new(1)]),
        )?;
        let mut consumer = item.scalar.clone();
        available.rewrite(&mut consumer)?;
        assert_eq!(consumer, item.scalar);
        Ok(())
    }
}
