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

use databend_common_exception::ErrorCode;
use databend_common_exception::Result;

use super::SExpr;
use crate::ColumnSet;
use crate::MetadataRef;
use crate::Symbol;
use crate::plans::AggregateMode;
use crate::plans::Operator;
use crate::plans::RelOperator;
use crate::plans::ScalarExpr;
use crate::plans::SubqueryExpr;
use crate::plans::Visitor as ScalarExprVisitor;

impl SExpr {
    /// Validate that every column an operator references is produced by its inputs.
    ///
    /// For each node, the columns referenced by its scalar expressions must be a subset of
    /// the columns available to it: the output columns of its children, the columns of the
    /// enclosing scope when the node belongs to a correlated subquery, and, for a leaf
    /// operator, its own output columns. A correlated subquery embedded in a scalar
    /// expression is validated recursively with the enclosing node's available columns as
    /// its ambient scope.
    ///
    /// A violation means a binder or optimizer step dropped, renamed, or failed to
    /// propagate a column. Without this check, such a plan only fails at execution time
    /// with an opaque `Unable to get field named "<symbol>"` from the physical plan builder.
    pub fn validate_column_scope(&self, metadata: &MetadataRef) -> Result<()> {
        ColumnScopeValidator { metadata }.validate(self, &ColumnSet::new())
    }
}

struct ColumnScopeValidator<'a> {
    metadata: &'a MetadataRef,
}

impl ColumnScopeValidator<'_> {
    #[recursive::recursive]
    fn validate(&self, s_expr: &SExpr, ambient: &ColumnSet) -> Result<()> {
        let plan = s_expr.plan();

        let mut available = ambient.clone();
        if plan.arity() == 0 && !matches!(plan, RelOperator::ExpressionScan(_)) {
            // A leaf scan references its own columns in its predicates. ExpressionScan,
            // however, computes its outputs from values and cannot read those outputs.
            available.extend(s_expr.derive_relational_prop()?.output_columns.iter());
        }
        for child in s_expr.children() {
            available.extend(child.derive_relational_prop()?.output_columns.iter());
        }
        if let RelOperator::Aggregate(aggregate) = plan {
            // A `Final` aggregate keeps the same scalar items as the `Partial` one below it
            // and consumes its states, so its arguments come from the partial's input. In a
            // distributed plan an `Exchange` sits between the two.
            if aggregate.mode == AggregateMode::Final {
                let mut node = s_expr.child(0)?;
                while let RelOperator::Exchange(_) = node.plan() {
                    node = node.child(0)?;
                }
                if let RelOperator::Aggregate(_) = node.plan() {
                    for input in node.children() {
                        available.extend(input.derive_relational_prop()?.output_columns.iter());
                    }
                }
            }
            // `_grouping_id` is produced by the aggregate itself and read by `grouping()`.
            if let Some(grouping_sets) = &aggregate.grouping_sets {
                available.insert(grouping_sets.grouping_id_index);
            }
        }

        if let RelOperator::WindowGroup(group) = plan {
            // The physical builder evaluates scalar items over the child before evaluating
            // windows. An item cannot read its own output or another item's output.
            self.validate_scalars(
                plan,
                group.scalar_items.iter().map(|item| &item.scalar),
                &available,
            )?;
            let mut window_available = available.clone();
            window_available.extend(group.scalar_items.iter().map(|item| item.index));
            self.validate_scalars(
                plan,
                group
                    .windows
                    .iter()
                    .flat_map(|window| window.scalar_expr_iter()),
                &window_available,
            )?;
        } else {
            self.validate_scalars(plan, plan.scalar_expr_iter(), &available)?;
        }
        // The right side of a LATERAL join is correlated to the left side without a
        // `SubqueryExpr`, so the left outputs become its ambient scope.
        if let RelOperator::Join(join) = plan
            && join.is_lateral
        {
            let mut lateral_ambient = ambient.clone();
            lateral_ambient.extend(
                s_expr
                    .child(0)?
                    .derive_relational_prop()?
                    .output_columns
                    .iter(),
            );
            self.validate(s_expr.child(0)?, ambient)?;
            return self.validate(s_expr.child(1)?, &lateral_ambient);
        }
        for child in s_expr.children() {
            self.validate(child, ambient)?;
        }
        Ok(())
    }

    fn validate_scalars<'a>(
        &self,
        plan: &RelOperator,
        scalars: impl IntoIterator<Item = &'a ScalarExpr>,
        available: &ColumnSet,
    ) -> Result<()> {
        let mut collector = ReferenceCollector::default();
        for scalar in scalars {
            collector.visit(scalar)?;
        }

        // A mark join converted to a semi join registers its marker column as removed; the
        // references left behind are dead and pruned by the physical plan builder.
        let metadata = self.metadata.read();
        let unresolved: Vec<Symbol> = collector
            .referenced
            .iter()
            .filter(|column| {
                !available.contains(column) && !metadata.is_removed_mark_index(**column)
            })
            .copied()
            .collect();
        drop(metadata);
        if !unresolved.is_empty() {
            return Err(ErrorCode::Internal(format!(
                "SExpr column scope violation in {:?}: references {} which no input produces; available columns: {}",
                plan.rel_op(),
                self.describe(&unresolved),
                self.describe(&available.iter().copied().collect::<Vec<_>>()),
            )));
        }
        for subquery in &collector.subqueries {
            self.validate(&subquery.subquery, available)?;
        }
        Ok(())
    }

    fn describe(&self, columns: &[Symbol]) -> String {
        let metadata = self.metadata.read();
        let mut columns = columns.to_vec();
        columns.sort();
        let rendered: Vec<String> = columns
            .iter()
            .map(|column| match metadata.columns().get(column.as_usize()) {
                Some(entry) => format!("{column} ({})", entry.name()),
                None => format!("{column} (unknown symbol)"),
            })
            .collect();
        format!("[{}]", rendered.join(", "))
    }
}

/// Collect the columns a scalar expression reads and the correlated subqueries it embeds.
///
/// Unlike `ScalarExpr::used_columns`, a subquery contributes nothing here: its outer
/// references are checked by validating the subquery plan itself against the enclosing
/// scope, which pinpoints the offending node inside the subquery.
#[derive(Default)]
struct ReferenceCollector<'a> {
    referenced: ColumnSet,
    subqueries: Vec<&'a SubqueryExpr>,
}

impl<'a> ScalarExprVisitor<'a> for ReferenceCollector<'a> {
    fn visit_bound_column_ref(&mut self, col: &'a crate::plans::BoundColumnRef) -> Result<()> {
        self.referenced.insert(col.column.index);
        Ok(())
    }

    fn visit_subquery(&mut self, subquery: &'a SubqueryExpr) -> Result<()> {
        self.subqueries.push(subquery);
        if let Some(child_expr) = subquery.child_expr.as_ref() {
            self.visit(child_expr)?;
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use databend_common_expression::DataSchemaRefExt;
    use databend_common_expression::types::DataType;
    use databend_common_expression::types::NumberDataType;
    use parking_lot::RwLock;

    use super::*;
    use crate::ColumnBindingBuilder;
    use crate::Metadata;
    use crate::Visibility;
    use crate::plans::BoundColumnRef;
    use crate::plans::EvalScalar;
    use crate::plans::ExpressionScan;
    use crate::plans::Filter;
    use crate::plans::Join;
    use crate::plans::JoinEquiCondition;
    use crate::plans::JoinType;
    use crate::plans::ScalarExpr;
    use crate::plans::ScalarItem;
    use crate::plans::Scan;
    use crate::plans::Window;
    use crate::plans::WindowFuncFrame;
    use crate::plans::WindowFuncType;
    use crate::plans::WindowGroup;

    fn metadata_with_columns(n: usize) -> MetadataRef {
        let mut metadata = Metadata::default();
        for i in 0..n {
            metadata.add_derived_column(format!("c{i}"), DataType::Number(NumberDataType::Int32));
        }
        Arc::new(RwLock::new(metadata))
    }

    fn column(index: usize) -> ScalarExpr {
        ScalarExpr::BoundColumnRef(BoundColumnRef {
            span: None,
            column: ColumnBindingBuilder::new(
                format!("c{index}"),
                Symbol::new(index),
                Box::new(DataType::Number(NumberDataType::Int32)),
                Visibility::Visible,
            )
            .build(),
        })
    }

    fn scan(columns: &[usize]) -> SExpr {
        SExpr::create_leaf(Scan {
            columns: columns.iter().copied().map(Symbol::new).collect(),
            ..Default::default()
        })
    }

    #[test]
    fn accepts_references_produced_by_children() -> Result<()> {
        let metadata = metadata_with_columns(3);
        let filter = Filter {
            predicates: vec![column(1)],
        };
        let s_expr = SExpr::create_unary(Arc::new(RelOperator::Filter(filter)), scan(&[0, 1]));
        s_expr.validate_column_scope(&metadata)
    }

    #[test]
    fn rejects_references_not_produced_by_children() {
        let metadata = metadata_with_columns(3);
        let eval = EvalScalar {
            items: vec![ScalarItem {
                scalar: column(2),
                index: Symbol::new(3),
            }],
        };
        let s_expr = SExpr::create_unary(Arc::new(RelOperator::EvalScalar(eval)), scan(&[0, 1]));
        let err = s_expr.validate_column_scope(&metadata).unwrap_err();
        assert_eq!(err.code(), ErrorCode::INTERNAL);
        assert!(
            err.message().contains("EvalScalar") && err.message().contains("2 (c2)"),
            "{err}"
        );
    }

    #[test]
    fn semi_and_anti_join_only_expose_the_retained_side() -> Result<()> {
        let metadata = metadata_with_columns(3);
        for (join_type, retained, dropped) in [
            (JoinType::LeftSemi, 0, 1),
            (JoinType::LeftAnti, 0, 1),
            (JoinType::RightSemi, 1, 0),
            (JoinType::RightAnti, 1, 0),
        ] {
            let join = SExpr::create_binary(
                Arc::new(RelOperator::Join(Join {
                    join_type,
                    equi_conditions: vec![JoinEquiCondition::new(column(0), column(1), false)],
                    // A mark-to-semi rewrite can leave the former marker index on the join.
                    marker_index: Some(Symbol::new(2)),
                    ..Default::default()
                })),
                scan(&[0]),
                scan(&[1]),
            );
            // Join conditions may reference both inputs, but only the retained side reaches
            // an operator above the join.
            join.validate_column_scope(&metadata)?;
            let prop = join.derive_relational_prop()?;
            assert_eq!(
                prop.output_columns,
                [Symbol::new(retained)].into_iter().collect()
            );
            assert!(prop.outer_columns.is_empty());
            let valid = SExpr::create_unary(
                Arc::new(RelOperator::Filter(Filter {
                    predicates: vec![column(retained)],
                })),
                join.clone(),
            );
            valid.validate_column_scope(&metadata)?;
            let invalid = SExpr::create_unary(
                Arc::new(RelOperator::Filter(Filter {
                    predicates: vec![column(dropped)],
                })),
                join,
            );
            let err = invalid.validate_column_scope(&metadata).unwrap_err();
            assert!(
                err.message().contains("Filter")
                    && err.message().contains(&format!("{dropped} (c{dropped})")),
                "{err}"
            );
        }
        Ok(())
    }

    #[test]
    fn window_group_scalar_items_only_read_child_columns() -> Result<()> {
        let metadata = metadata_with_columns(4);
        let group = |item_input| WindowGroup {
            scalar_items: vec![ScalarItem {
                scalar: column(item_input),
                index: Symbol::new(2),
            }],
            windows: vec![Window {
                span: None,
                index: Symbol::new(3),
                function: WindowFuncType::RowNumber,
                arguments: vec![ScalarItem {
                    scalar: column(2),
                    index: Symbol::new(2),
                }],
                partition_by: vec![],
                order_by: vec![],
                frame: WindowFuncFrame::default(),
                limit: None,
                top: None,
            }],
        };
        let valid = SExpr::create_unary(Arc::new(RelOperator::WindowGroup(group(0))), scan(&[0]));
        valid.validate_column_scope(&metadata)?;

        // The output of a scalar item must not be available to that item itself.
        let invalid = SExpr::create_unary(Arc::new(RelOperator::WindowGroup(group(2))), scan(&[0]));
        let err = invalid.validate_column_scope(&metadata).unwrap_err();
        assert!(
            err.message().contains("WindowGroup") && err.message().contains("2 (c2)"),
            "{err}"
        );

        // Sibling scalar items are evaluated over the same child, not in sequence.
        let mut sibling_group = group(0);
        sibling_group.scalar_items.push(ScalarItem {
            scalar: column(2),
            index: Symbol::new(1),
        });
        let sibling = SExpr::create_unary(
            Arc::new(RelOperator::WindowGroup(sibling_group)),
            scan(&[0]),
        );
        let err = sibling.validate_column_scope(&metadata).unwrap_err();
        assert!(
            err.message().contains("WindowGroup") && err.message().contains("2 (c2)"),
            "{err}"
        );
        Ok(())
    }

    #[test]
    fn expression_scan_values_read_from_child_not_own_outputs() -> Result<()> {
        let metadata = metadata_with_columns(3);
        let expression_scan = |source| ExpressionScan {
            expression_scan_index: 0,
            values: vec![vec![column(source)]],
            num_scalar_columns: 1,
            cache_index: 0,
            column_indexes: vec![Symbol::new(1)],
            data_types: vec![DataType::Number(NumberDataType::Int32)],
            schema: DataSchemaRefExt::create(vec![]),
        };
        let valid = SExpr::create_unary(
            Arc::new(RelOperator::ExpressionScan(expression_scan(0))),
            scan(&[0]),
        );
        valid.validate_column_scope(&metadata)?;
        let invalid = SExpr::create_unary(
            Arc::new(RelOperator::ExpressionScan(expression_scan(2))),
            scan(&[0]),
        );
        let err = invalid.validate_column_scope(&metadata).unwrap_err();
        assert!(
            err.message().contains("ExpressionScan") && err.message().contains("2 (c2)"),
            "{err}"
        );
        // A leaf ExpressionScan must not count its own output as a source for values.
        let self_reference = SExpr::create_leaf(expression_scan(1));
        let err = self_reference.validate_column_scope(&metadata).unwrap_err();
        assert!(
            err.message().contains("ExpressionScan") && err.message().contains("1 (c1)"),
            "{err}"
        );
        Ok(())
    }
}
