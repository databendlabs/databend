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

use crate::ColumnEntry;
use crate::MetadataRef;
use crate::ScalarExpr;
use crate::TableEntry;
use crate::binder::ColumnBindingBuilder;
use crate::optimizer::ir::Matcher;
use crate::optimizer::ir::SExpr;
use crate::optimizer::optimizers::operator::EquivalentConstantsVisitor;
use crate::optimizer::optimizers::rule::Rule;
use crate::optimizer::optimizers::rule::RuleID;
use crate::optimizer::optimizers::rule::TransformResult;
use crate::plans::BoundColumnRef;
use crate::plans::Filter;
use crate::plans::RelOp;
use crate::plans::Scan;
use crate::plans::SecureFilter;
use crate::plans::SubqueryExpr;
use crate::plans::VisitorMut;

pub struct RulePushDownFilterScan {
    id: RuleID,
    matchers: Vec<Matcher>,
    metadata: MetadataRef,
}

impl RulePushDownFilterScan {
    pub fn new(metadata: MetadataRef) -> Self {
        Self {
            id: RuleID::PushDownFilterScan,
            // Input:
            // (1)    Filter
            //          \
            //          Scan
            // (2)    Filter
            //          \
            //          SecureFilter
            //            \
            //            Scan
            //
            // Output:
            // Preserve the original plan structure, but write Filter.predicates into Scan.push_down_predicates.
            matchers: vec![
                Matcher::MatchOp {
                    op_type: RelOp::Filter,
                    children: vec![Matcher::MatchOp {
                        op_type: RelOp::Scan,
                        children: vec![],
                    }],
                },
                Matcher::MatchOp {
                    op_type: RelOp::Filter,
                    children: vec![Matcher::MatchOp {
                        op_type: RelOp::SecureFilter,
                        children: vec![Matcher::MatchOp {
                            op_type: RelOp::Scan,
                            children: vec![],
                        }],
                    }],
                },
            ],
            metadata,
        }
    }

    // Replace columns in a predicate.
    // If replace_view is true, we will use the columns of the source table to replace the columns in
    // the view, this allows us to perform push-down filtering operations at the storage layer.
    // If replace_view is false, we will replace column alias name with original column name.
    fn replace_predicate_column(
        predicate: &ScalarExpr,
        table_entries: &[TableEntry],
        column_entries: &[&ColumnEntry],
        replace_view: bool,
    ) -> Result<ScalarExpr> {
        struct ReplacePredicateColumnVisitor<'a> {
            table_entries: &'a [TableEntry],
            column_entries: &'a [&'a ColumnEntry],
            replace_view: bool,
        }

        impl<'a> VisitorMut<'a> for ReplacePredicateColumnVisitor<'a> {
            fn visit_bound_column_ref(&mut self, column: &mut BoundColumnRef) -> Result<()> {
                if let Some(base_column) =
                    self.column_entries
                        .iter()
                        .find_map(|column_entry| match column_entry {
                            ColumnEntry::BaseTableColumn(base_column)
                                if base_column.column_index == column.column.index =>
                            {
                                Some(base_column)
                            }
                            _ => None,
                        })
                {
                    if let Some(table_entry) = self
                        .table_entries
                        .iter()
                        .find(|table_entry| table_entry.index() == base_column.table_index)
                    {
                        let mut column_binding_builder = ColumnBindingBuilder::new(
                            base_column.column_name.clone(),
                            base_column.column_index,
                            column.column.data_type.clone(),
                            column.column.visibility.clone(),
                        )
                        .table_name(Some(table_entry.name().to_string()))
                        .database_name(Some(table_entry.database().to_string()))
                        .table_index(Some(table_entry.index()));

                        if self.replace_view {
                            column_binding_builder = column_binding_builder
                                .virtual_expr(column.column.virtual_expr.clone());
                        }

                        column.column = column_binding_builder.build();
                    }
                }
                Ok(())
            }

            fn visit_subquery_expr(&mut self, _subquery: &'a mut SubqueryExpr) -> Result<()> {
                Ok(())
            }
        }

        let mut visitor = ReplacePredicateColumnVisitor {
            table_entries,
            column_entries,
            replace_view,
        };
        let mut predicate = predicate.clone();
        visitor.visit(&mut predicate)?;

        Ok(predicate.clone())
    }

    fn find_push_down_predicates(
        &self,
        predicates: &[ScalarExpr],
        scan: &Scan,
    ) -> Result<Vec<ScalarExpr>> {
        let metadata = self.metadata.read();
        let column_entries = scan
            .columns
            .iter()
            .map(|index| metadata.column(*index))
            .collect::<Vec<_>>();
        let table_entries = metadata.tables();
        let is_source_of_view = table_entries.iter().any(|t| t.is_source_of_view());

        let mut filtered_predicates = vec![];
        let mut visitor = EquivalentConstantsVisitor::default();
        for predicate in predicates {
            let used_columns = predicate.used_columns();
            let mut contain_derived_column = false;
            for column_entry in column_entries.iter() {
                if let ColumnEntry::DerivedColumn(column) = column_entry {
                    // Don't push down predicate that contains derived column
                    // Because storage can't know such columns.
                    if used_columns.contains(&column.column_index) {
                        contain_derived_column = true;
                        break;
                    }
                }
            }
            if !contain_derived_column {
                let mut predicate = Self::replace_predicate_column(
                    predicate,
                    table_entries,
                    &column_entries,
                    is_source_of_view,
                )?;
                visitor.visit(&mut predicate)?;
                filtered_predicates.push(predicate);
            }
        }

        Ok(filtered_predicates)
    }
}

impl Rule for RulePushDownFilterScan {
    fn id(&self) -> RuleID {
        self.id
    }

    fn apply(&self, s_expr: &SExpr, state: &mut TransformResult) -> Result<()> {
        let i = self
            .matchers
            .iter()
            .position(|matcher| matcher.matches(s_expr))
            .unwrap();
        self.apply_matcher(i, s_expr, state)
    }

    fn apply_matcher(&self, idx: usize, s_expr: &SExpr, state: &mut TransformResult) -> Result<()> {
        let filter: Filter = s_expr.plan().clone().try_into()?;
        let child = s_expr.child(0)?;

        let (mut scan, secure_filter): (Scan, Option<SecureFilter>) = match idx {
            // Filter -> Scan
            0 => (child.plan().clone().try_into()?, None),
            // Filter -> SecureFilter -> Scan
            1 => {
                let secure_filter: SecureFilter = child.plan().clone().try_into()?;
                let scan: Scan = child.child(0)?.plan().clone().try_into()?;
                (scan, Some(secure_filter))
            }
            _ => unreachable!(),
        };

        let add_filters = self.find_push_down_predicates(&filter.predicates, &scan)?;
        match scan.push_down_predicates.as_mut() {
            Some(existing_filters) => {
                // Add `add_filters` to existing_filters if it's not already there.
                // Keep the order of `existing_filters` to ensure the tests are stable.
                for predicate in add_filters {
                    if !existing_filters.contains(&predicate) {
                        existing_filters.push(predicate);
                    }
                }
            }
            None => scan.push_down_predicates = Some(add_filters),
        }

        let scan_expr = SExpr::create_leaf(Arc::new(scan.into()));
        let child_expr = if let Some(secure_filter) = secure_filter {
            SExpr::create_unary(Arc::new(secure_filter.into()), Arc::new(scan_expr))
        } else {
            scan_expr
        };

        let mut result = SExpr::create_unary(Arc::new(filter.into()), Arc::new(child_expr));
        result.set_applied_rule(&self.id);
        state.add_result(result);
        Ok(())
    }

    fn matchers(&self) -> &[Matcher] {
        &self.matchers
    }
}
