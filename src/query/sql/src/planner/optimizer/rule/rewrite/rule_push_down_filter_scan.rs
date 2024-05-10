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

use crate::binder::ColumnBindingBuilder;
use crate::optimizer::extract::Matcher;
use crate::optimizer::rule::Rule;
use crate::optimizer::rule::TransformResult;
use crate::optimizer::RuleID;
use crate::optimizer::SExpr;
use crate::plans::BoundColumnRef;
use crate::plans::Filter;
use crate::plans::RelOp;
use crate::plans::Scan;
use crate::plans::SubqueryExpr;
use crate::plans::VisitorMut;
use crate::ColumnEntry;
use crate::MetadataRef;
use crate::ScalarExpr;
use crate::TableEntry;

pub struct RulePushDownFilterScan {
    id: RuleID,
    matchers: Vec<Matcher>,
    metadata: MetadataRef,
}

impl RulePushDownFilterScan {
    pub fn new(metadata: MetadataRef) -> Self {
        Self {
            id: RuleID::PushDownFilterScan,
            // Filter
            //  \
            //   Scan
            matchers: vec![Matcher::MatchOp {
                op_type: RelOp::Filter,
                children: vec![Matcher::MatchOp {
                    op_type: RelOp::Scan,
                    children: vec![],
                }],
            }],
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
                                .virtual_computed_expr(column.column.virtual_computed_expr.clone());
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
                let predicate = Self::replace_predicate_column(
                    predicate,
                    table_entries,
                    &column_entries,
                    is_source_of_view,
                )?;
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
        let filter: Filter = s_expr.plan().clone().try_into()?;
        let mut scan: Scan = s_expr.child(0)?.plan().clone().try_into()?;

        let add_filters = self.find_push_down_predicates(&filter.predicates, &scan)?;

        match scan.push_down_predicates.as_mut() {
            Some(vs) => vs.extend(add_filters),
            None => scan.push_down_predicates = Some(add_filters),
        }

        let mut result = SExpr::create_unary(
            Arc::new(filter.into()),
            Arc::new(SExpr::create_leaf(Arc::new(scan.into()))),
        );
        result.set_applied_rule(&self.id);
        state.add_result(result);
        Ok(())
    }

    fn matchers(&self) -> &[Matcher] {
        &self.matchers
    }
}
