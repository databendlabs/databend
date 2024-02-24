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

use databend_common_ast::ast::FormatTreeNode;

use crate::optimizer::SExpr;
use crate::planner::format::display_rel_operator::to_format_tree;
use crate::plans::RelOperator;
use crate::ColumnEntry;
use crate::IndexType;
use crate::Metadata;

/// A trait for humanizing IDs.
pub trait IdHumanizer {
    type ColumnId;
    type TableId;

    fn humanize_column_id(&self, id: Self::ColumnId) -> String;

    fn humanize_table_id(&self, id: Self::TableId) -> String;
}

/// A trait for humanizing operators.
pub trait OperatorHumanizer<I: IdHumanizer> {
    type Output;

    fn humanize_operator(&self, id_humanizer: &I, op: &RelOperator) -> Self::Output;
}

/// A default implementation of `IdHumanizer`.
/// It just simply prints the ID with a prefix.
pub struct DefaultIdHumanizer;

impl IdHumanizer for DefaultIdHumanizer {
    type ColumnId = IndexType;
    type TableId = IndexType;

    fn humanize_column_id(&self, id: Self::ColumnId) -> String {
        format!("column_{}", id)
    }

    fn humanize_table_id(&self, id: Self::TableId) -> String {
        format!("table_{}", id)
    }
}

/// A default implementation of `OperatorHumanizer`.
/// It will use `IdHumanizer` to humanize the IDs and then print the operator.
///
/// Although the `Output` is `FormatTreeNode`, the children of the operator are not filled.
///
/// # Example
/// ```
/// use databend_query::planner::format::display_rel_operator::DefaultIdHumanizer;
/// use databend_query::planner::format::display_rel_operator::DefaultOperatorHumanizer;
/// use databend_query::planner::format::display_rel_operator::Filter;
/// use databend_query::planner::format::display_rel_operator::IdHumanizer;
/// use databend_query::planner::format::display_rel_operator::OperatorHumanizer;
/// use databend_query::planner::format::display_rel_operator::RelOperator;
///
/// let id_humanizer = DefaultIdHumanizer;
/// let operator_humanizer = DefaultOperatorHumanizer;
/// let op = RelOperator::Filter(Filter { predicates: vec![] });
/// let tree = operator_humanizer.humanize_operator(&id_humanizer, &op);
///
/// assert_eq!(tree.payload, "Filter");
/// assert_eq!(tree.children.len(), 1);
/// ```
pub struct DefaultOperatorHumanizer;

impl<I: IdHumanizer<ColumnId = IndexType, TableId = IndexType>> OperatorHumanizer<I>
    for DefaultOperatorHumanizer
{
    type Output = FormatTreeNode;

    fn humanize_operator(&self, id_humanizer: &I, op: &RelOperator) -> Self::Output {
        to_format_tree(id_humanizer, op)
    }
}

impl IdHumanizer for Metadata {
    type ColumnId = IndexType;
    type TableId = IndexType;

    fn humanize_column_id(&self, id: Self::ColumnId) -> String {
        let column_entry = self.column(id);
        match column_entry {
            ColumnEntry::BaseTableColumn(column) => {
                let table = self.table(column.table_index);
                let db = table.database();
                let table = table.name();
                let column = column.column_name.as_str();
                format!("{}.{}.{}", db, table, column)
            }
            ColumnEntry::DerivedColumn(column) => {
                let column = column.alias.as_str();
                format!("derived.{}", column)
            }
            ColumnEntry::InternalColumn(column) => {
                let column = column.internal_column.column_name.as_str();
                format!("internal.{}", column)
            }
            ColumnEntry::VirtualColumn(column) => {
                let table = self.table(column.table_index);
                let db = table.database();
                let table = table.name();
                let column = column.column_name.as_str();
                format!("{}.{}.{}", db, table, column)
            }
        }
    }

    fn humanize_table_id(&self, id: Self::TableId) -> String {
        let table = self.table(id);
        let db = table.database();
        let table = table.name();
        format!("{}.{}", db, table)
    }
}

/// A humanizer for `SExpr`.
/// It will use `IdHumanizer` and `OperatorHumanizer` to humanize the `SExpr`.
/// The result is a `FormatTreeNode` with the operator and its children.
pub struct TreeHumanizer;

impl TreeHumanizer {
    pub fn humanize_s_expr<I: IdHumanizer, O: OperatorHumanizer<I, Output = FormatTreeNode>>(
        id_humanizer: &I,
        operator_humanizer: &O,
        s_expr: &SExpr,
    ) -> O::Output {
        let op = s_expr.plan();
        let mut tree = operator_humanizer.humanize_operator(id_humanizer, op);
        let children = s_expr
            .children()
            .map(|v| Self::humanize_s_expr(id_humanizer, operator_humanizer, v))
            .collect::<Vec<_>>();
        tree.children.extend(children);
        tree
    }
}
