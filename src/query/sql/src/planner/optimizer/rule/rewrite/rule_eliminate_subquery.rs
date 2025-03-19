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
use databend_common_expression::types::DataType;

use crate::optimizer::extract::Matcher;
use crate::optimizer::rule::Rule;
use crate::optimizer::rule::TransformResult;
use crate::optimizer::RuleID;
use crate::optimizer::SExpr;
use crate::plans::walk_expr_mut;
use crate::plans::BoundColumnRef;
use crate::plans::Exchange;
use crate::plans::JoinType;
use crate::plans::RelOperator;
use crate::plans::VisitorMut;
use crate::ColumnBinding;
use crate::ColumnBindingBuilder;
use crate::ColumnEntry;
use crate::IndexType;
use crate::MetadataRef;
use crate::ScalarExpr;
use crate::Visibility;

pub struct RuleEliminateSubquery {
    id: RuleID,
    matchers: Vec<Matcher>,
    pub(crate) metadata: MetadataRef,
}

impl RuleEliminateSubquery {
    pub fn new(metadata: MetadataRef) -> Self {
        Self {
            id: RuleID::EliminateSubquery,
            // Join (only for subquery)
            // |  \
            // *   *
            matchers: vec![Matcher::MatchFn {
                predicate: Box::new(|op| {
                    if let RelOperator::Join(join) = op {
                        return join.join_type == JoinType::LeftSemi
                            && join.equi_conditions.len() == 1;
                    }
                    false
                }),
                children: vec![Matcher::Leaf, Matcher::Leaf],
            }],
            metadata,
        }
    }
}

impl Rule for RuleEliminateSubquery {
    fn id(&self) -> RuleID {
        self.id
    }

    fn apply(&self, s_expr: &SExpr, state: &mut TransformResult) -> Result<()> {
        let RelOperator::Join(join) = s_expr.plan() else {
            return Ok(());
        };
        let mut as_source_visitor = ExprAsSourceVisitor::default();
        let mut left_expr = join.equi_conditions[0].left.clone();
        let mut right_expr = join.equi_conditions[0].right.clone();

        let left_used_tables = left_expr.used_tables()?;
        let right_used_tables = right_expr.used_tables()?;

        // restore possible aliases or duplicate loaded tables by `source_table_id` to determine whether they are the same columns of the same table
        as_source_visitor.visit(&mut left_expr)?;
        as_source_visitor.visit(&mut right_expr)?;

        if !as_source_visitor.success()
            || left_expr != right_expr
            || left_used_tables.len() != 1
            || right_used_tables.len() != 1
        {
            return Ok(());
        }
        let new_column_bindings = {
            let guard = self.metadata.read();

            let left_table_index = left_used_tables[0];
            let right_table_index = right_used_tables[0];
            let left_columns = guard.columns_by_table_index(left_table_index);
            let right_columns = guard.columns_by_table_index(right_table_index);
            let left_table = guard.table(left_table_index);
            // filter table function
            if left_table.database() == "system"
                || guard.table(right_table_index).database() == "system"
            {
                return Ok(());
            }
            let left_source_table_index = guard
                .get_source_table_index(Some(left_table.database()), left_table.table().name());

            if left_columns.len() != right_columns.len() {
                return Ok(());
            }
            let mut new_column_bindings = HashMap::with_capacity(left_columns.len());
            for (left_entry, right_entry) in left_columns.into_iter().zip(right_columns.into_iter())
            {
                let (
                    ColumnEntry::BaseTableColumn(left_column),
                    ColumnEntry::BaseTableColumn(right_column),
                ) = (left_entry, right_entry)
                else {
                    return Ok(());
                };
                new_column_bindings.insert(
                    right_column.column_index,
                    ColumnBindingBuilder::new(
                        left_column.column_name,
                        left_column.column_index,
                        Box::new(DataType::from(&left_column.data_type)),
                        Visibility::Visible,
                    )
                    .table_name(Some(left_table.table().name().to_string()))
                    .database_name(Some(left_table.database().to_string()))
                    .table_index(Some(left_table_index))
                    .source_table_index(left_source_table_index)
                    .column_position(left_column.column_position)
                    .virtual_expr(left_column.virtual_expr)
                    .build(),
                );
            }
            new_column_bindings
        };
        // restore ColumnBinding of same table in Subquery to table in Left
        let mut body_visitor =
            SubqueryBodyVisitor::new(new_column_bindings, SExpr::clone(&s_expr.children[0]));
        body_visitor.visit(&s_expr.children[1])?;

        if let Some(new_expr) = body_visitor.result() {
            state.add_result(new_expr);
        }
        Ok(())
    }

    fn matchers(&self) -> &[Matcher] {
        &self.matchers
    }
}

#[derive(Debug)]
struct SubqueryBodyVisitor {
    replacer: BindingReplacer,
    failed: bool,
    new_s_expr: Option<SExpr>,
}

impl SubqueryBodyVisitor {
    fn new(new_column_bindings: HashMap<IndexType, ColumnBinding>, expr: SExpr) -> Self {
        Self {
            replacer: BindingReplacer {
                new_column_bindings,
            },
            failed: false,
            new_s_expr: Some(expr),
        }
    }

    fn visit(&mut self, s_expr: &SExpr) -> Result<()> {
        for visit in s_expr.children() {
            self.visit(visit)?;
        }
        if self.failed {
            return Ok(());
        }
        if !matches!(
            s_expr.plan(),
            RelOperator::Scan(_)
                | RelOperator::Filter(_)
                | RelOperator::EvalScalar(_)
                | RelOperator::Sort(_)
                | RelOperator::Exchange(_)
        ) {
            self.failed = true;
            return Ok(());
        }
        match s_expr.plan() {
            RelOperator::Scan(_) | RelOperator::Sort(_) => (),
            RelOperator::EvalScalar(eval_scalar) => {
                let mut eval_scalar = eval_scalar.clone();

                for scalar_item in eval_scalar.items.iter_mut() {
                    self.replacer.visit(&mut scalar_item.scalar)?;
                }
                self.build_s_expr(RelOperator::EvalScalar(eval_scalar));
            }
            RelOperator::Filter(filter) => {
                let mut filter = filter.clone();

                for scalar_expr in filter.predicates.iter_mut() {
                    self.replacer.visit(scalar_expr)?;
                }
                self.build_s_expr(RelOperator::Filter(filter));
            }
            RelOperator::Exchange(exchange) => {
                let mut exchange = exchange.clone();
                if let Exchange::Hash(scalar_exprs) = &mut exchange {
                    for scalar_expr in scalar_exprs {
                        self.replacer.visit(scalar_expr)?;
                    }
                }
                self.build_s_expr(RelOperator::Exchange(exchange));
            }
            _ => {
                self.failed = true;
                return Ok(());
            }
        }
        Ok(())
    }

    fn build_s_expr(&mut self, op: RelOperator) {
        self.new_s_expr = Some(match self.new_s_expr.take() {
            None => SExpr::create_leaf(Arc::new(op)),
            Some(child_expr) => SExpr::create_unary(Arc::new(op), Arc::new(child_expr)),
        });
    }

    fn result(self) -> Option<SExpr> {
        (!self.failed).then_some(self.new_s_expr).flatten()
    }
}

#[derive(Debug)]
struct BindingReplacer {
    new_column_bindings: HashMap<IndexType, ColumnBinding>,
}

impl VisitorMut<'_> for BindingReplacer {
    fn visit_bound_column_ref(&mut self, col: &mut BoundColumnRef) -> Result<()> {
        if let Some(new_binding) = self.new_column_bindings.get(&col.column.index) {
            col.column = new_binding.clone();
        }
        Ok(())
    }
}

#[derive(Default)]
struct ExprAsSourceVisitor {
    failed: bool,
}

impl ExprAsSourceVisitor {
    fn success(&self) -> bool {
        !self.failed
    }
}

impl VisitorMut<'_> for ExprAsSourceVisitor {
    fn visit(&mut self, expr: &mut ScalarExpr) -> Result<()> {
        if self.failed {
            return Ok(());
        }
        walk_expr_mut(self, expr)
    }
    fn visit_bound_column_ref(&mut self, col: &mut BoundColumnRef) -> Result<()> {
        let Some(binding) = col.column.as_source() else {
            self.failed = true;
            return Ok(());
        };
        if binding.source_table_index.or(binding.table_index).is_none()
            || binding.column_position.is_none()
        {
            self.failed = true;
            return Ok(());
        }
        // filter table function
        if matches!(
            binding.database_name.as_ref().map(String::as_ref),
            Some("system")
        ) {
            return Ok(());
        }
        col.column = binding;
        Ok(())
    }
}
