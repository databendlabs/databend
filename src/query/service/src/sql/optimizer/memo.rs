// Copyright 2021 Datafuse Labs.
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

use std::collections::HashSet;

use common_exception::ErrorCode;
use common_exception::Result;

use super::RelExpr;
use super::RelationalProperty;
use crate::sql::optimizer::group::Group;
use crate::sql::optimizer::m_expr::MExpr;
use crate::sql::optimizer::s_expr::SExpr;
use crate::sql::plans::RelOperator;
use crate::sql::IndexType;

/// `Memo` is a search space which memoize possible plans of a query.
/// The plans inside `Memo` are organized with `Group`s.
/// Each `Group` is a set of logically equivalent relational expressions represented with `MExpr`.
#[derive(Clone)]
pub struct Memo {
    pub groups: Vec<Group>,
    pub root: Option<IndexType>,

    /// Hash table for detecting duplicated expressions.
    /// The entry is `(group_index, plan, children)`.
    pub m_expr_lookup_table: HashSet<(IndexType, RelOperator, Vec<IndexType>)>,
}

impl Memo {
    pub fn create() -> Self {
        Memo {
            groups: vec![],
            root: None,
            m_expr_lookup_table: HashSet::new(),
        }
    }

    pub fn root(&self) -> Option<&Group> {
        self.root.map(|index| &self.groups[index])
    }

    pub fn set_root(&mut self, group_index: IndexType) {
        self.root = Some(group_index);
    }

    // Initialize memo with given expression
    pub fn init(&mut self, s_expr: SExpr) -> Result<()> {
        let root = self.insert(None, s_expr)?;
        self.set_root(root);

        Ok(())
    }

    pub fn insert(&mut self, target_group: Option<IndexType>, s_expr: SExpr) -> Result<IndexType> {
        let mut children_group = vec![];
        for expr in s_expr.children() {
            // Insert children expressions recursively and collect their group indices
            let group = self.insert(None, expr.clone())?;
            children_group.push(group);
        }

        if let Some(group_index) = s_expr.original_group() {
            // The expression is extracted by PatternExtractor, no need to reinsert.
            return Ok(group_index);
        }

        // Create new group if not specified
        let group_index = match target_group {
            Some(index) => index,
            _ => {
                let rel_expr = RelExpr::with_s_expr(&s_expr);
                let relational_prop = rel_expr.derive_relational_prop()?;
                self.add_group(relational_prop)
            }
        };

        let plan = s_expr.plan();

        let m_expr = MExpr::create(
            group_index,
            self.group(group_index)?.num_exprs(),
            plan.clone(),
            children_group,
        );
        self.insert_m_expr(group_index, m_expr)?;

        Ok(group_index)
    }

    pub fn group(&self, index: IndexType) -> Result<&Group> {
        self.groups
            .get(index)
            .ok_or_else(|| ErrorCode::LogicalError(format!("Group index {} not found", index)))
    }

    pub fn insert_m_expr(&mut self, group_index: IndexType, m_expr: MExpr) -> Result<()> {
        if self.m_expr_lookup_table.insert((
            m_expr.group_index,
            m_expr.plan.clone(),
            m_expr.children.clone(),
        )) {
            self.group_mut(group_index).insert(m_expr)
        } else {
            Ok(())
        }
    }

    fn group_mut(&mut self, index: IndexType) -> &mut Group {
        &mut self.groups[index]
    }

    fn add_group(&mut self, relational_prop: RelationalProperty) -> IndexType {
        let group_index = self.groups.len();
        let group = Group::create(group_index, relational_prop);
        self.groups.push(group);
        group_index
    }
}
