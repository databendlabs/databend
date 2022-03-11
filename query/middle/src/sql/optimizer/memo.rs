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

use std::fmt::Debug;
use std::fmt::Formatter;

use common_exception::Result;

use crate::sql::optimizer::group::Group;
use crate::sql::optimizer::m_expr::MExpr;
use crate::sql::optimizer::s_expr::SExpr;
use crate::sql::IndexType;

/// `Memo` is a search space which memoize possible plans of a query.
/// The plans inside `Memo` are organized with `Group`s.
/// Each `Group` is a set of logically equivalent relational expressions represented with `MExpr`.
#[derive(Clone)]
pub struct Memo {
    groups: Vec<Group>,
    root: Option<IndexType>,
}

impl Memo {
    pub fn create() -> Self {
        Memo {
            groups: vec![],
            root: None,
        }
    }

    pub fn root(&self) -> Option<&Group> {
        self.root.map(|index| &self.groups[index])
    }

    pub fn set_root(&mut self, group_index: IndexType) {
        self.root = Some(group_index);
    }

    // Initialize memo with given expression
    pub fn init(&mut self, expression: SExpr) -> Result<()> {
        let root = self.insert(None, expression)?;
        self.set_root(root);

        Ok(())
    }

    pub fn insert(
        &mut self,
        target_group: Option<IndexType>,
        expression: SExpr,
    ) -> Result<IndexType> {
        let mut children_group = vec![];
        for expr in expression.children() {
            // Insert children expressions recursively and collect their group indices
            let group = self.insert(None, expr.clone())?;
            children_group.push(group);
        }

        if let Some(group_index) = expression.original_group() {
            // The expression is extracted by PatternExtractor, no need to reinsert.
            return Ok(group_index);
        }

        let mut new_group = false;

        // Create new group if not specified
        let group_index = match target_group {
            Some(index) => index,
            _ => {
                new_group = true;
                self.add_group()
            }
        };

        if new_group {
            let group = self.group_mut(group_index);
            let relational_prop = expression.compute_relational_prop();
            group.set_relational_prop(relational_prop);
        }

        let plan = expression.plan();

        let group_expression = MExpr::create(group_index, plan, children_group);
        self.insert_m_expr(group_index, group_expression)?;

        Ok(group_index)
    }

    pub fn group(&self, index: IndexType) -> &Group {
        &self.groups[index]
    }

    pub fn insert_m_expr(&mut self, group_index: IndexType, expression: MExpr) -> Result<()> {
        self.group_mut(group_index).insert(expression)
    }

    fn group_mut(&mut self, index: IndexType) -> &mut Group {
        &mut self.groups[index]
    }

    fn add_group(&mut self) -> IndexType {
        let group_index = self.groups.len();
        let group = Group::create(group_index);
        self.groups.push(group);
        group_index
    }
}

impl Debug for Memo {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        writeln!(f, "{{")?;
        for group in self.groups.iter() {
            writeln!(f, "\tGroup_{}: {:?}", group.group_index(), group)?;
        }
        write!(f, "}}")?;
        Ok(())
    }
}
