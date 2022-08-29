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

use common_exception::Result;

use crate::sql::optimizer::m_expr::MExpr;
use crate::sql::optimizer::property::RelationalProperty;
use crate::sql::IndexType;

/// `Group` is a set of logically equivalent relational expressions represented with `MExpr`.
#[derive(Clone)]
pub struct Group {
    pub group_index: IndexType,
    pub m_exprs: Vec<MExpr>,

    /// Relational property shared by expressions in a same `Group`
    pub relational_prop: RelationalProperty,
}

impl Group {
    pub fn create(index: IndexType, relational_prop: RelationalProperty) -> Self {
        Group {
            group_index: index,
            m_exprs: vec![],
            relational_prop,
        }
    }

    pub fn group_index(&self) -> IndexType {
        self.group_index
    }

    pub fn num_exprs(&self) -> usize {
        self.m_exprs.len()
    }

    pub fn insert(&mut self, m_expr: MExpr) -> Result<()> {
        self.m_exprs.push(m_expr);
        Ok(())
    }
}
