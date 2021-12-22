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
use std::iter::Iterator;

use common_arrow::arrow_format::ipc::flatbuffers::bitflags::_core::fmt::Formatter;
use common_exception::Result;

use crate::sql::optimizer::m_expr::MExpr;
use crate::sql::optimizer::property::RelationalProperty;
use crate::sql::IndexType;

/// `Group` is a set of logically equivalent relational expressions represented with `MExpr`.
#[derive(Clone)]
pub struct Group {
    group_index: IndexType,
    expressions: Vec<MExpr>,

    /// Relational property shared by expressions in a same `Group`
    relational_prop: Option<RelationalProperty>,
}

impl Group {
    pub fn create(index: IndexType) -> Self {
        Group {
            group_index: index,
            expressions: vec![],
            relational_prop: None,
        }
    }

    pub fn group_index(&self) -> IndexType {
        self.group_index
    }

    pub fn iter(&self) -> impl Iterator<Item = &MExpr> {
        self.expressions.iter()
    }

    pub fn insert(&mut self, group_expression: MExpr) -> Result<()> {
        self.expressions.push(group_expression);
        Ok(())
    }

    pub fn set_relational_prop(&mut self, relational_prop: RelationalProperty) {
        self.relational_prop = Some(relational_prop);
    }

    pub fn relational_prop(&self) -> Option<&RelationalProperty> {
        self.relational_prop.as_ref()
    }
}

impl Debug for Group {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "[")?;
        for (index, m_expr) in self.expressions.iter().enumerate() {
            write!(f, "{:?}", m_expr)?;
            if index < self.expressions.len() - 1 {
                write!(f, ", ")?;
            }
        }
        write!(f, "]")?;

        Ok(())
    }
}
