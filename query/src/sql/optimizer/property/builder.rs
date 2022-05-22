// Copyright 2022 Datafuse Labs.
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

use common_exception::ErrorCode;
use common_exception::Result;

use crate::sql::optimizer::MExpr;
use crate::sql::optimizer::Memo;
use crate::sql::optimizer::RelationalProperty;
use crate::sql::optimizer::SExpr;
use crate::sql::plans::Operator;

/// A helper to access children of `SExpr` and `MExpr` in
/// a unified view.
pub enum RelExpr<'a> {
    SExpr { expr: &'a SExpr },
    MExpr { expr: &'a MExpr, memo: &'a Memo },
}

impl<'a> RelExpr<'a> {
    pub fn with_s_expr(s_expr: &'a SExpr) -> Self {
        Self::SExpr { expr: s_expr }
    }

    pub fn with_m_expr(m_expr: &'a MExpr, memo: &'a Memo) -> Self {
        Self::MExpr { expr: m_expr, memo }
    }

    pub fn derive_relational_prop(&self) -> Result<RelationalProperty> {
        let plan = match self {
            RelExpr::SExpr { expr } => expr.plan(),
            RelExpr::MExpr { expr, .. } => expr.plan(),
        };

        if let Some(logical) = plan.as_logical() {
            let prop = logical.derive_relational_prop(self)?;
            Ok(prop)
        } else {
            Err(ErrorCode::LogicalError(
                "Cannot derive relational property from physical plan".to_string(),
            ))
        }
    }

    pub fn derive_relational_prop_child(&self, index: usize) -> Result<RelationalProperty> {
        match self {
            RelExpr::SExpr { expr } => {
                let child = expr.child(index)?;
                let rel_expr = RelExpr::with_s_expr(child);
                rel_expr.derive_relational_prop()
            }
            RelExpr::MExpr { expr, memo } => memo
                .group(expr.group_index())
                .relational_prop()
                .cloned()
                .ok_or_else(|| {
                    ErrorCode::LogicalError(
                        "Relational property should have been filled".to_string(),
                    )
                }),
        }
    }
}
