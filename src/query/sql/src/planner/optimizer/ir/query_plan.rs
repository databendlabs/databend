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

use super::PExpr;
use crate::optimizer::ir::SExpr;

/// Query output from physical planning, including the enabled finalization passes.
/// Its private physical expression evolves independently of logical SExpr.
#[derive(Clone, Debug)]
pub struct PlannedQuery {
    expr: PExpr,
}

impl PlannedQuery {
    pub(in crate::planner::optimizer) fn new(expr: PExpr) -> Self {
        Self { expr }
    }

    /// Selected physical expression, independent of the logical expression type.
    pub fn expr(&self) -> &PExpr {
        &self.expr
    }

    pub(crate) fn into_expr(self) -> PExpr {
        self.expr
    }

    pub(crate) fn remove_root_merge(&self) -> Self {
        if matches!(
            self.expr.plan(),
            crate::plans::RelOperator::Exchange(crate::plans::Exchange::Merge)
        ) {
            Self::new(self.expr.unary_child().clone())
        } else {
            self.clone()
        }
    }
}

/// Statement container for the two query lifecycles. Logical optimizers never accept
/// this enum: they consume SExpr; execution consumes only PlannedQuery.
#[derive(Clone, Debug)]
pub enum QueryPlan {
    Logical(SExpr),
    Planned(PlannedQuery),
}

impl From<SExpr> for QueryPlan {
    fn from(expr: SExpr) -> Self {
        Self::Logical(expr)
    }
}

impl QueryPlan {
    pub fn into_logical(self) -> Result<SExpr> {
        match self {
            Self::Logical(expr) => Ok(expr),
            Self::Planned(_) => Err(ErrorCode::Internal(
                "Expected a bound logical query, not a planned query",
            )),
        }
    }

    pub fn planned(&self) -> Result<&PlannedQuery> {
        match self {
            Self::Planned(plan) => Ok(plan),
            Self::Logical(_) => Err(ErrorCode::Internal(
                "Query must be physically planned before execution",
            )),
        }
    }

    /// Inspect a logical query; planned queries must be accessed through `planned`.
    pub fn logical(&self) -> Result<&SExpr> {
        match self {
            Self::Logical(expr) => Ok(expr),
            Self::Planned(_) => Err(ErrorCode::Internal("Expected logical query")),
        }
    }

    pub fn get_udfs(&self) -> Result<std::collections::HashSet<&String>> {
        match self {
            Self::Logical(expr) => expr.get_udfs(),
            Self::Planned(plan) => plan.expr().get_udfs(),
        }
    }

    pub(crate) fn remove_root_merge(&self) -> Self {
        match self {
            Self::Planned(plan) => Self::Planned(plan.remove_root_merge()),
            Self::Logical(_) => self.clone(),
        }
    }
}
