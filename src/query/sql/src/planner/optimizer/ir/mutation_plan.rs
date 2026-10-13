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
use crate::plans::Mutation;

/// Mutation binding and input-plan selection have distinct tree representations.
/// Execution accepts only the planned tree; logical preparation never sees PExpr.
#[derive(Clone, Debug)]
pub enum MutationPlan {
    Logical(SExpr),
    Planned(PExpr),
}

impl From<SExpr> for MutationPlan {
    fn from(expr: SExpr) -> Self {
        Self::Logical(expr)
    }
}

impl MutationPlan {
    pub fn logical(&self) -> Result<&SExpr> {
        match self {
            Self::Logical(expr) => Ok(expr),
            Self::Planned(_) => Err(ErrorCode::Internal("Expected a bound logical mutation")),
        }
    }

    pub fn into_logical(self) -> Result<SExpr> {
        match self {
            Self::Logical(expr) => Ok(expr),
            Self::Planned(_) => Err(ErrorCode::Internal("Expected a bound logical mutation")),
        }
    }

    pub fn planned(&self) -> Result<&PExpr> {
        match self {
            Self::Planned(expr) => Ok(expr),
            Self::Logical(_) => Err(ErrorCode::Internal(
                "Mutation must be physically planned before execution",
            )),
        }
    }

    pub fn into_planned(self) -> Result<PExpr> {
        match self {
            Self::Planned(expr) => Ok(expr),
            Self::Logical(_) => Err(ErrorCode::Internal(
                "Mutation must be physically planned before execution",
            )),
        }
    }

    pub fn mutation(&self) -> Result<&Mutation> {
        let plan = match self {
            Self::Logical(expr) => expr.plan(),
            Self::Planned(expr) => expr.plan(),
        };
        plan.as_mutation()
            .ok_or_else(|| ErrorCode::Internal("Expected a mutation root"))
    }

    pub fn input_udfs(&self) -> Result<std::collections::HashSet<&String>> {
        match self {
            Self::Logical(expr) => expr.child(0)?.get_udfs(),
            Self::Planned(expr) => expr.child(0)?.get_udfs(),
        }
    }
}
