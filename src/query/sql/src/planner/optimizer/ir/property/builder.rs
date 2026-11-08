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

use databend_common_catalog::table_context::TableContext;
use databend_common_exception::ErrorCode;
use databend_common_exception::Result;

use crate::IndexType;
use crate::optimizer::ir::PExpr;
use crate::optimizer::ir::SExpr;
use crate::optimizer::ir::StatContext;
use crate::optimizer::ir::StatInfo;
use crate::optimizer::ir::expr::MExpr;
use crate::optimizer::ir::memo::Memo;
use crate::optimizer::ir::property::PhysicalProperty;
use crate::optimizer::ir::property::RelationalProperty;
use crate::optimizer::ir::property::RequiredProperty;
use crate::plans::Operator;
use crate::plans::PhysicalOperator;

/// Borrow concrete trees or Memo contexts for property derivation. Tree access
/// uses static dispatch and preserves the tree's caches; Memo child properties
/// come from their referenced groups or the current optimization context.
pub enum RelExpr<'a> {
    PExpr {
        expr: &'a PExpr,
    },
    SExpr {
        expr: &'a SExpr,
    },
    MExpr {
        expr: &'a MExpr,
        memo: &'a Memo,
    },
    OptContext {
        expr: &'a MExpr,
        memo: &'a Memo,
        children_best_props: &'a [PhysicalProperty],
    },
}

impl<'a> RelExpr<'a> {
    pub fn with_p_expr(expr: &'a PExpr) -> Self {
        Self::PExpr { expr }
    }

    pub fn with_s_expr(expr: &'a SExpr) -> Self {
        Self::SExpr { expr }
    }

    pub fn with_m_expr(m_expr: &'a MExpr, memo: &'a Memo) -> Self {
        Self::MExpr { expr: m_expr, memo }
    }

    pub fn with_opt_context(
        m_expr: &'a MExpr,
        memo: &'a Memo,
        children_best_props: &'a [PhysicalProperty],
    ) -> Self {
        Self::OptContext {
            expr: m_expr,
            memo,
            children_best_props,
        }
    }

    #[recursive::recursive]
    pub fn derive_relational_prop(&self) -> Result<Arc<RelationalProperty>> {
        match self {
            RelExpr::PExpr { expr } => expr.derive_relational_prop(),
            RelExpr::SExpr { expr } => expr.derive_relational_prop(),
            RelExpr::MExpr { expr, .. } | RelExpr::OptContext { expr, .. } => {
                expr.plan.derive_relational_prop(self)
            }
        }
    }

    pub fn derive_relational_prop_child(&self, index: usize) -> Result<Arc<RelationalProperty>> {
        match self {
            RelExpr::PExpr { expr } => expr.child(index)?.derive_relational_prop(),
            RelExpr::SExpr { expr } => expr.child(index)?.derive_relational_prop(),
            RelExpr::MExpr { expr, memo } | RelExpr::OptContext { expr, memo, .. } => {
                Ok(expr.child_group(memo, index)?.relational_prop.clone())
            }
        }
    }

    #[recursive::recursive]
    pub fn derive_cardinality(&self, stat_ctx: &StatContext) -> Result<Arc<StatInfo>> {
        match self {
            RelExpr::PExpr { expr } => expr.derive_cardinality(stat_ctx),
            RelExpr::SExpr { expr } => expr.derive_cardinality(stat_ctx),
            RelExpr::MExpr { expr, .. } | RelExpr::OptContext { expr, .. } => {
                expr.plan.derive_stats(self, stat_ctx)
            }
        }
    }

    pub(crate) fn derive_cardinality_child(
        &self,
        index: IndexType,
        stat_ctx: &StatContext,
    ) -> Result<Arc<StatInfo>> {
        match self {
            RelExpr::PExpr { expr } => expr.child(index)?.derive_cardinality(stat_ctx),
            RelExpr::SExpr { expr } => expr.child(index)?.derive_cardinality(stat_ctx),
            RelExpr::MExpr { expr, memo } | RelExpr::OptContext { expr, memo, .. } => {
                Ok(expr.child_group(memo, index)?.stat_info.clone())
            }
        }
    }

    #[recursive::recursive]
    pub fn derive_physical_prop(&self) -> Result<PhysicalProperty> {
        match self {
            RelExpr::PExpr { expr } => expr.plan().derive_physical_prop(self),
            RelExpr::SExpr { .. } => Err(Self::logical_physical_error()),
            RelExpr::MExpr { expr, .. } | RelExpr::OptContext { expr, .. } => {
                expr.plan.derive_physical_prop(self)
            }
        }
    }

    pub fn derive_physical_prop_child(&self, index: usize) -> Result<PhysicalProperty> {
        match self {
            RelExpr::PExpr { expr } => Self::with_p_expr(expr.child(index)?).derive_physical_prop(),
            RelExpr::SExpr { .. } => Err(Self::logical_physical_error()),
            RelExpr::MExpr { .. } => Err(ErrorCode::Internal(
                "Cannot derive physical property from MExpr".to_string(),
            )),
            RelExpr::OptContext {
                children_best_props,
                ..
            } => Ok(children_best_props
                .get(index)
                .ok_or_else(|| ErrorCode::Internal("Cannot find child best property".to_string()))?
                .clone()),
        }
    }

    pub fn compute_required_prop_child(
        &self,
        ctx: Arc<dyn TableContext>,
        index: usize,
        input: &RequiredProperty,
    ) -> Result<RequiredProperty> {
        match self {
            RelExpr::PExpr { expr } => expr
                .plan()
                .compute_required_prop_child(ctx, self, index, input),
            RelExpr::SExpr { .. } => Err(Self::logical_physical_error()),
            RelExpr::MExpr { expr, .. } | RelExpr::OptContext { expr, .. } => expr
                .plan
                .compute_required_prop_child(ctx, self, index, input),
        }
    }

    pub fn compute_required_prop_children(
        &self,
        ctx: Arc<dyn TableContext>,
        input: &RequiredProperty,
    ) -> Result<Vec<Vec<RequiredProperty>>> {
        match self {
            RelExpr::PExpr { expr } => expr.plan().compute_required_prop_children(ctx, self, input),
            RelExpr::SExpr { .. } => Err(Self::logical_physical_error()),
            RelExpr::MExpr { expr, .. } | RelExpr::OptContext { expr, .. } => {
                expr.plan.compute_required_prop_children(ctx, self, input)
            }
        }
    }

    fn logical_physical_error() -> ErrorCode {
        ErrorCode::Internal(
            "Cannot derive execution properties from a logical expression".to_string(),
        )
    }
}
