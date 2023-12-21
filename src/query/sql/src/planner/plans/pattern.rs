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

use crate::optimizer::PhysicalProperty;
use crate::optimizer::RelExpr;
use crate::optimizer::RelationalProperty;
use crate::optimizer::RequiredProperty;
use crate::optimizer::StatInfo;
use crate::plans::Operator;
use crate::plans::RelOp;

#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub struct PatternPlan {
    pub plan_type: RelOp,
}

impl Operator for PatternPlan {
    fn rel_op(&self) -> RelOp {
        self.plan_type.clone()
    }

    fn is_pattern(&self) -> bool {
        true
    }

    fn derive_relational_prop(
        &self,
        _rel_expr: &RelExpr,
    ) -> databend_common_exception::Result<Arc<RelationalProperty>> {
        Err(ErrorCode::Internal(
            "Cannot derive relational property for pattern plan",
        ))
    }

    fn derive_physical_prop(
        &self,
        _rel_expr: &RelExpr,
    ) -> databend_common_exception::Result<PhysicalProperty> {
        Err(ErrorCode::Internal(
            "Cannot derive physical property for pattern plan",
        ))
    }

    fn derive_cardinality(
        &self,
        _rel_expr: &RelExpr,
    ) -> databend_common_exception::Result<Arc<StatInfo>> {
        Err(ErrorCode::Internal(
            "Cannot derive cardinality for pattern plan",
        ))
    }

    fn compute_required_prop_child(
        &self,
        _ctx: Arc<dyn TableContext>,
        _rel_expr: &RelExpr,
        _child_index: usize,
        _required: &RequiredProperty,
    ) -> databend_common_exception::Result<RequiredProperty> {
        unreachable!()
    }
}
