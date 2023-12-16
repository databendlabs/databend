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

use super::Operator;
use super::RelOp;
use crate::optimizer::PhysicalProperty;
use crate::optimizer::RelExpr;
use crate::optimizer::RelationalProperty;
use crate::optimizer::RequiredProperty;
use crate::optimizer::StatInfo;

#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub struct AddRowNumber;

impl Operator for AddRowNumber {
    fn rel_op(&self) -> RelOp {
        RelOp::AddRowNumber
    }

    fn derive_relational_prop(
        &self,
        rel_expr: &RelExpr,
    ) -> databend_common_exception::Result<Arc<RelationalProperty>> {
        rel_expr.derive_relational_prop_child(0)
    }

    fn derive_physical_prop(
        &self,
        rel_expr: &RelExpr,
    ) -> databend_common_exception::Result<PhysicalProperty> {
        rel_expr.derive_physical_prop_child(0)
    }

    fn derive_cardinality(
        &self,
        rel_expr: &RelExpr,
    ) -> databend_common_exception::Result<Arc<StatInfo>> {
        rel_expr.derive_cardinality_child(0)
    }

    fn compute_required_prop_child(
        &self,
        _ctx: Arc<dyn TableContext>,
        _rel_expr: &RelExpr,
        _child_index: usize,
        required: &RequiredProperty,
    ) -> databend_common_exception::Result<RequiredProperty> {
        Ok(required.clone())
    }
}
