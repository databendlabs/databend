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

use crate::optimizer::PhysicalProperty;
use crate::optimizer::RelExpr;
use crate::optimizer::RelationalProperty;
use crate::optimizer::RequiredProperty;
use crate::optimizer::StatInfo;
use crate::plans::Operator;
use crate::plans::RelOp;

#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub struct RecursiveCte {}

impl RecursiveCte {}

impl Operator for RecursiveCte {
    fn rel_op(&self) -> RelOp {
        todo!()
    }

    fn arity(&self) -> usize {
        todo!()
    }

    fn derive_relational_prop(
        &self,
        rel_expr: &RelExpr,
    ) -> databend_common_exception::Result<Arc<RelationalProperty>> {
        todo!()
    }

    fn derive_physical_prop(
        &self,
        rel_expr: &RelExpr,
    ) -> databend_common_exception::Result<PhysicalProperty> {
        todo!()
    }

    fn derive_stats(&self, rel_expr: &RelExpr) -> databend_common_exception::Result<Arc<StatInfo>> {
        todo!()
    }

    fn compute_required_prop_child(
        &self,
        ctx: Arc<dyn TableContext>,
        rel_expr: &RelExpr,
        child_index: usize,
        required: &RequiredProperty,
    ) -> databend_common_exception::Result<RequiredProperty> {
        todo!()
    }

    fn compute_required_prop_children(
        &self,
        _ctx: Arc<dyn TableContext>,
        _rel_expr: &RelExpr,
        _required: &RequiredProperty,
    ) -> databend_common_exception::Result<Vec<Vec<RequiredProperty>>> {
        todo!()
    }
}
