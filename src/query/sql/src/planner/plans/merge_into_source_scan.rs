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

use super::Operator;

#[derive(Clone, Debug, Hash, PartialEq)]
pub struct MergeIntoSourceScan {}

impl Operator for MergeIntoSourceScan {
    fn rel_op(&self) -> super::RelOp {
        todo!()
    }

    fn derive_relational_prop(
        &self,
        rel_expr: &crate::optimizer::RelExpr,
    ) -> common_exception::Result<std::sync::Arc<crate::optimizer::RelationalProperty>> {
        todo!()
    }

    fn derive_physical_prop(
        &self,
        rel_expr: &crate::optimizer::RelExpr,
    ) -> common_exception::Result<crate::optimizer::PhysicalProperty> {
        todo!()
    }

    fn derive_cardinality(
        &self,
        rel_expr: &crate::optimizer::RelExpr,
    ) -> common_exception::Result<std::sync::Arc<crate::optimizer::StatInfo>> {
        todo!()
    }

    fn compute_required_prop_child(
        &self,
        ctx: std::sync::Arc<dyn common_catalog::table_context::TableContext>,
        rel_expr: &crate::optimizer::RelExpr,
        child_index: usize,
        required: &crate::optimizer::RequiredProperty,
    ) -> common_exception::Result<crate::optimizer::RequiredProperty> {
        todo!()
    }
}

impl std::cmp::Eq for MergeIntoSourceScan {}
