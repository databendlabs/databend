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

use common_catalog::table_context::TableContext;

use crate::optimizer::PhysicalProperty;
use crate::optimizer::RelExpr;
use crate::optimizer::RelationalProperty;
use crate::optimizer::RequiredProperty;
use crate::optimizer::StatInfo;
use crate::plans::Operator;
use crate::plans::RelOp;
use crate::IndexType;
use crate::ScalarExpr;

/// An item of set-returning function.
/// Contains definition of srf and its output columns.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct SrfItem {
    pub scalar: ScalarExpr,
    // pub srf_name: String,
    // pub args: Vec<ScalarExpr>,
    pub index: IndexType,
}

/// `ProjectSet` is a plan that evaluate a series of
/// set-returning functions, zip the result together,
/// and return the joined result with input relation.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct ProjectSet {
    pub srfs: Vec<SrfItem>,
}

impl Operator for ProjectSet {
    fn rel_op(&self) -> RelOp {
        RelOp::ProjectSet
    }

    fn derive_relational_prop(
        &self,
        rel_expr: &RelExpr,
    ) -> common_exception::Result<RelationalProperty> {
        let mut child_prop = rel_expr.derive_relational_prop_child(0)?;
        for srf in &self.srfs {
            child_prop.output_columns.insert(srf.index);
        }
        Ok(child_prop)
    }

    fn derive_physical_prop(
        &self,
        rel_expr: &RelExpr,
    ) -> common_exception::Result<PhysicalProperty> {
        rel_expr.derive_physical_prop_child(0)
    }

    fn derive_cardinality(&self, rel_expr: &RelExpr) -> common_exception::Result<StatInfo> {
        let mut input_stat = rel_expr.derive_cardinality_child(0)?;
        // ProjectSet is set-returning functions, precise_cardinality set None
        input_stat.statistics.precise_cardinality = None;
        Ok(input_stat)
    }

    fn compute_required_prop_child(
        &self,
        _ctx: Arc<dyn TableContext>,
        _rel_expr: &RelExpr,
        _child_index: usize,
        required: &RequiredProperty,
    ) -> common_exception::Result<RequiredProperty> {
        Ok(required.clone())
    }
}
