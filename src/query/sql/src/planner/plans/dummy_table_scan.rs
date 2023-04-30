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
use common_exception::Result;

use super::Operator;
use crate::optimizer::ColumnSet;
use crate::optimizer::PhysicalProperty;
use crate::optimizer::RelExpr;
use crate::optimizer::RelationalProperty;
use crate::optimizer::StatInfo;
use crate::optimizer::Statistics;

#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub struct DummyTableScan;

impl DummyTableScan {
    pub fn used_columns(&self) -> Result<ColumnSet> {
        Ok(ColumnSet::new())
    }
}

impl Operator for DummyTableScan {
    fn rel_op(&self) -> super::RelOp {
        super::RelOp::DummyTableScan
    }

    fn derive_relational_prop(
        &self,
        _rel_expr: &crate::optimizer::RelExpr,
    ) -> Result<RelationalProperty> {
        Ok(RelationalProperty {
            output_columns: ColumnSet::new(),
            outer_columns: ColumnSet::new(),
            used_columns: ColumnSet::new(),
        })
    }

    fn derive_physical_prop(
        &self,
        _rel_expr: &crate::optimizer::RelExpr,
    ) -> Result<PhysicalProperty> {
        Ok(PhysicalProperty {
            distribution: crate::optimizer::Distribution::Serial,
        })
    }

    fn derive_cardinality(&self, _rel_expr: &RelExpr) -> Result<StatInfo> {
        Ok(StatInfo {
            cardinality: 1.0,
            statistics: Statistics {
                precise_cardinality: Some(1),
                column_stats: Default::default(),
            },
        })
    }

    fn compute_required_prop_child(
        &self,
        _ctx: Arc<dyn TableContext>,
        _rel_expr: &crate::optimizer::RelExpr,
        _child_index: usize,
        required: &crate::optimizer::RequiredProperty,
    ) -> Result<crate::optimizer::RequiredProperty> {
        Ok(required.clone())
    }
}
