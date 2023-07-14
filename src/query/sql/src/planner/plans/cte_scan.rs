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

use std::hash::Hash;
use std::sync::Arc;

use common_catalog::table_context::TableContext;
use common_exception::Result;
use common_expression::DataBlock;
use common_storages_memory::MemoryTable;
use parking_lot::RwLock;

use crate::optimizer::ColumnSet;
use crate::optimizer::PhysicalProperty;
use crate::optimizer::RelExpr;
use crate::optimizer::RelationalProperty;
use crate::optimizer::RequiredProperty;
use crate::optimizer::StatInfo;
use crate::optimizer::Statistics;
use crate::plans::Operator;
use crate::plans::RelOp;

#[derive(Clone, Debug)]
pub struct CteScan {
    pub cte_idx: usize,
    pub memory_table: Arc<RwLock<Vec<DataBlock>>>,
}

impl CteScan {
    pub fn used_columns(&self) -> Result<ColumnSet> {
        Ok(ColumnSet::new())
    }
}

impl PartialEq for CteScan {
    fn eq(&self, other: &Self) -> bool {
        self.cte_idx == other.cte_idx
    }
}

impl Eq for CteScan {}

impl Hash for CteScan {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        self.cte_idx.hash(state);
    }
}

impl Operator for CteScan {
    fn rel_op(&self) -> RelOp {
        RelOp::CteScan
    }

    fn derive_relational_prop(&self, rel_expr: &RelExpr) -> Result<Arc<RelationalProperty>> {
        Ok(Arc::new(RelationalProperty {
            output_columns: ColumnSet::new(),
            outer_columns: ColumnSet::new(),
            used_columns: ColumnSet::new(),
        }))
    }

    fn derive_physical_prop(&self, _rel_expr: &RelExpr) -> Result<PhysicalProperty> {
        Ok(PhysicalProperty {
            distribution: crate::optimizer::Distribution::Random,
        })
    }

    fn derive_cardinality(&self, rel_expr: &RelExpr) -> Result<Arc<StatInfo>> {
        Ok(Arc::new(StatInfo {
            cardinality: 1.0,
            statistics: Statistics {
                precise_cardinality: None,
                column_stats: Default::default(),
            },
        }))
    }

    fn compute_required_prop_child(
        &self,
        ctx: Arc<dyn TableContext>,
        rel_expr: &RelExpr,
        child_index: usize,
        required: &RequiredProperty,
    ) -> Result<RequiredProperty> {
        unreachable!()
    }
}
