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
use databend_common_exception::Result;

use crate::ColumnSet;
use crate::optimizer::ir::Distribution;
use crate::optimizer::ir::RelExpr;
use crate::optimizer::ir::RelationalProperty;
use crate::optimizer::ir::RequiredProperty;
use crate::optimizer::ir::StatInfo;
use crate::optimizer::ir::Statistics;
use crate::plans::Operator;
use crate::plans::RelOp;

#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub struct Limit {
    pub before_exchange: bool,
    pub limit: Option<usize>,
    pub offset: usize,
    pub lazy_columns: ColumnSet,
}

impl Limit {
    pub fn derive_limit_stats(&self, stat_info: Arc<StatInfo>) -> Result<Arc<StatInfo>> {
        let cardinality = match self.limit {
            Some(limit) if (limit as f64) < stat_info.cardinality => limit as f64,
            _ => stat_info.cardinality,
        };
        let precise_cardinality = match (self.limit, stat_info.statistics.precise_cardinality) {
            (Some(limit), Some(pc)) => {
                Some((pc.saturating_sub(self.offset as u64)).min(limit as u64))
            }
            _ => None,
        };

        Ok(Arc::new(StatInfo {
            cardinality,
            statistics: Statistics {
                precise_cardinality,
                column_stats: Default::default(),
            },
        }))
    }

    pub fn without_lazy_columns(&self) -> Limit {
        Limit {
            before_exchange: self.before_exchange,
            limit: self.limit,
            offset: self.offset,
            lazy_columns: Default::default(),
        }
    }
}

impl Operator for Limit {
    fn rel_op(&self) -> RelOp {
        RelOp::Limit
    }

    fn compute_required_prop_child(
        &self,
        _ctx: Arc<dyn TableContext>,
        _rel_expr: &RelExpr,
        _child_index: usize,
        required: &RequiredProperty,
    ) -> Result<RequiredProperty> {
        let mut required = required.clone();
        required.distribution = Distribution::Serial;
        Ok(required)
    }

    fn compute_required_prop_children(
        &self,
        _ctx: Arc<dyn TableContext>,
        _rel_expr: &RelExpr,
        _required: &RequiredProperty,
    ) -> Result<Vec<Vec<RequiredProperty>>> {
        Ok(vec![vec![RequiredProperty {
            distribution: Distribution::Serial,
        }]])
    }

    fn derive_relational_prop(&self, rel_expr: &RelExpr) -> Result<Arc<RelationalProperty>> {
        rel_expr.derive_relational_prop_child(0)
    }

    fn derive_stats(&self, rel_expr: &RelExpr) -> Result<Arc<StatInfo>> {
        let stat_info = rel_expr.derive_cardinality_child(0)?;
        self.derive_limit_stats(stat_info)
    }
}
