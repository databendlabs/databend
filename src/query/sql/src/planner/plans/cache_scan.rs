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
use databend_common_expression::DataSchemaRef;

use crate::optimizer::ColumnSet;
use crate::optimizer::Distribution;
use crate::optimizer::PhysicalProperty;
use crate::optimizer::RelExpr;
use crate::optimizer::RelationalProperty;
use crate::optimizer::RequiredProperty;
use crate::optimizer::StatInfo;
use crate::optimizer::Statistics;
use crate::plans::Operator;
use crate::plans::RelOp;

#[derive(Clone, Debug, PartialEq, Eq, Hash, serde::Serialize, serde::Deserialize)]
pub enum CacheSource {
    HashJoinBuild((usize, Vec<usize>)),
}

impl CacheSource {
    pub fn project(&self, projection: &[usize]) -> Self {
        match self {
            CacheSource::HashJoinBuild((cache_index, column_indexes)) => {
                let column_indexes = column_indexes.iter().map(|idx| projection[*idx]).collect();
                CacheSource::HashJoinBuild((*cache_index, column_indexes))
            }
        }
    }
}

// Constant table is a table with constant values.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct CacheScan {
    pub cache_source: CacheSource,
    pub columns: ColumnSet,
    pub schema: DataSchemaRef,
}

impl CacheScan {
    pub fn prune_columns(&self, columns: ColumnSet) -> Self {
        let mut projection = columns
            .iter()
            .map(|index| self.schema.index_of(&index.to_string()).unwrap())
            .collect::<Vec<_>>();
        projection.sort();

        let schema = Arc::new(self.schema.project(&projection));
        let cache_source = self.cache_source.project(&projection);
        let columns = self.columns.intersection(&columns).cloned().collect();

        CacheScan {
            cache_source,
            schema,
            columns,
        }
    }

    pub fn used_columns(&self) -> Result<ColumnSet> {
        Ok(ColumnSet::new())
    }
}

impl std::hash::Hash for CacheScan {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        self.cache_source.hash(state);
        let mut column = self.columns.iter().collect::<Vec<_>>();
        column.sort();
        for column in column.iter() {
            column.hash(state);
        }
    }
}

impl Operator for CacheScan {
    fn rel_op(&self) -> RelOp {
        RelOp::CacheScan
    }

    fn arity(&self) -> usize {
        0
    }

    fn derive_relational_prop(&self, _rel_expr: &RelExpr) -> Result<Arc<RelationalProperty>> {
        Ok(Arc::new(RelationalProperty {
            output_columns: self.columns.clone(),
            outer_columns: Default::default(),
            used_columns: Default::default(),
            orderings: vec![],
            partition_orderings: None,
        }))
    }

    fn derive_physical_prop(&self, _rel_expr: &RelExpr) -> Result<PhysicalProperty> {
        Ok(PhysicalProperty {
            distribution: Distribution::Serial,
        })
    }

    fn derive_stats(&self, _rel_expr: &RelExpr) -> Result<Arc<StatInfo>> {
        Ok(Arc::new(StatInfo {
            cardinality: 0.0,
            statistics: Statistics {
                precise_cardinality: None,
                column_stats: Default::default(),
            },
        }))
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
        Ok(required.clone())
    }
}
