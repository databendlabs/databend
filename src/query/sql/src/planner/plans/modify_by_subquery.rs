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

use std::collections::HashMap;
use std::collections::HashSet;
use std::fmt;
use std::sync::Arc;

use databend_common_catalog::table_context::TableContext;
use databend_common_exception::Result;
use databend_common_meta_app::schema::CatalogInfo;
use databend_common_meta_app::schema::TableInfo;
use databend_storages_common_table_meta::meta::TableSnapshot;
use itertools::Itertools;

use super::Operator;
use super::RelOp;
use super::SubqueryDesc;
use crate::evaluator::BlockOperator;
use crate::evaluator::RemoteBlockOperator;
use crate::executor::PhysicalPlan;
use crate::optimizer::PhysicalProperty;
use crate::optimizer::RelExpr;
use crate::optimizer::RelationalProperty;
use crate::optimizer::RequiredProperty;
use crate::optimizer::SelectivityEstimator;
use crate::optimizer::StatInfo;
use crate::optimizer::Statistics;
use crate::optimizer::MAX_SELECTIVITY;
use crate::ColumnSet;

#[derive(Clone, Debug, serde::Serialize, serde::Deserialize)]
pub enum RemoteSubqueryMutation {
    Delete,
    Update(Vec<RemoteBlockOperator>),
}

#[derive(Clone, Debug)]
pub enum SubqueryMutation {
    Delete,
    Update(Vec<BlockOperator>),
}

impl From<RemoteSubqueryMutation> for SubqueryMutation {
    fn from(typ: RemoteSubqueryMutation) -> Self {
        match typ {
            RemoteSubqueryMutation::Delete => Self::Delete,
            RemoteSubqueryMutation::Update(operators) => {
                let operators = operators
                    .into_iter()
                    .map(|operator| operator.into())
                    .collect::<Vec<_>>();
                Self::Update(operators)
            }
        }
    }
}

#[derive(Clone)]
pub struct ModifyBySubquery {
    pub filter: Box<PhysicalPlan>,
    pub snapshot: Arc<TableSnapshot>,
    pub output_columns: ColumnSet,
    pub subquery_desc: SubqueryDesc,
    pub typ: RemoteSubqueryMutation,
    pub table_info: TableInfo,
    pub catalog_info: CatalogInfo,
}

impl fmt::Debug for ModifyBySubquery {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(
            f,
            "ModifyBySubquery: snapshot_id: {:?}, output_columns: {:?}",
            self.snapshot.snapshot_id, self.output_columns
        )
    }
}

impl PartialEq for ModifyBySubquery {
    fn eq(&self, other: &Self) -> bool {
        self.snapshot.snapshot_id == other.snapshot.snapshot_id
            && self.output_columns == other.output_columns
    }
}

impl Eq for ModifyBySubquery {}

impl std::hash::Hash for ModifyBySubquery {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        self.snapshot.snapshot_id.hash(state);
        for column in self.output_columns.iter().sorted() {
            column.hash(state);
        }
    }
}

impl Operator for ModifyBySubquery {
    fn rel_op(&self) -> RelOp {
        RelOp::AsyncFunction
    }

    fn arity(&self) -> usize {
        1
    }

    fn derive_relational_prop(&self, _rel_expr: &RelExpr) -> Result<Arc<RelationalProperty>> {
        Ok(Arc::new(RelationalProperty {
            output_columns: self.output_columns.clone(),
            ..Default::default()
        }))
    }

    fn derive_physical_prop(&self, rel_expr: &RelExpr) -> Result<PhysicalProperty> {
        rel_expr.derive_physical_prop_child(0)
    }

    fn derive_stats(&self, rel_expr: &RelExpr) -> Result<Arc<StatInfo>> {
        let stat_info = rel_expr.derive_cardinality_child(0)?;
        let (input_cardinality, mut statistics) =
            (stat_info.cardinality, stat_info.statistics.clone());
        // Derive cardinality
        let mut sb = SelectivityEstimator::new(&mut statistics, HashSet::new());
        let selectivity = MAX_SELECTIVITY;
        // Update other columns's statistic according to selectivity.
        sb.update_other_statistic_by_selectivity(selectivity);
        let cardinality = input_cardinality * selectivity;
        // Derive column statistics
        let column_stats = if cardinality == 0.0 {
            HashMap::new()
        } else {
            statistics.column_stats
        };
        Ok(Arc::new(StatInfo {
            cardinality,
            statistics: Statistics {
                precise_cardinality: None,
                column_stats,
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
        Ok(required.clone())
    }
}
