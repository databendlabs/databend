// Copyright 2022 Datafuse Labs.
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

use common_catalog::table::TableStatistics;
use common_exception::Result;
use itertools::Itertools;

use crate::sql::optimizer::ColumnSet;
use crate::sql::optimizer::RelExpr;
use crate::sql::optimizer::RelationalProperty;
use crate::sql::plans::LogicalOperator;
use crate::sql::plans::Operator;
use crate::sql::plans::PhysicalOperator;
use crate::sql::plans::RelOp;
use crate::sql::plans::Scalar;
use crate::sql::plans::SortItem;
use crate::sql::IndexType;

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Prewhere {
    pub columns: ColumnSet,
    pub predicates: Vec<Scalar>,
}

#[derive(Clone, Debug)]
pub struct LogicalGet {
    pub table_index: IndexType,
    pub columns: ColumnSet,
    pub push_down_predicates: Option<Vec<Scalar>>,
    pub limit: Option<usize>,
    pub order_by: Option<Vec<SortItem>>,
    pub prewhere: Option<Prewhere>,

    // statistics will be ignored in comparison and hashing
    pub statistics: Option<TableStatistics>,
}

impl PartialEq for LogicalGet {
    fn eq(&self, other: &Self) -> bool {
        self.table_index == other.table_index
            && self.columns == other.columns
            && self.push_down_predicates == other.push_down_predicates
    }
}

impl Eq for LogicalGet {}

impl std::hash::Hash for LogicalGet {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        self.table_index.hash(state);
        for column in self.columns.iter().sorted() {
            column.hash(state);
        }
        self.push_down_predicates.hash(state);
    }
}

impl Operator for LogicalGet {
    fn rel_op(&self) -> RelOp {
        RelOp::LogicalGet
    }

    fn is_physical(&self) -> bool {
        false
    }

    fn is_logical(&self) -> bool {
        true
    }

    fn as_logical(&self) -> Option<&dyn LogicalOperator> {
        Some(self)
    }

    fn as_physical(&self) -> Option<&dyn PhysicalOperator> {
        None
    }
}

impl LogicalOperator for LogicalGet {
    fn derive_relational_prop<'a>(&self, _rel_expr: &RelExpr<'a>) -> Result<RelationalProperty> {
        Ok(RelationalProperty {
            output_columns: self.columns.clone(),
            outer_columns: Default::default(),
            cardinality: self
                .statistics
                .as_ref()
                .map_or(0.0, |stat| stat.num_rows.map_or(0.0, |num| num as f64)),
        })
    }
}
