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

use common_exception::ErrorCode;
use common_exception::Result;

use crate::optimizer::SExpr;
use crate::plans::LogicalGet;
use crate::plans::RelOperator;
use crate::plans::Scalar;
use crate::plans::VirtualColumnRef;

pub struct VirutalColumnOptimizer {
    virtual_columns: Vec<VirtualColumnRef>,
}

impl VirutalColumnOptimizer {
    pub fn new() -> Self {
        Self {
            virtual_columns: Vec::new(),
        }
    }

    pub fn optimize(&mut self, expr: &SExpr) -> Result<SExpr> {
        match expr.plan() {
            RelOperator::LogicalGet(p) => {
                if self.virtual_columns.is_empty() {
                    return Ok(expr.clone());
                }
                let mut virtual_columns = Vec::new();
                for virtual_column in &self.virtual_columns {
                    if p.columns.contains(&virtual_column.column.index) {
                        virtual_columns.push(virtual_column.clone());
                    }
                }
                if virtual_columns.is_empty() {
                    return Ok(expr.clone());
                }
                Ok(SExpr::create_leaf(RelOperator::LogicalGet(LogicalGet {
                    table_index: p.table_index,
                    columns: p.columns.clone(),
                    push_down_predicates: p.push_down_predicates.clone(),
                    limit: p.limit,
                    order_by: p.order_by.clone(),
                    statistics: p.statistics.clone(),
                    prewhere: p.prewhere.clone(),
                    virtual_columns: Some(virtual_columns),
                })))
            }
            RelOperator::LogicalJoin(p) => Ok(SExpr::create_binary(
                RelOperator::LogicalJoin(p.clone()),
                self.optimize(expr.child(0)?)?,
                self.optimize(expr.child(1)?)?,
            )),
            RelOperator::EvalScalar(p) => {
                for s in p.items.iter() {
                    if let Scalar::VirtualColumnRef(ref column) = s.scalar {
                        self.virtual_columns.push(column.clone());
                    }
                }
                Ok(SExpr::create_unary(
                    RelOperator::EvalScalar(p.clone()),
                    self.optimize(expr.child(0)?)?,
                ))
            }
            RelOperator::Filter(p) => Ok(SExpr::create_unary(
                RelOperator::Filter(p.clone()),
                self.optimize(expr.child(0)?)?,
            )),
            RelOperator::Aggregate(p) => Ok(SExpr::create_unary(
                RelOperator::Aggregate(p.clone()),
                self.optimize(expr.child(0)?)?,
            )),
            RelOperator::Sort(p) => Ok(SExpr::create_unary(
                RelOperator::Sort(p.clone()),
                self.optimize(expr.child(0)?)?,
            )),
            RelOperator::Limit(p) => Ok(SExpr::create_unary(
                RelOperator::Limit(p.clone()),
                self.optimize(expr.child(0)?)?,
            )),
            RelOperator::DummyTableScan(_) | RelOperator::UnionAll(_) => Ok(expr.clone()),

            _ => Err(ErrorCode::Internal(
                "Attempting to do virtual columns of a physical plan is not allowed",
            )),
        }
    }
}
