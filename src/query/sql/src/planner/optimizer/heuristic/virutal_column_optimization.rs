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

use std::collections::HashMap;

use common_exception::ErrorCode;
use common_exception::Result;

use crate::optimizer::SExpr;
use crate::planner::IndexType;
use crate::plans::LogicalGet;
use crate::plans::Prewhere;
use crate::plans::RelOperator;
use crate::plans::Scalar;
use crate::plans::VirtualColumnRef;

pub struct VirutalColumnOptimizer {
    virtual_column_map: HashMap<IndexType, Vec<VirtualColumnRef>>,
}

impl VirutalColumnOptimizer {
    pub fn new() -> Self {
        Self {
            virtual_column_map: HashMap::new(),
        }
    }

    fn collect_virtual_columns(&mut self, mut scalars: Vec<&Scalar>) {
        while !scalars.is_empty() {
            let scalar = scalars.pop().unwrap();
            match scalar {
                Scalar::VirtualColumnRef(ref column) => {
                    let table_index = column.table_index;
                    self.virtual_column_map
                        .entry(table_index)
                        .or_insert_with(Vec::new)
                        .push(column.clone());
                }
                Scalar::AndExpr(expr) => {
                    scalars.push(&*expr.left);
                    scalars.push(&*expr.right);
                }
                Scalar::OrExpr(expr) => {
                    scalars.push(&*expr.left);
                    scalars.push(&*expr.right);
                }
                Scalar::ComparisonExpr(expr) => {
                    scalars.push(&*expr.left);
                    scalars.push(&*expr.right);
                }
                Scalar::AggregateFunction(func) => {
                    for arg in func.args.iter() {
                        scalars.push(arg);
                    }
                }
                Scalar::FunctionCall(func) => {
                    for arg in func.arguments.iter() {
                        scalars.push(arg);
                    }
                }
                Scalar::CastExpr(expr) => {
                    scalars.push(&*expr.argument);
                }
                _ => {}
            }
        }
    }

    pub fn optimize(&mut self, expr: &SExpr) -> Result<SExpr> {
        match expr.plan() {
            RelOperator::LogicalGet(p) => {
                let virtual_columns = self.virtual_column_map.remove(&p.table_index);
                if let Some(prewhere) = &p.prewhere {
                    let mut scalars = Vec::with_capacity(prewhere.predicates.len());
                    for scalar in prewhere.predicates.iter() {
                        scalars.push(scalar);
                    }
                    self.collect_virtual_columns(scalars);
                }
                let prewhere_virtual_columns = self.virtual_column_map.remove(&p.table_index);

                if virtual_columns.is_none() && prewhere_virtual_columns.is_none() {
                    return Ok(expr.clone());
                }
                let prewhere = if prewhere_virtual_columns.is_none() {
                    p.prewhere.clone()
                } else {
                    let prewhere = p.prewhere.as_ref().unwrap();
                    Some(Prewhere {
                        output_columns: prewhere.output_columns.clone(),
                        prewhere_columns: prewhere.prewhere_columns.clone(),
                        predicates: prewhere.predicates.clone(),
                        virtual_columns: prewhere_virtual_columns,
                    })
                };

                Ok(SExpr::create_leaf(RelOperator::LogicalGet(LogicalGet {
                    table_index: p.table_index,
                    columns: p.columns.clone(),
                    push_down_predicates: p.push_down_predicates.clone(),
                    limit: p.limit,
                    order_by: p.order_by.clone(),
                    statistics: p.statistics.clone(),
                    prewhere,
                    virtual_columns,
                })))
            }
            RelOperator::LogicalJoin(p) => Ok(SExpr::create_binary(
                RelOperator::LogicalJoin(p.clone()),
                self.optimize(expr.child(0)?)?,
                self.optimize(expr.child(1)?)?,
            )),
            RelOperator::EvalScalar(p) => {
                let mut scalars = Vec::with_capacity(p.items.len());
                for item in p.items.iter() {
                    scalars.push(&item.scalar);
                }
                self.collect_virtual_columns(scalars);
                Ok(SExpr::create_unary(
                    RelOperator::EvalScalar(p.clone()),
                    self.optimize(expr.child(0)?)?,
                ))
            }
            RelOperator::Filter(p) => {
                let mut scalars = Vec::with_capacity(p.predicates.len());
                for scalar in p.predicates.iter() {
                    scalars.push(scalar);
                }
                self.collect_virtual_columns(scalars);

                Ok(SExpr::create_unary(
                    RelOperator::Filter(p.clone()),
                    self.optimize(expr.child(0)?)?,
                ))
            }
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
