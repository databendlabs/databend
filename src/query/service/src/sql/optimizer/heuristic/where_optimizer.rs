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

use common_exception::Result;

use crate::sql::optimizer::ColumnSet;
use crate::sql::optimizer::SExpr;
use crate::sql::plans::Filter;
use crate::sql::plans::LogicalGet;
use crate::sql::plans::PatternPlan;
use crate::sql::plans::Prewhere;
use crate::sql::plans::RelOp;
use crate::sql::plans::Scalar;
use crate::sql::MetadataRef;

pub struct WhereOptimizer {
    metadata: MetadataRef,
    pattern: SExpr,
}

impl WhereOptimizer {
    pub fn new(metadata: MetadataRef) -> Self {
        Self {
            metadata,
            pattern: SExpr::create_unary(
                PatternPlan {
                    plan_type: RelOp::Filter,
                }
                .into(),
                SExpr::create_leaf(
                    PatternPlan {
                        plan_type: RelOp::LogicalGet,
                    }
                    .into(),
                ),
            ),
        }
    }

    fn collect_columns(expr: &Scalar, columns: &mut ColumnSet) {
        match expr {
            Scalar::BoundColumnRef(column) => {
                columns.insert(column.column.index);
            }
            Scalar::AndExpr(and) => {
                Self::collect_columns(and.left.as_ref(), columns);
                Self::collect_columns(and.right.as_ref(), columns);
            }
            Scalar::OrExpr(or) => {
                Self::collect_columns(or.left.as_ref(), columns);
                Self::collect_columns(or.right.as_ref(), columns);
            }
            Scalar::ComparisonExpr(cmp) => {
                Self::collect_columns(cmp.left.as_ref(), columns);
                Self::collect_columns(cmp.right.as_ref(), columns);
            }
            Scalar::FunctionCall(func) => {
                for arg in func.arguments.iter() {
                    Self::collect_columns(arg, columns);
                }
            }
            Scalar::CastExpr(cast) => {
                Self::collect_columns(cast.argument.as_ref(), columns);
            }
            // 1. ConstantExpr is not collected.
            // 2. SubqueryExpr is not collected.
            // 3. AggregateFunction will not appear in where clause.
            _ => {}
        }
    }

    // analyze if the expression can be moved to prewhere
    fn analyze(expr: &Scalar, columns_to_scan: usize) -> (bool, ColumnSet) {
        let mut columns = ColumnSet::new();

        // columns in subqueries are not considered
        Self::collect_columns(expr, &mut columns);

        // viable conditions:
        // 1. Condition depend on some column. Constant expressions are not moved.
        // 2. Do not move conditions involving all queried columns.
        // 3. Only current table columns are considered. (This condition is always true in current Pattern (Filter -> LogicalGet)).
        (
            !columns.is_empty() && columns.len() < columns_to_scan,
            columns,
        )
    }

    pub fn where_optimize(&self, s_expr: SExpr) -> Result<SExpr> {
        let rel_op = s_expr.plan();
        if s_expr.match_pattern(&self.pattern) {
            let mut filter: Filter = s_expr.plan().clone().try_into()?;
            let mut get: LogicalGet = s_expr.child(0)?.plan().clone().try_into()?;
            let metadata = self.metadata.read().clone();

            let table = metadata.table(get.table_index).table.clone();
            if !table.support_prewhere() {
                // cannot optimize
                return Ok(s_expr);
            }

            let mut prewhere_columns = ColumnSet::new();
            let mut prewhere_pred = Vec::new();
            let mut remain_pred = Vec::new();

            let columns_to_scan = get.columns.len();

            // filter.predicates are already splited by AND
            for pred in filter.predicates.iter() {
                let (viable, columns) = Self::analyze(pred, columns_to_scan);
                if viable {
                    prewhere_pred.push(pred.clone());
                    prewhere_columns.extend(&columns);
                } else {
                    remain_pred.push(pred.clone());
                }
            }

            get.prewhere = if prewhere_pred.is_empty() {
                None
            } else {
                Some(Prewhere {
                    columns: prewhere_columns,
                    predicates: prewhere_pred,
                })
            };

            if !remain_pred.is_empty() {
                filter.predicates = remain_pred;
                Ok(SExpr::create_unary(
                    filter.into(),
                    SExpr::create_leaf(get.into()),
                ))
            } else {
                Ok(SExpr::create_leaf(get.into()))
            }
        } else {
            let children = s_expr
                .children()
                .iter()
                .map(|expr| self.where_optimize(expr.clone()))
                .collect::<Result<Vec<_>>>()?;
            Ok(SExpr::create(rel_op.clone(), children, None))
        }
    }
}
