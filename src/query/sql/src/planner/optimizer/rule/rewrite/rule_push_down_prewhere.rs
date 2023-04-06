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

use crate::optimizer::rule::Rule;
use crate::optimizer::ColumnSet;
use crate::optimizer::RuleID;
use crate::optimizer::SExpr;
use crate::plans::Filter;
use crate::plans::PatternPlan;
use crate::plans::Prewhere;
use crate::plans::RelOp;
use crate::plans::ScalarExpr;
use crate::plans::Scan;
use crate::MetadataRef;

pub struct RulePushDownPrewhere {
    id: RuleID,
    metadata: MetadataRef,
    patterns: Vec<SExpr>,
}

impl RulePushDownPrewhere {
    pub fn new(metadata: MetadataRef) -> Self {
        Self {
            id: RuleID::PushDownPrewhere,
            metadata,
            patterns: vec![SExpr::create_unary(
                PatternPlan {
                    plan_type: RelOp::Filter,
                }
                .into(),
                SExpr::create_leaf(
                    PatternPlan {
                        plan_type: RelOp::Scan,
                    }
                    .into(),
                ),
            )],
        }
    }

    /// will throw error if the bound column ref is not in the table, such as subquery
    fn collect_columns_impl(expr: &ScalarExpr, columns: &mut ColumnSet) -> Option<()> {
        match expr {
            ScalarExpr::BoundColumnRef(column) => {
                column.column.table_name.as_ref()?;
                columns.insert(column.column.index);
                Some(())
            }
            ScalarExpr::AndExpr(and) => {
                Self::collect_columns_impl(and.left.as_ref(), columns)?;
                Self::collect_columns_impl(and.right.as_ref(), columns)
            }
            ScalarExpr::OrExpr(or) => {
                Self::collect_columns_impl(or.left.as_ref(), columns)?;
                Self::collect_columns_impl(or.right.as_ref(), columns)
            }
            ScalarExpr::NotExpr(not) => Self::collect_columns_impl(not.argument.as_ref(), columns),
            ScalarExpr::ComparisonExpr(cmp) => {
                Self::collect_columns_impl(cmp.left.as_ref(), columns)?;
                Self::collect_columns_impl(cmp.right.as_ref(), columns)
            }
            ScalarExpr::FunctionCall(func) => {
                for arg in func.arguments.iter() {
                    Self::collect_columns_impl(arg, columns)?;
                }
                Some(())
            }
            ScalarExpr::CastExpr(cast) => {
                Self::collect_columns_impl(cast.argument.as_ref(), columns)
            }
            // 1. ConstantExpr is not collected.
            // 2. SubqueryExpr and AggregateFunction will not appear in Filter-LogicalGet
            _ => None,
        }
    }

    // analyze if the expression can be moved to prewhere
    fn collect_columns(expr: &ScalarExpr) -> Option<ColumnSet> {
        let mut columns = ColumnSet::new();
        // columns in subqueries are not considered
        let _ = Self::collect_columns_impl(expr, &mut columns);
        if !columns.is_empty() {
            return Some(columns);
        }
        None
    }

    pub fn prewhere_optimize(&self, s_expr: &SExpr) -> Result<SExpr> {
        let filter: Filter = s_expr.plan().clone().try_into()?;
        let mut get: Scan = s_expr.child(0)?.plan().clone().try_into()?;
        let metadata = self.metadata.read().clone();

        let table = metadata.table(get.table_index).table();
        if !table.support_prewhere() {
            // cannot optimize
            return Ok(s_expr.clone());
        }

        let mut prewhere_columns = ColumnSet::new();
        let mut prewhere_pred = Vec::new();

        // filter.predicates are already split by AND
        for pred in filter.predicates.iter() {
            match Self::collect_columns(pred) {
                Some(columns) => {
                    prewhere_pred.push(pred.clone());
                    prewhere_columns.extend(&columns);
                }
                None => return Ok(s_expr.clone()),
            }
        }

        if !prewhere_pred.is_empty() {
            if let Some(prewhere) = get.prewhere.as_ref() {
                prewhere_pred.extend(prewhere.predicates.clone());
                prewhere_columns.extend(&prewhere.prewhere_columns);
            }

            get.prewhere = Some(Prewhere {
                output_columns: get.columns.clone(),
                prewhere_columns,
                predicates: prewhere_pred,
            });
        }
        Ok(SExpr::create_leaf(get.into()))
    }
}

impl Rule for RulePushDownPrewhere {
    fn id(&self) -> RuleID {
        self.id
    }

    fn patterns(&self) -> &Vec<SExpr> {
        &self.patterns
    }

    fn apply(
        &self,
        s_expr: &SExpr,
        state: &mut crate::optimizer::rule::TransformResult,
    ) -> Result<()> {
        let mut result = self.prewhere_optimize(s_expr)?;
        result.set_applied_rule(&self.id);
        state.add_result(result);
        Ok(())
    }
}
