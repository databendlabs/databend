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

use common_exception::Result;

use crate::optimizer::rule::Rule;
use crate::optimizer::ColumnSet;
use crate::optimizer::RuleID;
use crate::optimizer::SExpr;
use crate::plans::Filter;
use crate::plans::PatternPlan;
use crate::plans::Prewhere;
use crate::plans::RelOp;
use crate::plans::Scan;
use crate::MetadataRef;

pub struct RulePushDownPrewhere {
    id: RuleID,
    patterns: Vec<SExpr>,
    metadata: MetadataRef,
}

impl RulePushDownPrewhere {
    pub fn new(metadata: MetadataRef) -> Self {
        Self {
            id: RuleID::PushDownPrewhere,
            patterns: vec![SExpr::create_unary(
                Arc::new(
                    PatternPlan {
                        plan_type: RelOp::Filter,
                    }
                    .into(),
                ),
                Arc::new(SExpr::create_leaf(Arc::new(
                    PatternPlan {
                        plan_type: RelOp::Scan,
                    }
                    .into(),
                ))),
            )],
            metadata,
        }
    }

    pub fn prewhere_optimize(&self, s_expr: &SExpr) -> Result<SExpr> {
        let mut get: Scan = s_expr.child(0)?.plan().clone().try_into()?;
        let metadata = self.metadata.read().clone();

        let table = metadata.table(get.table_index).table();
        if !table.support_prewhere() {
            // cannot optimize
            return Ok(s_expr.clone());
        }
        let filter: Filter = s_expr.plan().clone().try_into()?;

        let mut prewhere_columns = ColumnSet::new();
        let mut prewhere_pred = Vec::new();

        for pred in filter.predicates.iter() {
            let columns = pred.used_columns();
            prewhere_columns.extend(columns);
            prewhere_pred.push(pred.clone());
        }

        // As the matched pattern is Filter-Scan,
        // we can guarantee that the columns in `filter.predicates` are all in scan.columns
        debug_assert!(get.columns.is_superset(&prewhere_columns));

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
        Ok(SExpr::create_leaf(Arc::new(get.into())))
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
