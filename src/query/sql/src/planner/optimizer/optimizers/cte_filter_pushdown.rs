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
use std::sync::Arc;

use async_trait::async_trait;
use databend_common_ast::Span;
use databend_common_exception::Result;

use crate::optimizer::ir::SExpr;
use crate::optimizer::optimizers::recursive::RecursiveRuleOptimizer;
use crate::optimizer::optimizers::rule::RuleID;
use crate::optimizer::Optimizer;
use crate::optimizer::OptimizerContext;
use crate::plans::Filter;
use crate::plans::FunctionCall;
use crate::plans::RelOperator;
use crate::plans::ScalarExpr;

static PUSHDOWN_FILTER_RULES: &[RuleID] = &[
    RuleID::PushDownFilterUnion,
    RuleID::PushDownFilterAggregate,
    RuleID::PushDownFilterWindow,
    RuleID::PushDownFilterWindowTopN,
    RuleID::PushDownFilterSort,
    RuleID::PushDownFilterEvalScalar,
    RuleID::PushDownFilterJoin,
    RuleID::PushDownFilterProjectSet,
    RuleID::PushDownFilterScan,
];

#[derive(Clone)]
pub struct CTEFilterPushdownOptimizer {
    cte_filters: HashMap<String, Vec<ScalarExpr>>,
    inner_optimizer: RecursiveRuleOptimizer,
}

impl CTEFilterPushdownOptimizer {
    pub fn new(ctx: Arc<OptimizerContext>) -> Self {
        let inner_optimizer = RecursiveRuleOptimizer::new(ctx.clone(), PUSHDOWN_FILTER_RULES);
        Self {
            cte_filters: HashMap::new(),
            inner_optimizer,
        }
    }

    #[recursive::recursive]
    fn collect_filters(&mut self, s_expr: &SExpr) -> Result<()> {
        if let RelOperator::Filter(filter) = s_expr.plan() {
            let child = s_expr.child(0)?;
            if let RelOperator::MaterializedCTERef(cte) = child.plan() {
                let and_predicate = if filter.predicates.len() == 1 {
                    filter.predicates[0].clone()
                } else {
                    filter.predicates.iter().skip(1).fold(
                        filter.predicates[0].clone(),
                        |acc, pred| {
                            ScalarExpr::FunctionCall(FunctionCall {
                                span: Span::None,
                                func_name: "and".to_string(),
                                params: vec![],
                                arguments: vec![acc, pred.clone()],
                            })
                        },
                    )
                };

                let predicates = self.cte_filters.entry(cte.cte_name.clone()).or_default();
                predicates.push(and_predicate);
            }
        }

        for child in s_expr.children() {
            self.collect_filters(child)?;
        }

        Ok(())
    }

    #[recursive::recursive]
    fn add_filters_to_ctes(&self, s_expr: &SExpr) -> Result<SExpr> {
        let new_children = s_expr
            .children()
            .map(|child| self.add_filters_to_ctes(child))
            .collect::<Result<Vec<_>>>()?;

        let mut result = if new_children
            .iter()
            .zip(s_expr.children())
            .any(|(new, old)| !new.eq(old))
        {
            s_expr.replace_children(new_children.into_iter().map(Arc::new))
        } else {
            s_expr.clone()
        };

        if let RelOperator::MaterializedCTE(cte) = s_expr.plan() {
            if let Some(predicates) = self.cte_filters.get(&cte.cte_name) {
                if !predicates.is_empty() {
                    let or_predicate = if predicates.len() == 1 {
                        predicates[0].clone()
                    } else {
                        predicates
                            .iter()
                            .skip(1)
                            .fold(predicates[0].clone(), |acc, pred| {
                                ScalarExpr::FunctionCall(FunctionCall {
                                    span: Span::None,
                                    func_name: "or".to_string(),
                                    params: vec![],
                                    arguments: vec![acc, pred.clone()],
                                })
                            })
                    };

                    let filter = Filter {
                        predicates: vec![or_predicate],
                    };

                    let filter_expr = SExpr::create_unary(
                        Arc::new(RelOperator::Filter(filter)),
                        Arc::new(result.child(0)?.clone()),
                    );

                    result = result.replace_children(vec![Arc::new(filter_expr)]);
                }
            }
        }

        Ok(result)
    }
}

#[async_trait]
impl Optimizer for CTEFilterPushdownOptimizer {
    async fn optimize(&mut self, s_expr: &SExpr) -> Result<SExpr> {
        self.cte_filters.clear();

        self.collect_filters(s_expr)?;

        let expr_with_filters = self.add_filters_to_ctes(s_expr)?;

        self.inner_optimizer.optimize(&expr_with_filters).await
    }

    fn name(&self) -> String {
        "CTEFilterPushdownOptimizer".to_string()
    }
}
