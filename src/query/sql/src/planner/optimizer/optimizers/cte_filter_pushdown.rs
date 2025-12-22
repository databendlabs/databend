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

use crate::optimizer::Optimizer;
use crate::optimizer::OptimizerContext;
use crate::optimizer::ir::SExpr;
use crate::optimizer::optimizers::operator::PullUpFilterOptimizer;
use crate::optimizer::optimizers::recursive::RecursiveRuleOptimizer;
use crate::optimizer::optimizers::rule::DEFAULT_REWRITE_RULES;
use crate::plans::BoundColumnRef;
use crate::plans::Filter;
use crate::plans::FunctionCall;
use crate::plans::RelOperator;
use crate::plans::ScalarExpr;
use crate::plans::VisitorMut;

pub struct CTEFilterPushdownOptimizer {
    cte_filters: HashMap<String, Option<Vec<ScalarExpr>>>,
    pull_up_filter_optimizer: PullUpFilterOptimizer,
    rule_optimizer: RecursiveRuleOptimizer,
}

struct ColumnMappingRewriter {
    mapping: HashMap<usize, usize>,
}

impl VisitorMut<'_> for ColumnMappingRewriter {
    fn visit_bound_column_ref(&mut self, col: &mut BoundColumnRef) -> Result<()> {
        if let Some(&new_index) = self.mapping.get(&col.column.index) {
            col.column.index = new_index;
        }
        Ok(())
    }
}

impl CTEFilterPushdownOptimizer {
    pub fn new(ctx: Arc<OptimizerContext>) -> Self {
        let pull_up_filter_optimizer = PullUpFilterOptimizer::new(ctx.clone());
        let inner_optimizer = RecursiveRuleOptimizer::new(ctx.clone(), &DEFAULT_REWRITE_RULES);
        Self {
            cte_filters: HashMap::new(),
            pull_up_filter_optimizer,
            rule_optimizer: inner_optimizer,
        }
    }

    #[recursive::recursive]
    fn collect_filters(&mut self, s_expr: &SExpr) -> Result<()> {
        if let RelOperator::Filter(filter) = s_expr.plan() {
            let child = s_expr.child(0)?;
            if let RelOperator::MaterializedCTERef(cte) = child.plan() {
                let mut and_predicate = if filter.predicates.len() == 1 {
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

                let mut rewriter = ColumnMappingRewriter {
                    mapping: cte.column_mapping.clone(),
                };
                rewriter.visit(&mut and_predicate)?;

                match self.cte_filters.get_mut(&cte.cte_name) {
                    Some(Some(predicates)) => {
                        predicates.push(and_predicate);
                    }
                    Some(None) => {
                        // Already marked as None, do nothing
                    }
                    None => {
                        self.cte_filters
                            .insert(cte.cte_name.clone(), Some(vec![and_predicate]));
                    }
                }
            }
        } else {
            for child in s_expr.children() {
                if let RelOperator::MaterializedCTERef(cte) = child.plan() {
                    self.cte_filters.insert(cte.cte_name.clone(), None);
                }
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
            if let Some(Some(predicates)) = self.cte_filters.get(&cte.cte_name) {
                if !predicates.is_empty() {
                    log::info!("Pushing predicates to CTE {}", cte.cte_name);
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

        if self.cte_filters.iter().all(|(_, v)| v.is_none()) {
            return Ok(s_expr.clone());
        }

        let expr_with_filters = self.add_filters_to_ctes(s_expr)?;

        let expr_with_pulled_up_filters = self
            .pull_up_filter_optimizer
            .optimize(&expr_with_filters)
            .await?;

        self.rule_optimizer
            .optimize(&expr_with_pulled_up_filters)
            .await
    }

    fn name(&self) -> String {
        "CTEFilterPushdownOptimizer".to_string()
    }
}
