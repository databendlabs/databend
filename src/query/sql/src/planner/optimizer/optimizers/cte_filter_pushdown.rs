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

use crate::Symbol;
use crate::optimizer::Optimizer;
use crate::optimizer::OptimizerContext;
use crate::optimizer::ir::SExpr;
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
    rule_optimizer: RecursiveRuleOptimizer,
}

struct ColumnMappingRewriter {
    mapping: HashMap<Symbol, Symbol>,
}

#[derive(Default)]
struct PredicateDedupNormalizer;

impl VisitorMut<'_> for PredicateDedupNormalizer {
    fn visit_bound_column_ref(&mut self, col: &mut BoundColumnRef) -> Result<()> {
        col.column.table_index = None;
        Ok(())
    }
}

fn dedup_normalized_predicate(predicate: &ScalarExpr) -> Result<ScalarExpr> {
    let mut normalized = predicate.clone();
    let mut normalizer = PredicateDedupNormalizer;
    normalizer.visit(&mut normalized)?;
    Ok(normalized)
}

fn dedup_append_predicate(predicates: &mut Vec<ScalarExpr>, predicate: ScalarExpr) -> Result<()> {
    let normalized = dedup_normalized_predicate(&predicate)?;
    let mut exists = false;
    for current in predicates.iter() {
        if dedup_normalized_predicate(current)? == normalized {
            exists = true;
            break;
        }
    }

    if !exists {
        predicates.push(predicate);
    }

    Ok(())
}

fn flatten_conjuncts(predicate: &ScalarExpr) -> Vec<ScalarExpr> {
    match predicate {
        ScalarExpr::FunctionCall(func)
            if matches!(func.func_name.as_str(), "and" | "and_filters") =>
        {
            func.arguments.iter().flat_map(flatten_conjuncts).collect()
        }
        _ => vec![predicate.clone()],
    }
}

fn build_conjunction(predicates: Vec<ScalarExpr>) -> Option<ScalarExpr> {
    predicates.into_iter().reduce(|acc, predicate| {
        ScalarExpr::FunctionCall(FunctionCall {
            span: Span::None,
            func_name: "and".to_string(),
            params: vec![],
            arguments: vec![acc, predicate],
        })
    })
}

fn build_disjunction(predicates: Vec<ScalarExpr>) -> Option<ScalarExpr> {
    predicates.into_iter().reduce(|acc, predicate| {
        ScalarExpr::FunctionCall(FunctionCall {
            span: Span::None,
            func_name: "or".to_string(),
            params: vec![],
            arguments: vec![acc, predicate],
        })
    })
}

fn extract_common_conjuncts(predicates: &[ScalarExpr]) -> Result<Vec<ScalarExpr>> {
    if predicates.is_empty() {
        return Ok(vec![]);
    }

    let predicate_conjuncts = predicates.iter().map(flatten_conjuncts).collect::<Vec<_>>();
    let normalized_conjuncts = predicate_conjuncts
        .iter()
        .map(|conjuncts| {
            conjuncts
                .iter()
                .map(dedup_normalized_predicate)
                .collect::<Result<Vec<_>>>()
        })
        .collect::<Result<Vec<_>>>()?;

    let mut common_predicates = Vec::new();
    let mut common_normalized = Vec::new();
    for (predicate, normalized) in predicate_conjuncts[0]
        .iter()
        .zip(normalized_conjuncts[0].iter())
    {
        if common_normalized
            .iter()
            .any(|current| current == normalized)
        {
            continue;
        }

        if normalized_conjuncts
            .iter()
            .skip(1)
            .all(|conjuncts| conjuncts.iter().any(|current| current == normalized))
        {
            common_predicates.push(predicate.clone());
            common_normalized.push(normalized.clone());
        }
    }

    let mut residual_predicates = Vec::new();
    let mut has_empty_residual = false;
    for (conjuncts, normalized) in predicate_conjuncts.iter().zip(normalized_conjuncts.iter()) {
        let residual_conjuncts = conjuncts
            .iter()
            .zip(normalized.iter())
            .filter_map(|(predicate, normalized)| {
                (!common_normalized
                    .iter()
                    .any(|current| current == normalized))
                .then_some(predicate.clone())
            })
            .collect::<Vec<_>>();

        if residual_conjuncts.is_empty() {
            has_empty_residual = true;
            continue;
        }

        if let Some(residual_predicate) = build_conjunction(residual_conjuncts) {
            dedup_append_predicate(&mut residual_predicates, residual_predicate)?;
        }
    }

    if !has_empty_residual {
        if let Some(residual_predicate) = build_disjunction(residual_predicates) {
            common_predicates.push(residual_predicate);
        }
    }

    Ok(common_predicates)
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
        let inner_optimizer = RecursiveRuleOptimizer::new(ctx.clone(), &DEFAULT_REWRITE_RULES);
        Self {
            cte_filters: HashMap::new(),
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
                        dedup_append_predicate(predicates, and_predicate)?;
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
                let pushdown_predicates = extract_common_conjuncts(predicates)?;
                if !pushdown_predicates.is_empty() {
                    log::info!("Pushing predicates to CTE {}", cte.cte_name);
                    let filter = Filter {
                        predicates: pushdown_predicates,
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

        self.rule_optimizer.optimize(&expr_with_filters).await
    }

    fn name(&self) -> String {
        "CTEFilterPushdownOptimizer".to_string()
    }
}

#[cfg(test)]
mod tests {
    use databend_common_expression::Scalar;
    use databend_common_expression::types::DataType;

    use super::*;
    use crate::Symbol;
    use crate::Visibility;
    use crate::binder::ColumnBindingBuilder;
    use crate::plans::BoundColumnRef;
    use crate::plans::ConstantExpr;

    fn bound_column(index: usize, table_index: usize, name: &str) -> ScalarExpr {
        BoundColumnRef {
            span: None,
            column: ColumnBindingBuilder::new(
                name.to_string(),
                Symbol::new(index),
                Box::new(DataType::String),
                Visibility::Visible,
            )
            .table_index(Some(table_index))
            .build(),
        }
        .into()
    }

    fn eq_string(index: usize, table_index: usize, name: &str, value: &str) -> ScalarExpr {
        ScalarExpr::FunctionCall(FunctionCall {
            span: Span::None,
            func_name: "eq".to_string(),
            params: vec![],
            arguments: vec![
                bound_column(index, table_index, name),
                ConstantExpr {
                    span: None,
                    value: Scalar::String(value.to_string()),
                }
                .into(),
            ],
        })
    }

    fn eq_columns(
        left_index: usize,
        left_table_index: usize,
        left_name: &str,
        right_index: usize,
        right_table_index: usize,
        right_name: &str,
    ) -> ScalarExpr {
        ScalarExpr::FunctionCall(FunctionCall {
            span: Span::None,
            func_name: "eq".to_string(),
            params: vec![],
            arguments: vec![
                bound_column(left_index, left_table_index, left_name),
                bound_column(right_index, right_table_index, right_name),
            ],
        })
    }

    fn and(arguments: Vec<ScalarExpr>) -> ScalarExpr {
        arguments
            .into_iter()
            .reduce(|acc, arg| {
                ScalarExpr::FunctionCall(FunctionCall {
                    span: Span::None,
                    func_name: "and".to_string(),
                    params: vec![],
                    arguments: vec![acc, arg],
                })
            })
            .unwrap()
    }

    fn or(arguments: Vec<ScalarExpr>) -> ScalarExpr {
        arguments
            .into_iter()
            .reduce(|acc, arg| {
                ScalarExpr::FunctionCall(FunctionCall {
                    span: Span::None,
                    func_name: "or".to_string(),
                    params: vec![],
                    arguments: vec![acc, arg],
                })
            })
            .unwrap()
    }

    #[test]
    fn test_dedup_append_predicate_preserves_first_occurrence() {
        let predicate = eq_string(10, 1, "s_store_name", "ese");
        let mut predicates = vec![predicate.clone()];

        dedup_append_predicate(&mut predicates, predicate.clone()).unwrap();
        dedup_append_predicate(&mut predicates, eq_string(11, 1, "s_city", "beijing")).unwrap();
        dedup_append_predicate(&mut predicates, predicate).unwrap();

        assert_eq!(predicates.len(), 2);
        assert_eq!(predicates[0], eq_string(10, 1, "s_store_name", "ese"));
        assert_eq!(predicates[1], eq_string(11, 1, "s_city", "beijing"));
    }

    #[test]
    fn test_dedup_append_predicate_ignores_table_index_noise() {
        let mut predicates = vec![eq_string(10, 1, "s_store_name", "ese")];

        dedup_append_predicate(&mut predicates, eq_string(10, 2, "s_store_name", "ese")).unwrap();

        assert_eq!(predicates.len(), 1);
    }

    #[test]
    fn test_extract_common_conjuncts_keeps_shared_predicates_outside_or() {
        let join_predicate = eq_columns(0, 1, "ss_store_sk", 1, 2, "s_store_sk");
        let store_predicate = eq_string(2, 2, "s_store_name", "ese");
        let first_window = eq_string(3, 3, "t_hour", "8");
        let second_window = eq_string(3, 3, "t_hour", "9");

        let result = extract_common_conjuncts(&[
            and(vec![
                join_predicate.clone(),
                store_predicate.clone(),
                first_window.clone(),
            ]),
            and(vec![
                join_predicate.clone(),
                store_predicate.clone(),
                second_window.clone(),
            ]),
        ])
        .unwrap();

        assert_eq!(result, vec![
            join_predicate,
            store_predicate,
            or(vec![first_window, second_window]),
        ]);
    }

    #[test]
    fn test_extract_common_conjuncts_drops_residual_or_when_branch_is_subset() {
        let join_predicate = eq_columns(0, 1, "ss_store_sk", 1, 2, "s_store_sk");
        let store_predicate = eq_string(2, 2, "s_store_name", "ese");
        let window_predicate = eq_string(3, 3, "t_hour", "8");

        let result = extract_common_conjuncts(&[
            and(vec![join_predicate.clone(), store_predicate.clone()]),
            and(vec![
                join_predicate.clone(),
                store_predicate.clone(),
                window_predicate,
            ]),
        ])
        .unwrap();

        assert_eq!(result, vec![join_predicate, store_predicate]);
    }
}
