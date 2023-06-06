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

use ahash::HashMap;
use common_catalog::plan::VectorSimilarityInfo;
use common_exception::ErrorCode;
use common_exception::Result;
use common_vector::index::IndexName;
use common_vector::index::MetricType;
use common_vector::index::VectorIndex;

use crate::optimizer::rule::Rule;
use crate::optimizer::RuleID;
use crate::optimizer::SExpr;
use crate::plans::FunctionCall;
use crate::plans::PatternPlan;
use crate::plans::RelOp;
use crate::MetadataRef;
use crate::ScalarExpr;

pub struct RuleTryApplyVectorIndex {
    id: RuleID,
    patterns: Vec<SExpr>,
    metadata: MetadataRef,
}

impl RuleTryApplyVectorIndex {
    pub fn new(metadata: MetadataRef) -> Self {
        Self {
            metadata,
            id: RuleID::TryApplyVectorIndex,
            // Input:
            //   Limit
            //     \
            //    Sort
            //      \
            //      EvalScalar
            //        \
            //         Scan
            //
            // Output:
            //   Scan(order_by, limit, similarity)
            patterns: vec![SExpr::create_unary(
                Arc::new(
                    PatternPlan {
                        plan_type: RelOp::Limit,
                    }
                    .into(),
                ),
                Arc::new(SExpr::create_unary(
                    Arc::new(
                        PatternPlan {
                            plan_type: RelOp::Sort,
                        }
                        .into(),
                    ),
                    Arc::new(SExpr::create_unary(
                        Arc::new(
                            PatternPlan {
                                plan_type: RelOp::EvalScalar,
                            }
                            .into(),
                        ),
                        Arc::new(SExpr::create_leaf(Arc::new(
                            PatternPlan {
                                plan_type: RelOp::Scan,
                            }
                            .into(),
                        ))),
                    )),
                )),
            )],
        }
    }
}

impl Rule for RuleTryApplyVectorIndex {
    fn id(&self) -> crate::optimizer::RuleID {
        self.id
    }

    fn apply(
        &self,
        s_expr: &crate::optimizer::SExpr,
        state: &mut crate::optimizer::rule::TransformResult,
    ) -> common_exception::Result<()> {
        let limit = s_expr.plan().as_limit().unwrap();
        let sort = s_expr.walk_down(1).plan().as_sort().unwrap();
        let eval_scalar = s_expr.walk_down(2).plan().as_eval_scalar().unwrap();

        let meta = self.metadata.read();
        let vector_indexes = meta.vector_indexes();

        if limit.offset != 0 || limit.limit.is_none() || sort.items.len() != 1 || !sort.items[0].asc
        {
            state.add_result(s_expr.clone());
            return Ok(());
        }

        let sort_by_idx = eval_scalar
            .items
            .iter()
            .position(|item| item.index == sort.items[0].index)
            .unwrap();
        let sort_by_item = &eval_scalar.items[sort_by_idx];
        match &sort_by_item.scalar {
            ScalarExpr::FunctionCall(func) => {
                let similarity = parse_similarity_func(func, vector_indexes)?;
                if similarity.is_none() {
                    state.add_result(s_expr.clone());
                    return Ok(());
                }
                let mut new_scan = s_expr.walk_down(3).clone();
                let mut new_operator = new_scan.plan.as_ref().clone();
                new_operator.as_scan_mut().unwrap().order_by = Some(sort.items.clone());
                new_operator.as_scan_mut().unwrap().limit = Some(limit.limit.unwrap());
                new_operator.as_scan_mut().unwrap().similarity =
                    Some(Box::new(similarity.unwrap()));
                new_scan.plan = Arc::new(new_operator);
                new_scan.set_applied_rule(&self.id);
                state.add_result(new_scan);
            }
            _ => state.add_result(s_expr.clone()),
        }
        Ok(())
    }

    fn patterns(&self) -> &Vec<crate::optimizer::SExpr> {
        &self.patterns
    }
}

fn parse_similarity_func(
    func: &FunctionCall,
    vector_indexes: &HashMap<String, VectorIndex>,
) -> Result<Option<VectorSimilarityInfo>> {
    let metric = match func.func_name.to_ascii_lowercase().as_str() {
        "cosine_distance" => Ok(MetricType::Cosine),
        _ => Err(ErrorCode::BadArguments(format!(
            "invalid similarity function: {}",
            func.func_name
        ))),
    }?;
    if func.arguments.len() != 2 {
        return Err(ErrorCode::BadArguments(format!(
            "invalid arguments for similarity function: {}",
            func.func_name
        )));
    }
    let (column_idx, column_name) = match &func.arguments[0] {
        ScalarExpr::BoundColumnRef(expr) => {
            Ok((expr.column.index, expr.column.column_name.clone()))
        }
        _ => Err(ErrorCode::BadArguments(format!(
            "invalid arguments for similarity function: {}",
            func.func_name
        ))),
    }?;
    let index_name_postfix = IndexName::create_postfix(column_name.as_str(), &metric);
    let index = vector_indexes.get(index_name_postfix.as_str());
    if index.is_none() {
        return Ok(None);
    }
    let index = index.unwrap();
    let target = match &func.arguments[1] {
        ScalarExpr::ConstantExpr(expr) => Ok(expr.value.clone()),
        _ => Err(ErrorCode::BadArguments(format!(
            "invalid arguments for similarity function: {}",
            func.func_name
        ))),
    }?;
    Ok(Some(VectorSimilarityInfo {
        vector_index: index.clone(),
        metric,
        column: column_idx,
        target,
    }))
}
