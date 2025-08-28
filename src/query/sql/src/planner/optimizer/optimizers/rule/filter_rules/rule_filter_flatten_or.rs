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

use databend_common_exception::Result;

use crate::optimizer::ir::Matcher;
use crate::optimizer::ir::SExpr;
use crate::optimizer::optimizers::rule::Rule;
use crate::optimizer::optimizers::rule::RuleID;
use crate::optimizer::optimizers::rule::TransformResult;
use crate::plans::Filter;
use crate::plans::FunctionCall;
use crate::plans::RelOp;
use crate::plans::ScalarExpr;

pub struct RuleFilterFlattenOr {
    id: RuleID,
    matchers: Vec<Matcher>,
}

impl RuleFilterFlattenOr {
    pub fn new() -> Self {
        Self {
            id: RuleID::FilterFlattenOr,
            // Filter
            //  \
            //   *
            matchers: vec![Matcher::MatchOp {
                op_type: RelOp::Filter,
                children: vec![Matcher::Leaf],
            }],
        }
    }
}

impl Default for RuleFilterFlattenOr {
    fn default() -> Self {
        Self::new()
    }
}

impl Rule for RuleFilterFlattenOr {
    fn id(&self) -> RuleID {
        self.id
    }

    fn apply(&self, s_expr: &SExpr, state: &mut TransformResult) -> Result<()> {
        let mut filter: Filter = s_expr.plan().clone().try_into()?;
        let mut has_replace = false;

        for predicate in filter.predicates.iter_mut() {
            let mut or_exprs = Vec::new();
            flatten_or_expr(predicate, &mut or_exprs);

            if or_exprs.len() > 2 {
                *predicate = FunctionCall {
                    span: None,
                    func_name: "or_filters".to_string(),
                    params: vec![],
                    arguments: or_exprs,
                }
                .into();
                has_replace = true
            }
        }
        if !has_replace {
            state.add_result(s_expr.clone());
            return Ok(());
        }
        let mut res =
            SExpr::create_unary(Arc::new(filter.into()), Arc::new(s_expr.child(0)?.clone()));
        res.set_applied_rule(&self.id());
        state.add_result(res);

        Ok(())
    }

    fn matchers(&self) -> &[Matcher] {
        &self.matchers
    }
}

fn flatten_or_expr(expr: &ScalarExpr, or_exprs: &mut Vec<ScalarExpr>) {
    match expr {
        ScalarExpr::FunctionCall(func) if func.func_name == "or" => {
            for argument in func.arguments.iter() {
                flatten_or_expr(argument, or_exprs);
            }
        }
        _ => or_exprs.push(expr.clone()),
    }
}
