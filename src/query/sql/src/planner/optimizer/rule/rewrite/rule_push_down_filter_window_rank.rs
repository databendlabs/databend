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
use databend_common_expression::types::NumberScalar;

use crate::optimizer::extract::Matcher;
use crate::optimizer::rule::Rule;
use crate::optimizer::rule::TransformResult;
use crate::optimizer::RuleID;
use crate::optimizer::SExpr;
use crate::plans::ComparisonOp;
use crate::plans::ConstantExpr;
use crate::plans::Filter;
use crate::plans::RelOp;
use crate::plans::ScalarExpr;
use crate::plans::Sort;
use crate::plans::Window;
use crate::plans::WindowFuncType;

pub struct RulePushDownFilterWindowRank {
    id: RuleID,
    matchers: Vec<Matcher>,
}

impl RulePushDownFilterWindowRank {
    pub fn new() -> Self {
        Self {
            id: RuleID::PushDownFilterWindowRank,
            matchers: vec![Matcher::MatchOp {
                op_type: RelOp::Filter,
                children: vec![Matcher::MatchOp {
                    op_type: RelOp::Window,
                    children: vec![Matcher::MatchOp {
                        op_type: RelOp::Sort,
                        children: vec![Matcher::Leaf],
                    }],
                }],
            }],
        }
    }
}

impl Rule for RulePushDownFilterWindowRank {
    fn id(&self) -> RuleID {
        self.id
    }

    fn apply(&self, s_expr: &SExpr, state: &mut TransformResult) -> Result<()> {
        let filter: Filter = s_expr.plan().clone().try_into()?;
        let window_expr = s_expr.child(0)?;
        let window: Window = window_expr.plan().clone().try_into()?;
        let sort_expr = window_expr.child(0)?;
        let mut sort: Sort = sort_expr.plan().clone().try_into()?;

        if !matches!(window.function, WindowFuncType::Rank) {
            return Ok(());
        }

        let predicates = filter
            .predicates
            .into_iter()
            .filter_map(|predicate| extract_top_n(window.index, predicate))
            .collect::<Vec<_>>();

        let Some(top_n) = predicates.into_iter().min() else {
            return Ok(());
        };

        if top_n == 0 {
            // TODO
            return Ok(());
        }

        sort.window_top_n = Some(top_n);

        let mut result = SExpr::create_unary(
            s_expr.plan.clone(),
            SExpr::create_unary(
                window_expr.plan.clone(),
                sort_expr.replace_plan(Arc::new(sort.into())).into(),
            )
            .into(),
        );

        result.set_applied_rule(&self.id);

        state.add_result(result);

        Ok(())
    }

    fn matchers(&self) -> &[Matcher] {
        &self.matchers
    }
}

fn extract_top_n(column: usize, predicate: ScalarExpr) -> Option<usize> {
    let ScalarExpr::FunctionCall(call) = predicate else {
        return None;
    };

    let func_name = &call.func_name;
    if func_name == ComparisonOp::Equal.to_func_name() {
        match (&call.arguments[0], &call.arguments[1]) {
            (ScalarExpr::BoundColumnRef(col), ScalarExpr::ConstantExpr(cnst))
            | (ScalarExpr::ConstantExpr(cnst), ScalarExpr::BoundColumnRef(col)) => {
                return if col.column.index != column {
                    None
                } else {
                    extract_i64(cnst).map(|n| n.max(0) as usize)
                };
            }
            _ => return None,
        }
    }

    let (left, right) = match (
        func_name == ComparisonOp::LTE.to_func_name()
            || func_name == ComparisonOp::LT.to_func_name(),
        func_name == ComparisonOp::GTE.to_func_name()
            || func_name == ComparisonOp::GT.to_func_name(),
    ) {
        (true, _) => (&call.arguments[0], &call.arguments[1]),
        (_, true) => (&call.arguments[1], &call.arguments[0]),
        _ => return None,
    };

    let eq = func_name == ComparisonOp::GTE.to_func_name()
        || func_name == ComparisonOp::LTE.to_func_name();

    match (left, right) {
        (ScalarExpr::BoundColumnRef(col), ScalarExpr::ConstantExpr(cnst)) => {
            if col.column.index != column {
                None
            } else {
                extract_i64(cnst).map(|n| {
                    if eq {
                        n.max(0) as usize
                    } else {
                        n.max(1) as usize - 1
                    }
                })
            }
        }
        _ => None,
    }
}

fn extract_i64(expr: &ConstantExpr) -> Option<i64> {
    expr.value.as_number().and_then(|&n| match n {
        NumberScalar::UInt8(n) => Some(n as i64),
        NumberScalar::UInt16(n) => Some(n as i64),
        NumberScalar::UInt32(n) => Some(n as i64),
        NumberScalar::UInt64(n) if n <= i64::MAX as u64 => Some(n as i64),
        NumberScalar::Int8(n) => Some(n as i64),
        NumberScalar::Int16(n) => Some(n as i64),
        NumberScalar::Int32(n) => Some(n as i64),
        NumberScalar::Int64(n) => Some(n as i64),
        _ => None,
    })
}
