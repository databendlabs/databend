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
use databend_common_expression::Scalar;

use crate::optimizer::extract::Matcher;
use crate::optimizer::rule::constant::is_falsy;
use crate::optimizer::rule::constant::is_true;
use crate::optimizer::rule::Rule;
use crate::optimizer::RuleID;
use crate::optimizer::SExpr;
use crate::plans::ConstantExpr;
use crate::plans::Filter;
use crate::plans::RelOp;
use crate::plans::ScalarExpr;

fn normalize_predicates(predicates: Vec<ScalarExpr>) -> Vec<ScalarExpr> {
    [remove_true_predicate, normalize_falsy_predicate]
        .into_iter()
        .fold(predicates, |acc, f| f(acc))
}

fn remove_true_predicate(predicates: Vec<ScalarExpr>) -> Vec<ScalarExpr> {
    predicates.into_iter().filter(|p| !is_true(p)).collect()
}

fn normalize_falsy_predicate(predicates: Vec<ScalarExpr>) -> Vec<ScalarExpr> {
    if predicates.iter().any(is_falsy) {
        vec![
            ConstantExpr {
                span: None,
                value: Scalar::Boolean(false),
            }
            .into(),
        ]
    } else {
        predicates
    }
}

/// Rule to normalize a Filter, including:
/// - Remove true predicates
/// - If there is a NULL or FALSE conjunction, replace the
///   whole filter with FALSE
pub struct RuleNormalizeScalarFilter {
    id: RuleID,
    matchers: Vec<Matcher>,
}

impl RuleNormalizeScalarFilter {
    pub fn new() -> Self {
        Self {
            id: RuleID::NormalizeScalarFilter,
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

impl Rule for RuleNormalizeScalarFilter {
    fn id(&self) -> RuleID {
        self.id
    }

    fn apply(
        &self,
        s_expr: &SExpr,
        state: &mut crate::optimizer::rule::TransformResult,
    ) -> Result<()> {
        let mut filter: Filter = s_expr.plan().clone().try_into()?;

        if filter
            .predicates
            .iter()
            .any(|p| is_true(p) || (is_falsy(p) && filter.predicates.len() > 1))
        {
            filter.predicates = normalize_predicates(filter.predicates);
            state.add_result(SExpr::create_unary(
                Arc::new(filter.into()),
                Arc::new(s_expr.child(0)?.clone()),
            ));
            Ok(())
        } else {
            Ok(())
        }
    }

    fn matchers(&self) -> &[Matcher] {
        &self.matchers
    }
}
