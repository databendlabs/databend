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

use common_datavalues::BooleanType;
use common_datavalues::DataValue;
use common_exception::Result;

use crate::sql::optimizer::rule::Rule;
use crate::sql::optimizer::RuleID;
use crate::sql::optimizer::SExpr;
use crate::sql::plans::ConstantExpr;
use crate::sql::plans::Filter;
use crate::sql::plans::PatternPlan;
use crate::sql::plans::RelOp;
use crate::sql::plans::Scalar;

fn normalize_predicates(predicates: Vec<Scalar>) -> Vec<Scalar> {
    [remove_true_predicate, normalize_falsy_predicate]
        .into_iter()
        .fold(predicates, |acc, f| f(acc))
}

fn is_true(predicate: &Scalar) -> bool {
    matches!(
        predicate,
        Scalar::ConstantExpr(ConstantExpr {
            value: DataValue::Boolean(true),
            ..
        })
    )
}

fn is_falsy(predicate: &Scalar) -> bool {
    matches!(
        predicate,
        Scalar::ConstantExpr(ConstantExpr {
            value,
            ..
        }) if value == &DataValue::Boolean(false) || value == &DataValue::Null
    )
}

fn remove_true_predicate(predicates: Vec<Scalar>) -> Vec<Scalar> {
    predicates.into_iter().filter(|p| !is_true(p)).collect()
}

fn normalize_falsy_predicate(predicates: Vec<Scalar>) -> Vec<Scalar> {
    if predicates.iter().any(is_falsy) {
        vec![ConstantExpr {
            value: DataValue::Boolean(false),
            data_type: BooleanType::new_impl(),
        }
        .into()]
    } else {
        predicates
    }
}

/// Rule to normalize a Filter, including:
/// - Remove true predicates
/// - If there is a NULL or FALSE conjuction, replace the
/// whole filter with FALSE
pub struct RuleNormalizeScalarFilter {
    id: RuleID,
    pattern: SExpr,
}

impl RuleNormalizeScalarFilter {
    pub fn new() -> Self {
        Self {
            id: RuleID::NormalizeScalarFilter,
            // Filter
            //  \
            //   *
            pattern: SExpr::create_unary(
                PatternPlan {
                    plan_type: RelOp::Filter,
                }
                .into(),
                SExpr::create_leaf(
                    PatternPlan {
                        plan_type: RelOp::Pattern,
                    }
                    .into(),
                ),
            ),
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
        state: &mut crate::sql::optimizer::rule::TransformState,
    ) -> Result<()> {
        let mut filter: Filter = s_expr.plan().clone().try_into()?;

        if filter
            .predicates
            .iter()
            .any(|p| is_true(p) || (is_falsy(p) && filter.predicates.len() > 1))
        {
            filter.predicates = normalize_predicates(filter.predicates);
            state.add_result(SExpr::create_unary(filter.into(), s_expr.child(0)?.clone()));
            Ok(())
        } else {
            Ok(())
        }
    }

    fn pattern(&self) -> &SExpr {
        &self.pattern
    }
}
