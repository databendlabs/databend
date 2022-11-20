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
use common_datavalues::DataTypeImpl;
use common_exception::Result;

use crate::optimizer::util::try_push_down_filter_join;
use crate::optimizer::ColumnSet;
use crate::optimizer::RuleID;
use crate::optimizer::SExpr;
use crate::plans::AndExpr;
use crate::plans::Filter;
use crate::plans::LogicalGet;
use crate::plans::OrExpr;
use crate::plans::PatternPlan;
use crate::plans::Prewhere;
use crate::plans::RelOp;
use crate::plans::Scalar;
use crate::IndexType;
use crate::MetadataRef;
use crate::ScalarExpr;

/// Extract or predicates from Filter to push down them to join.
/// For example: `select * from t1, t2 where (t1.a=1 and t2.b=2) or (t1.a=2 and t2.b=1)`
/// The predicate will be rewritten to `((t1.a=1 and t2.b=2) or (t1.a=2 and t2.b=1)) and (t1.a=1 or t1.a=2) and (t2.b=2 or t2.b=1)`
/// So `(t1.a=1 or t1.a=1), (t2.b=2 or t2.b=1)` may be pushed down join and reduce rows between join
pub struct ExtractOrPredicate {
    metadata: MetadataRef,
    pattern: SExpr,
}

impl ExtractOrPredicate {
    pub fn new(metadata: MetadataRef) -> Self {
        Self {
            // Filter
            //  \
            //   InnerJoin
            //   | \
            //   |  *
            //   *
            metadata,
            pattern: SExpr::create_unary(
                PatternPlan {
                    plan_type: RelOp::Filter,
                }
                .into(),
                SExpr::create_binary(
                    PatternPlan {
                        plan_type: RelOp::LogicalJoin,
                    }
                    .into(),
                    SExpr::create_leaf(
                        PatternPlan {
                            plan_type: RelOp::Pattern,
                        }
                        .into(),
                    ),
                    SExpr::create_leaf(
                        PatternPlan {
                            plan_type: RelOp::Pattern,
                        }
                        .into(),
                    ),
                ),
            ),
        }
    }

    fn extract_or_predicate(
        &self,
        or_expr: &OrExpr,
        required_table: IndexType,
    ) -> Result<Option<Scalar>> {
        let or_args = flatten_ors(or_expr.clone());
        let mut extracted_scalars = Vec::new();
        let meta_data = self.metadata.read();
        let table_name = meta_data.table(required_table).name();
        for or_arg in or_args.iter() {
            let mut sub_scalars = Vec::new();
            if let Scalar::AndExpr(and_expr) = or_arg {
                let and_args = flatten_ands(and_expr.clone());
                for and_arg in and_args.iter() {
                    if let Scalar::OrExpr(or_expr) = and_arg {
                        if let Some(scalar) = self.extract_or_predicate(or_expr, required_table)? {
                            sub_scalars.push(scalar);
                        }
                    } else {
                        let used_tables = and_arg.used_tables();
                        if used_tables.len() == 1 && used_tables.contains(table_name) {
                            sub_scalars.push(and_arg.clone());
                        }
                    }
                }
            } else {
                let used_tables = or_arg.used_tables();
                if used_tables.len() == 1 && used_tables.contains(table_name) {
                    sub_scalars.push(or_arg.clone());
                }
            }
            if sub_scalars.is_empty() {
                return Ok(None);
            }

            extracted_scalars.push(make_and_expr(&sub_scalars));
        }

        if !extracted_scalars.is_empty() {
            return Ok(Some(make_or_expr(&extracted_scalars)));
        }

        Ok(None)
    }

    fn rewrite_predicates(&self, s_expr: &SExpr) -> Result<Vec<Scalar>> {
        let mut filter: Filter = s_expr.plan().clone().try_into()?;
        let mut predicates = filter.predicates;
        let join_used_tables = s_expr.child(0)?.used_tables()?;
        let mut new_predicates = Vec::new();
        for predicate in predicates.iter() {
            if let Scalar::OrExpr(or_expr) = predicate {
                for join_used_table in join_used_tables.iter() {
                    if let Some(predicate) = self.extract_or_predicate(or_expr, *join_used_table)? {
                        new_predicates.push(predicate)
                    }
                }
            }
        }
        predicates.extend(new_predicates);
        Ok(predicates)
    }

    pub fn optimize(&self, s_expr: SExpr) -> Result<SExpr> {
        let rel_op = s_expr.plan();
        if s_expr.match_pattern(&self.pattern) {
            let predicates = self.rewrite_predicates(&s_expr)?;
            if predicates.is_empty() {
                return Ok(s_expr);
            }
            let (_, result) = try_push_down_filter_join(&s_expr, predicates)?;
            Ok(result)
        } else {
            let children = s_expr
                .children()
                .iter()
                .map(|expr| self.optimize(expr.clone()))
                .collect::<Result<Vec<_>>>()?;
            Ok(SExpr::create(rel_op.clone(), children, None, None))
        }
    }
}

// Flatten nested ORs, such as `a=1 or b=1 or c=1`
// It'll be flatten to [a=1, b=1, c=1]
fn flatten_ors(or_expr: OrExpr) -> Vec<Scalar> {
    let mut flattened_ors = Vec::new();
    let or_args = vec![*or_expr.left, *or_expr.right];
    for or_arg in or_args.iter() {
        match or_arg {
            Scalar::OrExpr(or_expr) => flattened_ors.extend(flatten_ors(or_expr.clone())),
            _ => flattened_ors.push(or_arg.clone()),
        }
    }
    flattened_ors
}

// Flatten nested ORs, such as `a=1 and b=1 and c=1`
// It'll be flatten to [a=1, b=1, c=1]
fn flatten_ands(and_expr: AndExpr) -> Vec<Scalar> {
    let mut flattened_ands = Vec::new();
    let and_args = vec![*and_expr.left, *and_expr.right];
    for and_arg in and_args.iter() {
        match and_arg {
            Scalar::AndExpr(and_expr) => flattened_ands.extend(flatten_ands(and_expr.clone())),
            _ => flattened_ands.push(and_arg.clone()),
        }
    }
    flattened_ands
}

fn make_and_expr(scalars: &[Scalar]) -> Scalar {
    if scalars.len() == 1 {
        return scalars[0].clone();
    }
    Scalar::AndExpr(AndExpr {
        left: Box::new(scalars[0].clone()),
        right: Box::new(make_and_expr(&scalars[1..])),
        return_type: Box::new(DataTypeImpl::Boolean(BooleanType::default())),
    })
}

fn make_or_expr(scalars: &[Scalar]) -> Scalar {
    if scalars.len() == 1 {
        return scalars[0].clone();
    }
    Scalar::OrExpr(OrExpr {
        left: Box::new(scalars[0].clone()),
        right: Box::new(make_or_expr(&scalars[1..])),
        return_type: Box::new(DataTypeImpl::Boolean(BooleanType::default())),
    })
}
