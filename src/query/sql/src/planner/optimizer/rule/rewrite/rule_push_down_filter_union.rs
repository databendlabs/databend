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

use ahash::HashMap;
use common_exception::ErrorCode;
use common_exception::Result;

use crate::optimizer::rule::Rule;
use crate::optimizer::rule::TransformResult;
use crate::optimizer::RuleID;
use crate::optimizer::SExpr;
use crate::plans::AggregateFunction;
use crate::plans::AndExpr;
use crate::plans::BoundColumnRef;
use crate::plans::CastExpr;
use crate::plans::ComparisonExpr;
use crate::plans::Filter;
use crate::plans::FunctionCall;
use crate::plans::OrExpr;
use crate::plans::PatternPlan;
use crate::plans::RelOp;
use crate::plans::Scalar;
use crate::plans::UnionAll;
use crate::ColumnBinding;
use crate::IndexType;
use crate::Visibility;

// For a union query, it's not allowed to add `filter` after union
// Such as: `(select * from t1 union all select * from t2) where a > 1`, it's invalid.
// However, it's possible to have `filter` after `union` when involved `view`
// Such as: `create view v_t as (select * from t1 union all select * from t2)`.
// Then use the view with filter, `select * from v_t where a > 1`;
// So it'll be efficient to push down `filter` to `union`, reduce the size of data to pull from table.
pub struct RulePushDownFilterUnion {
    id: RuleID,
    pattern: SExpr,
}

impl RulePushDownFilterUnion {
    pub fn new() -> Self {
        Self {
            id: RuleID::PushDownFilterUnion,
            // Filter
            //  \
            //   UnionAll
            //     /  \
            //   ...   ...
            pattern: SExpr::create_unary(
                PatternPlan {
                    plan_type: RelOp::Filter,
                }
                .into(),
                SExpr::create_binary(
                    PatternPlan {
                        plan_type: RelOp::UnionAll,
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
}

impl Rule for RulePushDownFilterUnion {
    fn id(&self) -> RuleID {
        self.id
    }

    fn apply(&self, s_expr: &SExpr, state: &mut TransformResult) -> Result<()> {
        let filter: Filter = s_expr.plan().clone().try_into()?;
        let union_s_expr = s_expr.child(0)?;
        let union: UnionAll = union_s_expr.plan().clone().try_into()?;

        // Create a filter which matches union's right child.
        let index_pairs: HashMap<IndexType, IndexType> =
            union.pairs.iter().map(|pair| (pair.0, pair.1)).collect();
        let new_predicates = filter
            .predicates
            .iter()
            .map(|predicate| replace_column_binding(&index_pairs, predicate.clone()))
            .collect::<Result<Vec<_>>>()?;
        let right_filer = Filter {
            predicates: new_predicates,
            is_having: filter.is_having,
        };

        let mut union_left_child = union_s_expr.child(0)?.clone();
        let mut union_right_child = union_s_expr.child(1)?.clone();

        // Add filter to union children
        union_left_child = SExpr::create_unary(filter.into(), union_left_child);
        union_right_child = SExpr::create_unary(right_filer.into(), union_right_child);

        let result = SExpr::create_binary(union.into(), union_left_child, union_right_child);
        state.add_result(result);

        Ok(())
    }

    fn pattern(&self) -> &SExpr {
        &self.pattern
    }
}

fn replace_column_binding(
    index_pairs: &HashMap<IndexType, IndexType>,
    scalar: Scalar,
) -> Result<Scalar> {
    match scalar {
        Scalar::BoundColumnRef(column) => {
            let index = column.column.index;
            if index_pairs.contains_key(&index) {
                let new_column = ColumnBinding {
                    database_name: None,
                    table_name: None,
                    column_name: "".to_string(),
                    index: *index_pairs.get(&index).unwrap(),
                    data_type: column.column.data_type,
                    visibility: Visibility::Visible,
                };
                return Ok(Scalar::BoundColumnRef(BoundColumnRef {
                    column: new_column,
                }));
            }
            Ok(Scalar::BoundColumnRef(column))
        }
        constant_expr @ Scalar::ConstantExpr(_) => Ok(constant_expr),
        Scalar::AndExpr(expr) => Ok(Scalar::AndExpr(AndExpr {
            left: Box::new(replace_column_binding(index_pairs, *expr.left)?),
            right: Box::new(replace_column_binding(index_pairs, *expr.right)?),
            return_type: expr.return_type,
        })),
        Scalar::OrExpr(expr) => Ok(Scalar::OrExpr(OrExpr {
            left: Box::new(replace_column_binding(index_pairs, *expr.left)?),
            right: Box::new(replace_column_binding(index_pairs, *expr.right)?),
            return_type: expr.return_type,
        })),
        Scalar::ComparisonExpr(expr) => Ok(Scalar::ComparisonExpr(ComparisonExpr {
            op: expr.op,
            left: Box::new(replace_column_binding(index_pairs, *expr.left)?),
            right: Box::new(replace_column_binding(index_pairs, *expr.right)?),
            return_type: expr.return_type,
        })),
        Scalar::AggregateFunction(expr) => Ok(Scalar::AggregateFunction(AggregateFunction {
            display_name: expr.display_name,
            func_name: expr.func_name,
            distinct: expr.distinct,
            params: expr.params,
            args: expr
                .args
                .into_iter()
                .map(|arg| replace_column_binding(index_pairs, arg))
                .collect::<Result<Vec<_>>>()?,
            return_type: expr.return_type,
        })),
        Scalar::FunctionCall(expr) => Ok(Scalar::FunctionCall(FunctionCall {
            arguments: expr
                .arguments
                .into_iter()
                .map(|arg| replace_column_binding(index_pairs, arg))
                .collect::<Result<Vec<_>>>()?,
            func_name: expr.func_name,
            arg_types: expr.arg_types,
            return_type: expr.return_type,
        })),
        Scalar::CastExpr(expr) => Ok(Scalar::CastExpr(CastExpr {
            argument: Box::new(replace_column_binding(index_pairs, *(expr.argument))?),
            from_type: expr.from_type,
            target_type: expr.target_type,
        })),
        Scalar::SubqueryExpr(_) => Err(ErrorCode::Unimplemented(
            "replace_column_binding: don't support subquery",
        )),
    }
}
