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

use common_exception::Result;
use common_expression::type_check;
use common_expression::RawExpr;
use common_functions_v2::scalars::BUILTIN_FUNCTIONS;

use crate::plans::ComparisonExpr;
use crate::plans::ComparisonOp;
use crate::plans::LogicalJoin;
use crate::plans::Scalar;

pub fn get_join_predicates(join: &LogicalJoin) -> Result<Vec<Scalar>> {
    Ok(join
        .left_conditions
        .iter()
        .zip(join.right_conditions.iter())
        .map(|(left_cond, right_cond)| {
            let registry = BUILTIN_FUNCTIONS;
            let raw_expr = RawExpr::FunctionCall {
                span: None,
                name: "=".to_string(),
                params: vec![],
                args: vec![left_cond.as_raw_expr(), right_cond.as_raw_expr()],
            };
            let expr = type_check::check(&raw_expr, &registry)
                .map_err(|(_, e)| common_exception::ErrorCode::SemanticError(e))?;
            Ok(Scalar::ComparisonExpr(ComparisonExpr {
                left: Box::new(left_cond.clone()),
                right: Box::new(right_cond.clone()),
                op: ComparisonOp::Equal,
                return_type: Box::new(expr.data_type().clone()),
            }))
        })
        .collect::<Result<Vec<_>>>()?
        .into_iter()
        .chain(join.non_equi_conditions.clone().into_iter())
        .collect())
}
