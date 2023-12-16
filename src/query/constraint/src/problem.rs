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

use z3::ast::forall_const;
use z3::ast::Ast;
use z3::Config;
use z3::Context;
use z3::SatResult;
use z3::Solver;

use crate::declare::is_true;
use crate::declare::var_bool;
use crate::declare::var_int;
use crate::mir::MirDataType;
use crate::mir::MirExpr;
use crate::mir::MirUnaryOperator;

/// Given the assertions are true, try to prove that a certain variable mut be not null.
///
/// Basically, this function tries to prove the following proposition:
///
///    ∀x. assertion(x) -> x is not null
///
/// ```
/// use databend_common_constraint::mir::*;
/// use databend_common_constraint::problem::variable_must_not_null;
///
/// // a > 0
/// let assertion = MirExpr::BinaryOperator {
///     op: MirBinaryOperator::Gt,
///     left: Box::new(MirExpr::Variable {
///         name: "a".to_string(),
///         data_type: MirDataType::Int,
///     }),
///     right: Box::new(MirExpr::Constant(MirConstant::Int(0))),
/// };
///
/// // a > 0 => a is not null
/// assert!(variable_must_not_null(&assertion, "a"));
/// ```
pub fn variable_must_not_null(assertion: &MirExpr, variable: &str) -> bool {
    let ctx = &Context::new(&Config::new());

    let variables = assertion.variables();
    let for_all_free_vars = variables
        .iter()
        .map(|(name, data_type)| match data_type {
            MirDataType::Bool => var_bool(ctx, name),
            MirDataType::Int => var_int(ctx, name),
        })
        .collect::<Vec<_>>();

    let assertion = if let Ok(assertion) = assertion.as_z3_ast(ctx, MirDataType::Bool) {
        is_true(ctx, &assertion)
    } else {
        return false;
    };

    let variable_is_not_null = MirExpr::UnaryOperator {
        op: MirUnaryOperator::Not,
        arg: Box::new(MirExpr::UnaryOperator {
            op: MirUnaryOperator::IsNull,
            arg: Box::new(MirExpr::Variable {
                name: variable.to_string(),
                data_type: variables[variable],
            }),
        }),
    };
    let variable_is_not_null =
        if let Ok(variable_is_not_null) = variable_is_not_null.as_z3_ast(ctx, MirDataType::Bool) {
            is_true(ctx, &variable_is_not_null)
        } else {
            return false;
        };

    let p = forall_const(
        ctx,
        for_all_free_vars
            .iter()
            .map(|v| v as &dyn Ast)
            .collect::<Vec<_>>()
            .as_slice(),
        &[],
        &assertion.implies(&variable_is_not_null),
    );

    let solver = Solver::new(ctx);
    solver.push();
    solver.assert(&p);
    let result = solver.check();

    result == SatResult::Sat
}
