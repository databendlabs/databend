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

use z3::ast::Bool;
use z3::ast::Dynamic;
use z3::Config;
use z3::Context;
use z3::Goal;
use z3::Tactic;

use crate::declare::is_true;
use crate::mir::MirDataType;
use crate::mir::MirExpr;

pub fn simplify(expr: &MirExpr) -> Option<Vec<MirExpr>> {
    let ctx = &Context::new(&Config::new());
    let variables = expr.variables();
    let ast = mir_to_z3_assertion(ctx, expr)?;
    let simplified = simplify_z3_ast(ctx, &ast)?;
    simplified
        .iter()
        .map(|formula| MirExpr::from_z3_ast(formula, &variables))
        .collect()
}

pub fn mir_to_z3_assertion<'ctx>(ctx: &'ctx Context, expr: &MirExpr) -> Option<Bool<'ctx>> {
    Some(is_true(ctx, &expr.as_z3_ast(ctx, MirDataType::Bool).ok()?))
}

pub fn simplify_z3_ast<'ctx>(ctx: &'ctx Context, ast: &Bool<'ctx>) -> Option<Vec<Dynamic<'ctx>>> {
    let goal = Goal::new(ctx, false, false, false);
    goal.assert(ast);

    let formulas = Tactic::repeat(
        ctx,
        &Tactic::new(ctx, "simplify")
            .and_then(&Tactic::new(ctx, "ctx-solver-simplify"))
            .and_then(&Tactic::new(ctx, "propagate-values"))
            .and_then(&Tactic::new(ctx, "propagate-ineqs")),
        5,
    )
    .apply(&goal, None)
    .ok()?
    .list_subgoals()
    .next()?
    .get_formulas::<Dynamic>();

    Some(formulas)
}
