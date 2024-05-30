// Copyright 2023 Datafuse Labs.
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

use databend_common_constraint::declare::*;
use z3::Config;
use z3::Context;
use z3::SatResult;
use z3::Solver;

#[test]
fn test_bool_is_true() {
    // Test is_true function
    let ctx = &Context::new(&Config::new());
    let solver = Solver::new(ctx);
    // is_true(TRUE) => true
    solver.assert(&is_true(ctx, &true_bool(ctx)));
    // is_true(FALSE) => false
    solver.assert(&is_true(ctx, &false_bool(ctx)).not());
    // is_true(NULL) => false
    solver.assert(&is_true(ctx, &null_bool(ctx)).not());
    assert_eq!(solver.check(), SatResult::Sat);
}

#[test]
fn test_bool_logical_and() {
    // Test logical AND function
    let ctx = &Context::new(&Config::new());
    let solver = Solver::new(ctx);

    // TRUE AND TRUE => TRUE
    solver.assert(&is_true(
        ctx,
        &and_bool(ctx, &true_bool(ctx), &true_bool(ctx)),
    ));
    // TRUE AND FALSE => FALSE
    solver.assert(&is_true(ctx, &and_bool(ctx, &true_bool(ctx), &false_bool(ctx))).not());
    // FALSE AND TRUE => FALSE
    solver.assert(&is_true(ctx, &and_bool(ctx, &false_bool(ctx), &true_bool(ctx))).not());
    // TRUE AND NULL => NULL
    solver.assert(&is_null_bool(
        ctx,
        &and_bool(ctx, &true_bool(ctx), &null_bool(ctx)),
    ));
    // NULL AND TRUE => NULL
    solver.assert(&is_null_bool(
        ctx,
        &and_bool(ctx, &null_bool(ctx), &true_bool(ctx)),
    ));
    // FALSE AND FALSE => FALSE
    solver.assert(&is_true(ctx, &and_bool(ctx, &false_bool(ctx), &false_bool(ctx))).not());
    // FALSE AND NULL => FALSE
    solver.assert(&is_true(ctx, &and_bool(ctx, &false_bool(ctx), &null_bool(ctx))).not());
    // NULL AND FALSE => FALSE
    solver.assert(&is_true(ctx, &and_bool(ctx, &null_bool(ctx), &false_bool(ctx))).not());
    // NULL AND NULL => NULL
    solver.assert(&is_null_bool(
        ctx,
        &and_bool(ctx, &null_bool(ctx), &null_bool(ctx)),
    ));

    assert_eq!(solver.check(), SatResult::Sat);
}

#[test]
fn test_bool_logical_or() {
    // Test logical OR function
    let ctx = &Context::new(&Config::new());
    let solver = Solver::new(ctx);

    // TRUE OR TRUE => TRUE
    solver.assert(&is_true(
        ctx,
        &or_bool(ctx, &true_bool(ctx), &true_bool(ctx)),
    ));
    // TRUE OR FALSE => TRUE
    solver.assert(&is_true(
        ctx,
        &or_bool(ctx, &true_bool(ctx), &false_bool(ctx)),
    ));
    // FALSE OR TRUE => TRUE
    solver.assert(&is_true(
        ctx,
        &or_bool(ctx, &false_bool(ctx), &true_bool(ctx)),
    ));
    // TRUE OR NULL => TRUE
    solver.assert(&is_true(
        ctx,
        &or_bool(ctx, &true_bool(ctx), &null_bool(ctx)),
    ));
    // NULL OR TRUE => TRUE
    solver.assert(&is_true(
        ctx,
        &or_bool(ctx, &null_bool(ctx), &true_bool(ctx)),
    ));
    // FALSE OR FALSE => FALSE
    solver.assert(&is_true(ctx, &or_bool(ctx, &false_bool(ctx), &false_bool(ctx))).not());
    // FALSE OR NULL => NULL
    solver.assert(&is_null_bool(
        ctx,
        &or_bool(ctx, &false_bool(ctx), &null_bool(ctx)),
    ));
    // NULL OR FALSE => NULL
    solver.assert(&is_null_bool(
        ctx,
        &or_bool(ctx, &null_bool(ctx), &false_bool(ctx)),
    ));
    // NULL OR NULL => NULL
    solver.assert(&is_null_bool(
        ctx,
        &or_bool(ctx, &null_bool(ctx), &null_bool(ctx)),
    ));

    assert_eq!(solver.check(), SatResult::Sat);
}

#[test]
fn test_bool_logical_not() {
    // Test logical NOT function
    let ctx = &Context::new(&Config::new());
    let solver = Solver::new(ctx);

    // NOT TRUE => FALSE
    solver.assert(&is_true(ctx, &not_bool(ctx, &true_bool(ctx))).not());
    // NOT FALSE => TRUE
    solver.assert(&is_true(ctx, &not_bool(ctx, &false_bool(ctx))));
    // NOT NULL => NULL
    solver.assert(&is_null_bool(ctx, &not_bool(ctx, &null_bool(ctx))));

    assert_eq!(solver.check(), SatResult::Sat);
}

#[test]
fn test_int_eq() {
    // Test int equality function
    let ctx = &Context::new(&Config::new());
    let solver = Solver::new(ctx);

    // 1 == 1 => TRUE
    solver.assert(&is_true(
        ctx,
        &eq_int(ctx, &const_int(ctx, 1), &const_int(ctx, 1)),
    ));
    // 1 == 2 => FALSE
    solver.assert(&is_true(ctx, &eq_int(ctx, &const_int(ctx, 1), &const_int(ctx, 2))).not());
    // 1 == NULL => NULL
    solver.assert(&is_null_bool(
        ctx,
        &eq_int(ctx, &const_int(ctx, 1), &null_int(ctx)),
    ));
    // NULL == 1 => NULL
    solver.assert(&is_null_bool(
        ctx,
        &eq_int(ctx, &null_int(ctx), &const_int(ctx, 1)),
    ));
    // NULL == NULL => NULL
    solver.assert(&is_null_bool(
        ctx,
        &eq_int(ctx, &null_int(ctx), &null_int(ctx)),
    ));

    assert_eq!(solver.check(), SatResult::Sat);
}

#[test]
fn test_int_lt() {
    // Test int less than function
    let ctx = &Context::new(&Config::new());
    let solver = Solver::new(ctx);

    // 1 < 2 => TRUE
    solver.assert(&is_true(
        ctx,
        &lt_int(ctx, &const_int(ctx, 1), &const_int(ctx, 2)),
    ));
    // 2 < 1 => FALSE
    solver.assert(&is_true(ctx, &lt_int(ctx, &const_int(ctx, 2), &const_int(ctx, 1))).not());
    // 1 < NULL => NULL
    solver.assert(&is_null_bool(
        ctx,
        &lt_int(ctx, &const_int(ctx, 1), &null_int(ctx)),
    ));
    // NULL < 1 => NULL
    solver.assert(&is_null_bool(
        ctx,
        &lt_int(ctx, &null_int(ctx), &const_int(ctx, 1)),
    ));
    // NULL < NULL => NULL
    solver.assert(&is_null_bool(
        ctx,
        &lt_int(ctx, &null_int(ctx), &null_int(ctx)),
    ));

    assert_eq!(solver.check(), SatResult::Sat);
}

#[test]
fn test_int_le() {
    // Test int less than or equal function
    let ctx = &Context::new(&Config::new());
    let solver = Solver::new(ctx);

    // 1 <= 2 => TRUE
    solver.assert(&is_true(
        ctx,
        &le_int(ctx, &const_int(ctx, 1), &const_int(ctx, 2)),
    ));
    // 2 <= 1 => FALSE
    solver.assert(&is_true(ctx, &le_int(ctx, &const_int(ctx, 2), &const_int(ctx, 1))).not());
    // 1 <= 1 => TRUE
    solver.assert(&is_true(
        ctx,
        &le_int(ctx, &const_int(ctx, 1), &const_int(ctx, 1)),
    ));
    // 1 <= NULL => NULL
    solver.assert(&is_null_bool(
        ctx,
        &le_int(ctx, &const_int(ctx, 1), &null_int(ctx)),
    ));
    // NULL <= 1 => NULL
    solver.assert(&is_null_bool(
        ctx,
        &le_int(ctx, &null_int(ctx), &const_int(ctx, 1)),
    ));
    // NULL <= NULL => NULL
    solver.assert(&is_null_bool(
        ctx,
        &le_int(ctx, &null_int(ctx), &null_int(ctx)),
    ));

    assert_eq!(solver.check(), SatResult::Sat);
}

#[test]
fn test_int_gt() {
    // Test int greater than function
    let ctx = &Context::new(&Config::new());
    let solver = Solver::new(ctx);

    // 2 > 1 => TRUE
    solver.assert(&is_true(
        ctx,
        &gt_int(ctx, &const_int(ctx, 2), &const_int(ctx, 1)),
    ));
    // 1 > 2 => FALSE
    solver.assert(&is_true(ctx, &gt_int(ctx, &const_int(ctx, 1), &const_int(ctx, 2))).not());
    // 1 > NULL => NULL
    solver.assert(&is_null_bool(
        ctx,
        &gt_int(ctx, &const_int(ctx, 1), &null_int(ctx)),
    ));
    // NULL > 1 => NULL
    solver.assert(&is_null_bool(
        ctx,
        &gt_int(ctx, &null_int(ctx), &const_int(ctx, 1)),
    ));
    // NULL > NULL => NULL
    solver.assert(&is_null_bool(
        ctx,
        &gt_int(ctx, &null_int(ctx), &null_int(ctx)),
    ));

    assert_eq!(solver.check(), SatResult::Sat);
}

#[test]
fn test_int_ge() {
    // Test int greater than or equal function
    let ctx = &Context::new(&Config::new());
    let solver = Solver::new(ctx);

    // 2 >= 1 => TRUE
    solver.assert(&is_true(
        ctx,
        &ge_int(ctx, &const_int(ctx, 2), &const_int(ctx, 1)),
    ));
    // 1 >= 2 => FALSE
    solver.assert(&is_true(ctx, &ge_int(ctx, &const_int(ctx, 1), &const_int(ctx, 2))).not());
    // 1 >= 1 => TRUE
    solver.assert(&is_true(
        ctx,
        &ge_int(ctx, &const_int(ctx, 1), &const_int(ctx, 1)),
    ));
    // 1 >= NULL => NULL
    solver.assert(&is_null_bool(
        ctx,
        &ge_int(ctx, &const_int(ctx, 1), &null_int(ctx)),
    ));
    // NULL >= 1 => NULL
    solver.assert(&is_null_bool(
        ctx,
        &ge_int(ctx, &null_int(ctx), &const_int(ctx, 1)),
    ));
    // NULL >= NULL => NULL
    solver.assert(&is_null_bool(
        ctx,
        &ge_int(ctx, &null_int(ctx), &null_int(ctx)),
    ));

    assert_eq!(solver.check(), SatResult::Sat);
}

#[test]
fn test_int_plus() {
    // Test int plus function
    let ctx = &Context::new(&Config::new());
    let solver = Solver::new(ctx);

    // 1 + 2 = 3 => TRUE
    solver.assert(&is_true(
        ctx,
        &eq_int(
            ctx,
            &plus_int(ctx, &const_int(ctx, 1), &const_int(ctx, 2)),
            &const_int(ctx, 3),
        ),
    ));
    // 1 + NULL = 3 => NULL
    solver.assert(&is_null_bool(
        ctx,
        &eq_int(
            ctx,
            &plus_int(ctx, &const_int(ctx, 1), &null_int(ctx)),
            &const_int(ctx, 3),
        ),
    ));
    // NULL + 1 = 3 => NULL
    solver.assert(&is_null_bool(
        ctx,
        &eq_int(
            ctx,
            &plus_int(ctx, &null_int(ctx), &const_int(ctx, 1)),
            &const_int(ctx, 3),
        ),
    ));
    // NULL + NULL = 3 => NULL
    solver.assert(&is_null_bool(
        ctx,
        &eq_int(
            ctx,
            &plus_int(ctx, &null_int(ctx), &null_int(ctx)),
            &const_int(ctx, 3),
        ),
    ));

    assert_eq!(solver.check(), SatResult::Sat);
}

#[test]
fn test_int_minus() {
    // Test int minus function
    let ctx = &Context::new(&Config::new());
    let solver = Solver::new(ctx);

    // 3 - 2 = 1 => TRUE
    solver.assert(&is_true(
        ctx,
        &eq_int(
            ctx,
            &minus_int(ctx, &const_int(ctx, 3), &const_int(ctx, 2)),
            &const_int(ctx, 1),
        ),
    ));
    // 1 - NULL = 3 => NULL
    solver.assert(&is_null_bool(
        ctx,
        &eq_int(
            ctx,
            &minus_int(ctx, &const_int(ctx, 1), &null_int(ctx)),
            &const_int(ctx, 3),
        ),
    ));
    // NULL - 1 = 3 => NULL
    solver.assert(&is_null_bool(
        ctx,
        &eq_int(
            ctx,
            &minus_int(ctx, &null_int(ctx), &const_int(ctx, 1)),
            &const_int(ctx, 3),
        ),
    ));
    // NULL - NULL = 3 => NULL
    solver.assert(&is_null_bool(
        ctx,
        &eq_int(
            ctx,
            &minus_int(ctx, &null_int(ctx), &null_int(ctx)),
            &const_int(ctx, 3),
        ),
    ));

    assert_eq!(solver.check(), SatResult::Sat);
}

#[test]
fn test_int_unary_minus() {
    // Test int minus function
    let ctx = &Context::new(&Config::new());
    let solver = Solver::new(ctx);

    // - 0 => 0
    solver.assert(&is_true(
        ctx,
        &eq_int(
            ctx,
            &unary_minus_int(ctx, &const_int(ctx, 0)),
            &const_int(ctx, 0),
        ),
    ));
    // - 2 => -2
    solver.assert(&is_true(
        ctx,
        &eq_int(
            ctx,
            &unary_minus_int(ctx, &const_int(ctx, 2)),
            &const_int(ctx, -2),
        ),
    ));
    // - NULL => NULL
    solver.assert(&is_null_int(ctx, &unary_minus_int(ctx, &null_int(ctx))));

    assert_eq!(solver.check(), SatResult::Sat);
}

#[test]
fn test_int_nullability() {
    // Test int nullability
    let ctx = &Context::new(&Config::new());
    let solver = Solver::new(ctx);

    let int = const_int(ctx, 1);
    solver.assert(&is_null_int(ctx, &int).not());
    solver.assert(&is_null_int(ctx, &null_int(ctx)));

    assert_eq!(solver.check(), SatResult::Sat);
}
