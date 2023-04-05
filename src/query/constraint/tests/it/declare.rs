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

use common_constraint::prelude::*;
use z3::ast::Int;
use z3::Config;
use z3::Context;
use z3::SatResult;
use z3::Solver;

#[test]
fn test_bool_is_true() {
    // Test is_true function
    let context = Context::new(&Config::new());
    let solver = Solver::new(&context);
    // is_true(TRUE) => true
    solver.assert(&is_true(&context, &true_bool(&context)));
    // is_true(FALSE) => false
    solver.assert(&is_true(&context, &false_bool(&context)).not());
    // is_true(NULL) => false
    solver.assert(&is_true(&context, &null_bool(&context)).not());
    assert_eq!(solver.check(), SatResult::Sat);
}

#[test]
fn test_bool_logical_and() {
    // Test logical AND function
    let context = Context::new(&Config::new());
    let solver = Solver::new(&context);

    // TRUE AND TRUE => TRUE
    solver.assert(&is_true(
        &context,
        &and_nullable_bool(&context, &true_bool(&context), &true_bool(&context)),
    ));
    // TRUE AND FALSE => FALSE
    solver.assert(
        &is_true(
            &context,
            &and_nullable_bool(&context, &true_bool(&context), &false_bool(&context)),
        )
        .not(),
    );
    // FALSE AND TRUE => FALSE
    solver.assert(
        &is_true(
            &context,
            &and_nullable_bool(&context, &false_bool(&context), &true_bool(&context)),
        )
        .not(),
    );
    // TRUE AND NULL => NULL
    solver.assert(&is_null_bool(
        &context,
        &and_nullable_bool(&context, &true_bool(&context), &null_bool(&context)),
    ));
    // NULL AND TRUE => NULL
    solver.assert(&is_null_bool(
        &context,
        &and_nullable_bool(&context, &null_bool(&context), &true_bool(&context)),
    ));
    // FALSE AND FALSE => FALSE
    solver.assert(
        &is_true(
            &context,
            &and_nullable_bool(&context, &false_bool(&context), &false_bool(&context)),
        )
        .not(),
    );
    // FALSE AND NULL => FALSE
    solver.assert(
        &is_true(
            &context,
            &and_nullable_bool(&context, &false_bool(&context), &null_bool(&context)),
        )
        .not(),
    );
    // NULL AND FALSE => FALSE
    solver.assert(
        &is_true(
            &context,
            &and_nullable_bool(&context, &null_bool(&context), &false_bool(&context)),
        )
        .not(),
    );
    // NULL AND NULL => NULL
    solver.assert(&is_null_bool(
        &context,
        &and_nullable_bool(&context, &null_bool(&context), &null_bool(&context)),
    ));

    assert_eq!(solver.check(), SatResult::Sat);
}

#[test]
fn test_bool_logical_or() {
    // Test logical OR function
    let context = Context::new(&Config::new());
    let solver = Solver::new(&context);

    // TRUE OR TRUE => TRUE
    solver.assert(&is_true(
        &context,
        &or_nullable_bool(&context, &true_bool(&context), &true_bool(&context)),
    ));
    // TRUE OR FALSE => TRUE
    solver.assert(&is_true(
        &context,
        &or_nullable_bool(&context, &true_bool(&context), &false_bool(&context)),
    ));
    // FALSE OR TRUE => TRUE
    solver.assert(&is_true(
        &context,
        &or_nullable_bool(&context, &false_bool(&context), &true_bool(&context)),
    ));
    // TRUE OR NULL => TRUE
    solver.assert(&is_true(
        &context,
        &or_nullable_bool(&context, &true_bool(&context), &null_bool(&context)),
    ));
    // NULL OR TRUE => TRUE
    solver.assert(&is_true(
        &context,
        &or_nullable_bool(&context, &null_bool(&context), &true_bool(&context)),
    ));
    // FALSE OR FALSE => FALSE
    solver.assert(
        &is_true(
            &context,
            &or_nullable_bool(&context, &false_bool(&context), &false_bool(&context)),
        )
        .not(),
    );
    // FALSE OR NULL => NULL
    solver.assert(&is_null_bool(
        &context,
        &or_nullable_bool(&context, &false_bool(&context), &null_bool(&context)),
    ));
    // NULL OR FALSE => NULL
    solver.assert(&is_null_bool(
        &context,
        &or_nullable_bool(&context, &null_bool(&context), &false_bool(&context)),
    ));
    // NULL OR NULL => NULL
    solver.assert(&is_null_bool(
        &context,
        &or_nullable_bool(&context, &null_bool(&context), &null_bool(&context)),
    ));

    assert_eq!(solver.check(), SatResult::Sat);
}

#[test]
fn test_bool_logical_not() {
    // Test logical NOT function
    let context = Context::new(&Config::new());
    let solver = Solver::new(&context);

    // NOT TRUE => FALSE
    solver.assert(&is_true(&context, &not_nullable_bool(&context, &true_bool(&context))).not());
    // NOT FALSE => TRUE
    solver.assert(&is_true(
        &context,
        &not_nullable_bool(&context, &false_bool(&context)),
    ));
    // NOT NULL => NULL
    solver.assert(&is_null_bool(
        &context,
        &not_nullable_bool(&context, &null_bool(&context)),
    ));

    assert_eq!(solver.check(), SatResult::Sat);
}

#[test]
fn test_int_eq() {
    // Test int equality function
    let context = Context::new(&Config::new());
    let solver = Solver::new(&context);

    // 1 == 1 => TRUE
    solver.assert(&is_true(
        &context,
        &eq_int(
            &context,
            &Int::from_i64(&context, 1),
            &Int::from_i64(&context, 1),
        ),
    ));
    // 1 == 2 => FALSE
    solver.assert(
        &is_true(
            &context,
            &eq_int(
                &context,
                &Int::from_i64(&context, 1),
                &Int::from_i64(&context, 2),
            ),
        )
        .not(),
    );
    // 1 == NULL => NULL
    solver.assert(&is_null_bool(
        &context,
        &eq_int(&context, &Int::from_i64(&context, 1), &null_int(&context)),
    ));
    // NULL == 1 => NULL
    solver.assert(&is_null_bool(
        &context,
        &eq_int(&context, &null_int(&context), &Int::from_i64(&context, 1)),
    ));
    // NULL == NULL => NULL
    solver.assert(&is_null_bool(
        &context,
        &eq_int(&context, &null_int(&context), &null_int(&context)),
    ));

    assert_eq!(solver.check(), SatResult::Sat);
}

#[test]
fn test_int_lt() {
    // Test int less than function
    let context = Context::new(&Config::new());
    let solver = Solver::new(&context);

    // 1 < 2 => TRUE
    solver.assert(&is_true(
        &context,
        &lt_int(
            &context,
            &Int::from_i64(&context, 1),
            &Int::from_i64(&context, 2),
        ),
    ));
    // 2 < 1 => FALSE
    solver.assert(
        &is_true(
            &context,
            &lt_int(
                &context,
                &Int::from_i64(&context, 2),
                &Int::from_i64(&context, 1),
            ),
        )
        .not(),
    );
    // 1 < NULL => NULL
    solver.assert(&is_null_bool(
        &context,
        &lt_int(&context, &Int::from_i64(&context, 1), &null_int(&context)),
    ));
    // NULL < 1 => NULL
    solver.assert(&is_null_bool(
        &context,
        &lt_int(&context, &null_int(&context), &Int::from_i64(&context, 1)),
    ));
    // NULL < NULL => NULL
    solver.assert(&is_null_bool(
        &context,
        &lt_int(&context, &null_int(&context), &null_int(&context)),
    ));

    assert_eq!(solver.check(), SatResult::Sat);
}

#[test]
fn test_int_le() {
    // Test int less than or equal function
    let context = Context::new(&Config::new());
    let solver = Solver::new(&context);

    // 1 <= 2 => TRUE
    solver.assert(&is_true(
        &context,
        &le_int(
            &context,
            &Int::from_i64(&context, 1),
            &Int::from_i64(&context, 2),
        ),
    ));
    // 2 <= 1 => FALSE
    solver.assert(
        &is_true(
            &context,
            &le_int(
                &context,
                &Int::from_i64(&context, 2),
                &Int::from_i64(&context, 1),
            ),
        )
        .not(),
    );
    // 1 <= 1 => TRUE
    solver.assert(&is_true(
        &context,
        &le_int(
            &context,
            &Int::from_i64(&context, 1),
            &Int::from_i64(&context, 1),
        ),
    ));
    // 1 <= NULL => NULL
    solver.assert(&is_null_bool(
        &context,
        &le_int(&context, &Int::from_i64(&context, 1), &null_int(&context)),
    ));
    // NULL <= 1 => NULL
    solver.assert(&is_null_bool(
        &context,
        &le_int(&context, &null_int(&context), &Int::from_i64(&context, 1)),
    ));
    // NULL <= NULL => NULL
    solver.assert(&is_null_bool(
        &context,
        &le_int(&context, &null_int(&context), &null_int(&context)),
    ));

    assert_eq!(solver.check(), SatResult::Sat);
}

#[test]
fn test_int_gt() {
    // Test int greater than function
    let context = Context::new(&Config::new());
    let solver = Solver::new(&context);

    // 2 > 1 => TRUE
    solver.assert(&is_true(
        &context,
        &gt_int(
            &context,
            &Int::from_i64(&context, 2),
            &Int::from_i64(&context, 1),
        ),
    ));
    // 1 > 2 => FALSE
    solver.assert(
        &is_true(
            &context,
            &gt_int(
                &context,
                &Int::from_i64(&context, 1),
                &Int::from_i64(&context, 2),
            ),
        )
        .not(),
    );
    // 1 > NULL => NULL
    solver.assert(&is_null_bool(
        &context,
        &gt_int(&context, &Int::from_i64(&context, 1), &null_int(&context)),
    ));
    // NULL > 1 => NULL
    solver.assert(&is_null_bool(
        &context,
        &gt_int(&context, &null_int(&context), &Int::from_i64(&context, 1)),
    ));
    // NULL > NULL => NULL
    solver.assert(&is_null_bool(
        &context,
        &gt_int(&context, &null_int(&context), &null_int(&context)),
    ));

    assert_eq!(solver.check(), SatResult::Sat);
}

#[test]
fn test_int_ge() {
    // Test int greater than or equal function
    let context = Context::new(&Config::new());
    let solver = Solver::new(&context);

    // 2 >= 1 => TRUE
    solver.assert(&is_true(
        &context,
        &ge_int(
            &context,
            &Int::from_i64(&context, 2),
            &Int::from_i64(&context, 1),
        ),
    ));
    // 1 >= 2 => FALSE
    solver.assert(
        &is_true(
            &context,
            &ge_int(
                &context,
                &Int::from_i64(&context, 1),
                &Int::from_i64(&context, 2),
            ),
        )
        .not(),
    );
    // 1 >= 1 => TRUE
    solver.assert(&is_true(
        &context,
        &ge_int(
            &context,
            &Int::from_i64(&context, 1),
            &Int::from_i64(&context, 1),
        ),
    ));
    // 1 >= NULL => NULL
    solver.assert(&is_null_bool(
        &context,
        &ge_int(&context, &Int::from_i64(&context, 1), &null_int(&context)),
    ));
    // NULL >= 1 => NULL
    solver.assert(&is_null_bool(
        &context,
        &ge_int(&context, &null_int(&context), &Int::from_i64(&context, 1)),
    ));
    // NULL >= NULL => NULL
    solver.assert(&is_null_bool(
        &context,
        &ge_int(&context, &null_int(&context), &null_int(&context)),
    ));

    assert_eq!(solver.check(), SatResult::Sat);
}

#[test]
fn test_int_plus() {
    // Test int plus function
    let context = Context::new(&Config::new());
    let solver = Solver::new(&context);

    // 1 + 2 = 3 => TRUE
    solver.assert(&is_true(
        &context,
        &eq_int(
            &context,
            &plus_int(
                &context,
                &Int::from_i64(&context, 1),
                &Int::from_i64(&context, 2),
            ),
            &Int::from_i64(&context, 3),
        ),
    ));
    // 1 + NULL = 3 => NULL
    solver.assert(&is_null_bool(
        &context,
        &eq_int(
            &context,
            &plus_int(&context, &Int::from_i64(&context, 1), &null_int(&context)),
            &Int::from_i64(&context, 3),
        ),
    ));
    // NULL + 1 = 3 => NULL
    solver.assert(&is_null_bool(
        &context,
        &eq_int(
            &context,
            &plus_int(&context, &null_int(&context), &Int::from_i64(&context, 1)),
            &Int::from_i64(&context, 3),
        ),
    ));
    // NULL + NULL = 3 => NULL
    solver.assert(&is_null_bool(
        &context,
        &eq_int(
            &context,
            &plus_int(&context, &null_int(&context), &null_int(&context)),
            &Int::from_i64(&context, 3),
        ),
    ));

    assert_eq!(solver.check(), SatResult::Sat);
}

#[test]
fn test_int_minus() {
    // Test int minus function
    let context = Context::new(&Config::new());
    let solver = Solver::new(&context);

    // 3 - 2 = 1 => TRUE
    solver.assert(&is_true(
        &context,
        &eq_int(
            &context,
            &minus_int(
                &context,
                &Int::from_i64(&context, 3),
                &Int::from_i64(&context, 2),
            ),
            &Int::from_i64(&context, 1),
        ),
    ));
    // 1 - NULL = 3 => NULL
    solver.assert(&is_null_bool(
        &context,
        &eq_int(
            &context,
            &minus_int(&context, &Int::from_i64(&context, 1), &null_int(&context)),
            &Int::from_i64(&context, 3),
        ),
    ));
    // NULL - 1 = 3 => NULL
    solver.assert(&is_null_bool(
        &context,
        &eq_int(
            &context,
            &minus_int(&context, &null_int(&context), &Int::from_i64(&context, 1)),
            &Int::from_i64(&context, 3),
        ),
    ));
    // NULL - NULL = 3 => NULL
    solver.assert(&is_null_bool(
        &context,
        &eq_int(
            &context,
            &minus_int(&context, &null_int(&context), &null_int(&context)),
            &Int::from_i64(&context, 3),
        ),
    ));

    assert_eq!(solver.check(), SatResult::Sat);
}
