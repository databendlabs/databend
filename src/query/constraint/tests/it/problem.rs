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

use common_constraint::prelude::and_nullable_bool;
use common_constraint::prelude::assert_int_is_not_null;
use common_constraint::prelude::gt_int;
use common_constraint::prelude::is_not_null_int;
use common_constraint::prelude::is_true;
use common_constraint::prelude::lt_int;
use common_constraint::prelude::or_nullable_bool;
use common_constraint::prelude::true_bool;
use z3::ast::Int;
use z3::Config;
use z3::Context;
use z3::SatResult;
use z3::Solver;

#[test]
fn test_assert_int_not_null() {
    let ctx = Context::new(&Config::default());
    let solver = Solver::new(&ctx);

    {
        // a is not null -> a is not null
        let proposition = is_not_null_int(&ctx, &Int::new_const(&ctx, "a"));
        assert_eq!(
            assert_int_is_not_null(
                &ctx,
                &solver,
                &[Int::new_const(&ctx, "a")],
                &Int::new_const(&ctx, "a"),
                &proposition
            ),
            SatResult::Sat
        );
    }

    {
        // a > 0 -> a is not null
        let proposition = is_true(
            &ctx,
            &gt_int(&ctx, &Int::new_const(&ctx, "a"), &Int::from_i64(&ctx, 0)),
        );
        assert_eq!(
            assert_int_is_not_null(
                &ctx,
                &solver,
                &[Int::new_const(&ctx, "a")],
                &Int::new_const(&ctx, "a"),
                &proposition
            ),
            SatResult::Sat
        );
    }

    {
        // a > 0 or true -> a is maybe not null
        let proposition = is_true(
            &ctx,
            &or_nullable_bool(
                &ctx,
                &gt_int(&ctx, &Int::new_const(&ctx, "a"), &Int::from_i64(&ctx, 0)),
                &true_bool(&ctx),
            ),
        );
        assert_eq!(
            assert_int_is_not_null(
                &ctx,
                &solver,
                &[Int::new_const(&ctx, "a")],
                &Int::new_const(&ctx, "a"),
                &proposition
            ),
            SatResult::Unsat
        );
    }

    {
        // a > 0 or a < 0 -> a is not null
        let proposition = is_true(
            &ctx,
            &or_nullable_bool(
                &ctx,
                &gt_int(&ctx, &Int::new_const(&ctx, "a"), &Int::from_i64(&ctx, 0)),
                &lt_int(&ctx, &Int::new_const(&ctx, "a"), &Int::from_i64(&ctx, 0)),
            ),
        );
        assert_eq!(
            assert_int_is_not_null(
                &ctx,
                &solver,
                &[Int::new_const(&ctx, "a")],
                &Int::new_const(&ctx, "a"),
                &proposition
            ),
            SatResult::Sat
        );
    }

    {
        // a > 0 and a < 1 -> a is not null
        let proposition = is_true(
            &ctx,
            &and_nullable_bool(
                &ctx,
                &gt_int(&ctx, &Int::new_const(&ctx, "a"), &Int::from_i64(&ctx, 0)),
                &lt_int(&ctx, &Int::new_const(&ctx, "a"), &Int::from_i64(&ctx, 0)),
            ),
        );
        assert_eq!(
            assert_int_is_not_null(
                &ctx,
                &solver,
                &[Int::new_const(&ctx, "a")],
                &Int::new_const(&ctx, "a"),
                &proposition
            ),
            SatResult::Sat
        );
    }
}

#[test]
fn test_assert_int_is_not_null_multiple_variable() {
    let ctx = Context::new(&Config::default());
    let solver = Solver::new(&ctx);

    {
        // a > 0 and b > 0 -> a is not null and b is not null
        let proposition = is_true(
            &ctx,
            &and_nullable_bool(
                &ctx,
                &gt_int(&ctx, &Int::new_const(&ctx, "a"), &Int::from_i64(&ctx, 0)),
                &gt_int(&ctx, &Int::new_const(&ctx, "b"), &Int::from_i64(&ctx, 0)),
            ),
        );
        assert_eq!(
            assert_int_is_not_null(
                &ctx,
                &solver,
                &[Int::new_const(&ctx, "a"), Int::new_const(&ctx, "b"),],
                &Int::new_const(&ctx, "a"),
                &proposition
            ),
            SatResult::Sat
        );
        assert_eq!(
            assert_int_is_not_null(
                &ctx,
                &solver,
                &[Int::new_const(&ctx, "a"), Int::new_const(&ctx, "b"),],
                &Int::new_const(&ctx, "b"),
                &proposition
            ),
            SatResult::Sat
        );
    }
}
