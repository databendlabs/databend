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

#[macro_use]
extern crate criterion;

use common_constraint::prelude::*;
use criterion::black_box;
use criterion::Criterion;
use z3::ast::Int;
use z3::Config;
use z3::Context;
use z3::Solver;

fn bench(c: &mut Criterion) {
    let mut group = c.benchmark_group("bench_parser");
    group.sample_size(10);

    group.bench_function("solve_int_not_null_shared_context", |b| {
        let ctx = Context::new(&Config::default());
        let solver = Solver::new(&ctx);

        b.iter(|| {
            // a > 0 and a < 1 -> a is not null
            let proposition = is_true(
                &ctx,
                &and_nullable_bool(
                    &ctx,
                    &gt_int(&ctx, &Int::new_const(&ctx, "a"), &Int::from_i64(&ctx, 0)),
                    &lt_int(&ctx, &Int::new_const(&ctx, "a"), &Int::from_i64(&ctx, 0)),
                ),
            );
            let result = assert_int_is_not_null(
                &ctx,
                &solver,
                &[Int::new_const(&ctx, "a")],
                &Int::new_const(&ctx, "a"),
                &proposition,
            );
            black_box(result);
        });
    });

    group.bench_function("solve_int_not_null_multiple_variables", |b| {
        let ctx = Context::new(&Config::default());
        let solver = Solver::new(&ctx);

        b.iter(|| {
            // (a > 0 and b > 0) or (a < 0 and b < 0) -> a is not null and b is not null
            let proposition = is_true(
                &ctx,
                &or_nullable_bool(
                    &ctx,
                    &and_nullable_bool(
                        &ctx,
                        &gt_int(&ctx, &Int::new_const(&ctx, "a"), &Int::from_i64(&ctx, 0)),
                        &gt_int(&ctx, &Int::new_const(&ctx, "b"), &Int::from_i64(&ctx, 0)),
                    ),
                    &and_nullable_bool(
                        &ctx,
                        &lt_int(&ctx, &Int::new_const(&ctx, "a"), &Int::from_i64(&ctx, 0)),
                        &lt_int(&ctx, &Int::new_const(&ctx, "b"), &Int::from_i64(&ctx, 0)),
                    ),
                ),
            );
            let result = assert_int_is_not_null(
                &ctx,
                &solver,
                &[Int::new_const(&ctx, "a")],
                &Int::new_const(&ctx, "a"),
                &proposition,
            );
            black_box(result);
        });
    });
}

criterion_group!(benches, bench);
criterion_main!(benches);
