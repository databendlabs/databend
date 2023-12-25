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

#[path = "../tests/it/scalars/parser.rs"]
mod parser;

use criterion::Criterion;
use databend_common_expression::type_check;
use databend_common_expression::DataBlock;
use databend_common_expression::Evaluator;
use databend_common_expression::FunctionContext;
use databend_common_functions::BUILTIN_FUNCTIONS;

fn bench(c: &mut Criterion) {
    let mut group = c.benchmark_group("bench_array");

    for n in [1, 10, 100, 1000, 100000] {
        let text = "[".to_string() + &"true,".repeat(n) + "]";

        group.bench_function(format!("parse/{n}"), |b| {
            b.iter(|| parser::parse_raw_expr(&text, &[]))
        });

        let raw_expr = parser::parse_raw_expr(&text, &[]);

        group.bench_function(format!("check/{n}"), |b| {
            b.iter(|| type_check::check(&raw_expr, &BUILTIN_FUNCTIONS))
        });

        let func_ctx = FunctionContext::default();
        let expr = type_check::check(&raw_expr, &BUILTIN_FUNCTIONS).unwrap();
        let block = DataBlock::new(vec![], 1);
        let evaluator = Evaluator::new(&block, &func_ctx, &BUILTIN_FUNCTIONS);

        group.bench_function(format!("eval/{n}"), |b| b.iter(|| evaluator.run(&expr)));
    }
}

criterion_group!(benches, bench);
criterion_main!(benches);
