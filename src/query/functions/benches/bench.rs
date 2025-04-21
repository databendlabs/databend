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

fn main() {
    divan::main();
}

// bench            fastest       │ slowest       │ median        │ mean          │ samples │ iters
// ╰─ dummy                       │               │               │               │         │
//    ├─ check                    │               │               │               │         │
//    │  ├─ 10240   2.847 ms      │ 3.482 ms      │ 2.915 ms      │ 2.926 ms      │ 100     │ 100
//    │  ╰─ 102400  29.78 ms      │ 35.36 ms      │ 30.27 ms      │ 30.59 ms      │ 17      │ 17
//    ├─ eval                     │               │               │               │         │
//    │  ├─ 10240   1.091 ms      │ 1.158 ms      │ 1.123 ms      │ 1.122 ms      │ 100     │ 100
//    │  ╰─ 102400  11.42 ms      │ 12.11 ms      │ 11.69 ms      │ 11.7 ms       │ 43      │ 43
//    ╰─ parse                    │               │               │               │         │
//       ├─ 10240   178.5 ms      │ 178.9 ms      │ 178.7 ms      │ 178.7 ms      │ 3       │ 3
//       ╰─ 102400  1.82 s        │ 1.82 s        │ 1.82 s        │ 1.82 s        │ 1       │ 1
#[divan::bench_group(max_time = 0.5)]
mod dummy {
    use databend_common_expression::type_check;
    use databend_common_expression::DataBlock;
    use databend_common_expression::Evaluator;
    use databend_common_expression::FunctionContext;
    use databend_common_functions::test_utils as parser;
    use databend_common_functions::BUILTIN_FUNCTIONS;

    #[divan::bench(args = [10240, 102400])]
    fn parse(bencher: divan::Bencher, n: usize) {
        let text = "[".to_string() + &"true,".repeat(n) + "]";
        bencher.bench(|| {
            let _ = divan::black_box(parser::parse_raw_expr(&text, &[]));
        });
    }

    #[divan::bench(args = [10240, 102400])]
    fn check(bencher: divan::Bencher, n: usize) {
        let text = "[".to_string() + &"true,".repeat(n) + "]";
        let raw_expr = parser::parse_raw_expr(&text, &[]);

        bencher.bench(|| {
            let _ = divan::black_box(type_check::check(&raw_expr, &BUILTIN_FUNCTIONS));
        });
    }

    #[divan::bench(args = [10240, 102400])]
    fn eval(bencher: divan::Bencher, n: usize) {
        let text = "[".to_string() + &"true,".repeat(n) + "]";
        let raw_expr = parser::parse_raw_expr(&text, &[]);
        let func_ctx = FunctionContext::default();
        let expr = type_check::check(&raw_expr, &BUILTIN_FUNCTIONS).unwrap();
        let block = DataBlock::new(vec![], 1);
        let evaluator = Evaluator::new(&block, &func_ctx, &BUILTIN_FUNCTIONS);

        bencher.bench(|| {
            let _ = divan::black_box(evaluator.run(&expr));
        });
    }
}
