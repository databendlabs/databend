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
    use databend_common_expression::types::BitmapType;
    use databend_common_expression::BlockEntry;
    use databend_common_expression::Column;
    use databend_common_expression::DataBlock;
    use databend_common_expression::Evaluator;
    use databend_common_expression::FromData;
    use databend_common_expression::FunctionContext;
    use databend_common_functions::aggregates::eval_aggr_for_test;
    use databend_common_functions::test_utils as parser;
    use databend_common_functions::BUILTIN_FUNCTIONS;
    use databend_common_io::deserialize_bitmap;
    use databend_common_io::HybridBitmap;

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

    fn build_bitmap_column(rows: u64) -> Column {
        let bitmaps = (0..rows)
            .map(|number| {
                let mut rb = HybridBitmap::new();
                rb.insert(1);
                rb.insert(number % 3);
                rb.insert(number % 5);

                let mut data = Vec::new();
                rb.serialize_into(&mut data).unwrap();
                data
            })
            .collect();

        BitmapType::from_data(bitmaps)
    }

    #[divan::bench(args = [100_000, 10_000_000])]
    fn bitmap_intersect(bencher: divan::Bencher, rows: usize) {
        // Emulate `CREATE TABLE ... AS SELECT build_bitmap`
        // followed by `SELECT bitmap_intersect(a) FROM c`.
        let column = build_bitmap_column(rows as u64);
        let entry: BlockEntry = column.into();

        bencher.bench(|| {
            let (result_column, _) = eval_aggr_for_test(
                "bitmap_intersect",
                vec![],
                std::slice::from_ref(&entry),
                rows,
                false,
                vec![],
            )
            .expect("bitmap_intersect evaluation");

            let Column::Bitmap(result) = result_column.remove_nullable() else {
                panic!("bitmap_intersect should return a Bitmap column");
            };
            let Some(bytes) = result.index(0) else {
                panic!("result should contain exactly one row");
            };
            let rb = deserialize_bitmap(bytes).expect("deserialize bitmap result");
            assert_eq!(rb.len(), 1);
            assert!(rb.contains(1));
        });
    }
}
