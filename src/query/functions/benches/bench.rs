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
    use databend_common_expression::DataBlock;
    use databend_common_expression::Evaluator;
    use databend_common_expression::FunctionContext;
    use databend_common_expression::type_check;
    use databend_common_expression_test_support as parser;
    use databend_common_functions::BUILTIN_FUNCTIONS;

    #[divan::bench(args = [10240, 102400])]
    fn parse(bencher: divan::Bencher, n: usize) {
        let text = "[".to_string() + &"true,".repeat(n) + "]";
        bencher.bench(|| {
            let _ = divan::black_box(parser::parse_raw_expr(&text, &[], &BUILTIN_FUNCTIONS));
        });
    }

    #[divan::bench(args = [10240, 102400])]
    fn check(bencher: divan::Bencher, n: usize) {
        let text = "[".to_string() + &"true,".repeat(n) + "]";
        let raw_expr = parser::parse_raw_expr(&text, &[], &BUILTIN_FUNCTIONS);

        bencher.bench(|| {
            let _ = divan::black_box(type_check::check(&raw_expr, &BUILTIN_FUNCTIONS));
        });
    }

    #[divan::bench(args = [10240, 102400])]
    fn eval(bencher: divan::Bencher, n: usize) {
        let text = "[".to_string() + &"true,".repeat(n) + "]";
        let raw_expr = parser::parse_raw_expr(&text, &[], &BUILTIN_FUNCTIONS);
        let func_ctx = FunctionContext::default();
        let expr = type_check::check(&raw_expr, &BUILTIN_FUNCTIONS).unwrap();
        let block = DataBlock::new(vec![], 1);
        let evaluator = Evaluator::new(&block, &func_ctx, &BUILTIN_FUNCTIONS);

        bencher.bench(|| {
            let _ = divan::black_box(evaluator.run(&expr));
        });
    }
}

#[divan::bench_group(max_time = 2)]
mod bitmap {
    use std::ops::BitAndAssign;
    use std::ops::BitOrAssign;
    use std::ops::BitXorAssign;
    use std::ops::SubAssign;

    use databend_common_expression::BlockEntry;
    use databend_common_expression::Column;
    use databend_common_expression::FromData;
    use databend_common_expression::types::BitmapType;
    use databend_common_expression::types::number::UInt64Type;
    use databend_common_functions::aggregates::eval_aggr;
    use databend_common_io::HybridBitmap;
    use rand::Rng;
    use rand::SeedableRng;
    use rand::rngs::SmallRng;

    fn create_bitmap(rng: &mut SmallRng) -> HybridBitmap {
        let mut bitmap = HybridBitmap::new();

        for _ in 0..20 {
            let v = rng.r#gen::<u64>();
            bitmap.insert(v & u16::MAX as u64);
        }

        if rng.r#gen::<u8>() % 4 != 0 {
            for _ in 0..50 {
                let v = rng.r#gen::<u64>();
                bitmap.insert(v);
            }

            for _ in 0..50 {
                let v = rng.r#gen::<u64>();
                bitmap.insert(v & u32::MAX as u64);
            }
        }

        bitmap
    }

    fn build_bitmap_column(rows: u64, seed: u64) -> Column {
        let mut rng = SmallRng::seed_from_u64(seed);
        let bitmaps = (0..rows)
            .map(|_| {
                let rb = create_bitmap(&mut rng);

                let mut data = Vec::new();
                rb.serialize_into(&mut data).unwrap();
                data
            })
            .collect();

        BitmapType::from_data(bitmaps)
    }

    fn build_disjoint_bitmap_column(rows: u64) -> Column {
        let bitmaps = (0..rows)
            .map(|number| {
                let mut rb = HybridBitmap::new();
                let base = number * 2;
                rb.insert(base);
                rb.insert(base + 1);

                let mut data = Vec::new();
                rb.serialize_into(&mut data).unwrap();
                data
            })
            .collect();

        BitmapType::from_data(bitmaps)
    }

    fn build_uint64_column<F>(rows: usize, generator: F) -> Column
    where F: FnMut(u64) -> u64 {
        let data: Vec<u64> = (0..rows as u64).map(generator).collect();
        UInt64Type::from_data(data)
    }

    fn mixed_small_large_pair() -> (HybridBitmap, HybridBitmap) {
        let small = HybridBitmap::from_iter([1_u64, 5, 13, 100]);
        let large = HybridBitmap::from_iter(0_u64..128);
        (small, large)
    }

    fn mixed_large_small_pair() -> (HybridBitmap, HybridBitmap) {
        let (small, large) = mixed_small_large_pair();
        (large, small)
    }

    fn eval_bitmap_result(entry: &BlockEntry, rows: usize, agg_name: &'static str) {
        let _ = eval_aggr(agg_name, vec![], std::slice::from_ref(entry), rows, vec![])
            .unwrap_or_else(|_| panic!("{agg_name} evaluation failed"));
    }

    #[divan::bench(args = [1000, 65535])]
    fn bitmap_intersect(bencher: divan::Bencher, rows: usize) {
        // Emulate `CREATE TABLE ... AS SELECT build_bitmap`
        // followed by `SELECT bitmap_intersect(a) FROM c`.
        let column = build_bitmap_column(rows as u64, 125);
        let entry = column.into();

        bencher.bench(|| {
            eval_bitmap_result(&entry, rows, "bitmap_intersect");
        });
    }

    #[divan::bench(args = [1000, 3000, 5000])]
    fn bitmap_union(bencher: divan::Bencher, rows: usize) {
        let column = build_bitmap_column(rows as u64, 785);
        let entry = column.into();

        bencher.bench(|| {
            eval_bitmap_result(&entry, rows, "bitmap_union");
        });
    }

    #[divan::bench(args = [1000, 3000, 5000])]
    fn bitmap_xor_agg_overlap(bencher: divan::Bencher, rows: usize) {
        let column = build_bitmap_column(rows as u64, 778);
        let entry = column.into();

        bencher.bench(|| {
            eval_bitmap_result(&entry, rows, "bitmap_xor_agg");
        });
    }

    #[divan::bench(args = [1000, 65535])]
    fn bitmap_not_count(bencher: divan::Bencher, rows: usize) {
        let column = build_bitmap_column(rows as u64, 125);
        let entry = column.into();

        bencher.bench(|| {
            eval_bitmap_result(&entry, rows, "bitmap_not_count");
        });
    }

    #[divan::bench(args = [100_000, 1_000_000])]
    fn bitmap_intersect_empty(bencher: divan::Bencher, rows: usize) {
        let column = build_disjoint_bitmap_column(rows as u64);
        let entry = column.into();

        bencher.bench(|| {
            eval_bitmap_result(&entry, rows, "bitmap_intersect");
        });
    }

    #[divan::bench(args = [100_000, 1_000_000])]
    fn bitmap_union_disjoint(bencher: divan::Bencher, rows: usize) {
        let column = build_disjoint_bitmap_column(rows as u64);
        let entry = column.into();

        bencher.bench(|| {
            eval_bitmap_result(&entry, rows, "bitmap_union");
        });
    }

    #[divan::bench(args = [100_000, 1_000_000])]
    fn bitmap_xor_agg(bencher: divan::Bencher, rows: usize) {
        let column = build_disjoint_bitmap_column(rows as u64);
        let entry = column.into();

        bencher.bench(|| {
            eval_bitmap_result(&entry, rows, "bitmap_xor_agg");
        });
    }

    #[divan::bench(args = [100_000, 1_000_000])]
    fn bitmap_construct_agg_dense(bencher: divan::Bencher, rows: usize) {
        let column = build_uint64_column(rows, |value| value);
        let entry = column.into();

        bencher.bench(|| {
            eval_bitmap_result(&entry, rows, "bitmap_construct_agg");
        });
    }

    #[divan::bench(args = [100_000, 1_000_000])]
    fn bitmap_construct_agg_repeating(bencher: divan::Bencher, rows: usize) {
        const CARDINALITY: u64 = 1024;
        let column = build_uint64_column(rows, |value| value % CARDINALITY);
        let entry = column.into();

        bencher.bench(|| {
            eval_bitmap_result(&entry, rows, "bitmap_construct_agg");
        });
    }

    #[divan::bench]
    fn bitmap_mixed_and_small_large(bencher: divan::Bencher) {
        bencher
            .with_inputs(mixed_small_large_pair)
            .bench_values(|(mut small, large)| {
                small.bitand_assign(large);
                small.len()
            });
    }

    #[divan::bench]
    fn bitmap_mixed_or_small_large(bencher: divan::Bencher) {
        bencher
            .with_inputs(mixed_small_large_pair)
            .bench_values(|(mut small, large)| {
                small.bitor_assign(large);
                small.len()
            });
    }

    #[divan::bench]
    fn bitmap_mixed_xor_small_large(bencher: divan::Bencher) {
        bencher
            .with_inputs(mixed_small_large_pair)
            .bench_values(|(mut small, large)| {
                small.bitxor_assign(large);
                small.len()
            });
    }

    #[divan::bench]
    fn bitmap_mixed_sub_small_large(bencher: divan::Bencher) {
        bencher
            .with_inputs(mixed_small_large_pair)
            .bench_values(|(mut small, large)| {
                small.sub_assign(large);
                small.len()
            });
    }

    #[divan::bench]
    fn bitmap_mixed_xor_large_small(bencher: divan::Bencher) {
        bencher
            .with_inputs(mixed_large_small_pair)
            .bench_values(|(mut large, small)| {
                large.bitxor_assign(small);
                large.len()
            });
    }

    #[divan::bench]
    fn bitmap_mixed_sub_large_small(bencher: divan::Bencher) {
        bencher
            .with_inputs(mixed_large_small_pair)
            .bench_values(|(mut large, small)| {
                large.sub_assign(small);
                large.len()
            });
    }
}

#[divan::bench_group(max_time = 0.5)]
mod bitmap_scalar {
    use databend_common_expression::BlockEntry;
    use databend_common_expression::DataBlock;
    use databend_common_expression::Evaluator;
    use databend_common_expression::Expr;
    use databend_common_expression::FromData;
    use databend_common_expression::FunctionContext;
    use databend_common_expression::type_check;
    use databend_common_expression::types::BitmapType;
    use databend_common_expression::types::DataType;
    use databend_common_expression_test_support as parser;
    use databend_common_functions::BUILTIN_FUNCTIONS;
    use databend_common_io::HybridBitmap;
    use rand::Rng;
    use rand::SeedableRng;
    use rand::rngs::SmallRng;

    fn serialize_bitmap(bitmap: &HybridBitmap) -> Vec<u8> {
        let mut data = Vec::new();
        bitmap.serialize_into(&mut data).unwrap();
        data
    }

    fn bitmap_entry(bitmap: &HybridBitmap) -> BlockEntry {
        BitmapType::from_data(vec![serialize_bitmap(bitmap); 1]).into()
    }

    fn build_expr(sql: &str, columns: &[(&str, DataType)]) -> Expr {
        let raw_expr = parser::parse_raw_expr(sql, columns, &BUILTIN_FUNCTIONS);
        type_check::check(&raw_expr, &BUILTIN_FUNCTIONS).unwrap()
    }

    fn eval_block(expr: &Expr, block: &DataBlock) {
        let func_ctx = FunctionContext::default();
        let evaluator = Evaluator::new(block, &func_ctx, &BUILTIN_FUNCTIONS);
        let result = evaluator.run(expr).unwrap();
        divan::black_box(result);
    }

    fn large_bitmap() -> HybridBitmap {
        let mut bm = HybridBitmap::new();
        for i in 0..10u64 {
            for v in i * 65536..i * 65536 + 5000 {
                bm.insert(v);
            }
        }
        for j in 10..50u64 {
            for v in j * 65536..j * 65536 + 100 {
                bm.insert(v);
            }
        }
        bm
    }

    fn overlap_large_bitmap() -> HybridBitmap {
        let mut bm = HybridBitmap::new();
        for i in 5..10u64 {
            for v in i * 65536 - 3000..i * 65536 + 5000 {
                bm.insert(v);
            }
        }
        for i in 10..50u64 {
            for v in i * 65536 - 30..i * 65536 + 50 {
                bm.insert(v);
            }
        }
        bm
    }

    fn subset_large_bitmap() -> HybridBitmap {
        let mut bm = HybridBitmap::new();
        for i in 0..5u64 {
            for v in i * 65536 + 1000..i * 65536 + 4000 {
                bm.insert(v);
            }
        }
        for j in 10..30u64 {
            for v in j * 65536 + 10..j * 65536 + 90 {
                bm.insert(v);
            }
        }
        bm
    }

    fn disjoint_large_bitmap() -> HybridBitmap {
        let mut bm = HybridBitmap::new();
        for i in 100..110u64 {
            for v in i * 65536..i * 65536 + 5000 {
                bm.insert(v);
            }
        }
        for j in 110..150u64 {
            for v in j * 65536..j * 65536 + 100 {
                bm.insert(v);
            }
        }

        bm
    }

    fn small_bitmap() -> HybridBitmap {
        HybridBitmap::from_iter(24 * 65536..24 * 65536 + 31)
    }

    fn overlap_small_bitmap() -> HybridBitmap {
        HybridBitmap::from_iter(24 * 65536 - 10..24 * 65536 + 21)
    }

    fn subset_small_bitmap() -> HybridBitmap {
        HybridBitmap::from_iter(24 * 65536 + 1..24 * 65536 + 30)
    }

    const C1: &[(&str, DataType)] = &[("a", DataType::Bitmap)];
    const C2: &[(&str, DataType)] = &[("a", DataType::Bitmap), ("b", DataType::Bitmap)];

    #[divan::bench]
    fn bitmap_contains_large(bencher: divan::Bencher) {
        let bm = large_bitmap();
        let block = DataBlock::new(vec![bitmap_entry(&bm)], 1);
        let expr = build_expr("bitmap_contains(a, 5*65536+2500)", C1);
        bencher.bench(|| eval_block(&expr, &block));
    }

    #[divan::bench]
    fn bitmap_contains_small(bencher: divan::Bencher) {
        let bm = small_bitmap();
        let block = DataBlock::new(vec![bitmap_entry(&bm)], 1);
        let expr = build_expr("bitmap_contains(a, 15)", C1);
        bencher.bench(|| eval_block(&expr, &block));
    }

    #[divan::bench]
    fn bitmap_count_large(bencher: divan::Bencher) {
        let bm = large_bitmap();
        let block = DataBlock::new(vec![bitmap_entry(&bm)], 1);
        let expr = build_expr("bitmap_count(a)", C1);
        bencher.bench(|| eval_block(&expr, &block));
    }

    #[divan::bench]
    fn bitmap_count_small(bencher: divan::Bencher) {
        let bm = small_bitmap();
        let block = DataBlock::new(vec![bitmap_entry(&bm)], 1);
        let expr = build_expr("bitmap_count(a)", C1);
        bencher.bench(|| eval_block(&expr, &block));
    }

    // bitmap_min
    #[divan::bench]
    fn bitmap_min_large(bencher: divan::Bencher) {
        let bm = large_bitmap();
        let block = DataBlock::new(vec![bitmap_entry(&bm)], 1);
        let expr = build_expr("bitmap_min(a)", C1);
        bencher.bench(|| eval_block(&expr, &block));
    }

    #[divan::bench]
    fn bitmap_min_small(bencher: divan::Bencher) {
        let bm = small_bitmap();
        let block = DataBlock::new(vec![bitmap_entry(&bm)], 1);
        let expr = build_expr("bitmap_min(a)", C1);
        bencher.bench(|| eval_block(&expr, &block));
    }

    #[divan::bench]
    fn bitmap_max_large(bencher: divan::Bencher) {
        let bm = large_bitmap();
        let block = DataBlock::new(vec![bitmap_entry(&bm)], 1);
        let expr = build_expr("bitmap_max(a)", C1);
        bencher.bench(|| eval_block(&expr, &block));
    }

    #[divan::bench]
    fn bitmap_max_small(bencher: divan::Bencher) {
        let bm = small_bitmap();
        let block = DataBlock::new(vec![bitmap_entry(&bm)], 1);
        let expr = build_expr("bitmap_max(a)", C1);
        bencher.bench(|| eval_block(&expr, &block));
    }

    fn bitmap_workload(lhs_n: u32, rhs_n: u32, shared: u32) -> (HybridBitmap, HybridBitmap) {
        let lhs_only = lhs_n - shared;
        let rhs_only = rhs_n - shared;
        let mut rng = SmallRng::seed_from_u64(42);

        fn fill(bm: &mut HybridBitmap, prefix: u32, rng: &mut SmallRng) {
            let card = rng.gen_range(100..=10000);
            for _ in 0..card {
                bm.insert((prefix as u64) << 32 | rng.r#gen::<u32>() as u64);
            }
        }

        let mut lhs = HybridBitmap::new();
        let mut rhs = HybridBitmap::new();
        for p in 0..shared {
            fill(&mut lhs, p, &mut rng);
            fill(&mut rhs, p, &mut rng);
        }
        for i in 0..lhs_only {
            fill(&mut lhs, shared + i, &mut rng);
        }
        for i in 0..rhs_only {
            fill(&mut rhs, shared + lhs_only + i, &mut rng);
        }
        (lhs, rhs)
    }

    #[derive(Clone, Copy, Debug)]
    enum BitmapLargeCase {
        OrHighOverlap,
        OrLowOverlap,
        OrLhsLarger,
        OrRhsLarger,
        AndHighOverlap,
        AndLowOverlap,
        AndLhsLarger,
        AndRhsLarger,
        XorHighOverlap,
        XorLowOverlap,
        XorLhsLarger,
        XorRhsLarger,
        NotHighOverlap,
        NotLowOverlap,
        NotLhsLarger,
        NotRhsLarger,
    }

    impl BitmapLargeCase {
        const ALL: &[BitmapLargeCase] = &[
            BitmapLargeCase::OrHighOverlap,
            BitmapLargeCase::OrLowOverlap,
            BitmapLargeCase::OrLhsLarger,
            BitmapLargeCase::OrRhsLarger,
            BitmapLargeCase::AndHighOverlap,
            BitmapLargeCase::AndLowOverlap,
            BitmapLargeCase::AndLhsLarger,
            BitmapLargeCase::AndRhsLarger,
            BitmapLargeCase::XorHighOverlap,
            BitmapLargeCase::XorLowOverlap,
            BitmapLargeCase::XorLhsLarger,
            BitmapLargeCase::XorRhsLarger,
            BitmapLargeCase::NotHighOverlap,
            BitmapLargeCase::NotLowOverlap,
            BitmapLargeCase::NotLhsLarger,
            BitmapLargeCase::NotRhsLarger,
        ];

        fn sql(self) -> &'static str {
            match self {
                BitmapLargeCase::OrHighOverlap
                | BitmapLargeCase::OrLowOverlap
                | BitmapLargeCase::OrLhsLarger
                | BitmapLargeCase::OrRhsLarger => "bitmap_or(a, b)",
                BitmapLargeCase::AndHighOverlap
                | BitmapLargeCase::AndLowOverlap
                | BitmapLargeCase::AndLhsLarger
                | BitmapLargeCase::AndRhsLarger => "bitmap_and(a, b)",
                BitmapLargeCase::XorHighOverlap
                | BitmapLargeCase::XorLowOverlap
                | BitmapLargeCase::XorLhsLarger
                | BitmapLargeCase::XorRhsLarger => "bitmap_xor(a, b)",
                BitmapLargeCase::NotHighOverlap
                | BitmapLargeCase::NotLowOverlap
                | BitmapLargeCase::NotLhsLarger
                | BitmapLargeCase::NotRhsLarger => "bitmap_not(a, b)",
            }
        }

        fn shape(self) -> (u32, u32, u32) {
            match self {
                BitmapLargeCase::OrHighOverlap
                | BitmapLargeCase::AndHighOverlap
                | BitmapLargeCase::XorHighOverlap
                | BitmapLargeCase::NotHighOverlap => (64, 64, 56),
                BitmapLargeCase::OrLowOverlap
                | BitmapLargeCase::AndLowOverlap
                | BitmapLargeCase::XorLowOverlap
                | BitmapLargeCase::NotLowOverlap => (64, 64, 8),
                BitmapLargeCase::OrLhsLarger
                | BitmapLargeCase::AndLhsLarger
                | BitmapLargeCase::XorLhsLarger
                | BitmapLargeCase::NotLhsLarger => (256, 32, 32),
                BitmapLargeCase::OrRhsLarger
                | BitmapLargeCase::AndRhsLarger
                | BitmapLargeCase::XorRhsLarger
                | BitmapLargeCase::NotRhsLarger => (32, 256, 32),
            }
        }
    }

    #[divan::bench(args = BitmapLargeCase::ALL)]
    fn bitmap_op_large(bencher: divan::Bencher, case: BitmapLargeCase) {
        let (l, r, s) = case.shape();
        let (a, b) = bitmap_workload(l, r, s);
        let block = DataBlock::new(vec![bitmap_entry(&a), bitmap_entry(&b)], 1);
        let expr = build_expr(case.sql(), C2);
        bencher.bench(|| eval_block(&expr, &block));
    }

    #[derive(Clone, Copy, Debug)]
    enum BitmapMixedCase {
        OrLargeSmall,
        OrSmallLarge,
        OrSmallSmall,
        AndLargeSmall,
        AndSmallLarge,
        AndSmallSmall,
        XorLargeSmall,
        XorSmallLarge,
        XorSmallSmall,
        NotLargeSmall,
        NotSmallLarge,
        NotSmallSmall,
    }

    impl BitmapMixedCase {
        const ALL: &[BitmapMixedCase] = &[
            BitmapMixedCase::OrLargeSmall,
            BitmapMixedCase::OrSmallLarge,
            BitmapMixedCase::OrSmallSmall,
            BitmapMixedCase::AndLargeSmall,
            BitmapMixedCase::AndSmallLarge,
            BitmapMixedCase::AndSmallSmall,
            BitmapMixedCase::XorLargeSmall,
            BitmapMixedCase::XorSmallLarge,
            BitmapMixedCase::XorSmallSmall,
            BitmapMixedCase::NotLargeSmall,
            BitmapMixedCase::NotSmallLarge,
            BitmapMixedCase::NotSmallSmall,
        ];

        fn sql(self) -> &'static str {
            match self {
                BitmapMixedCase::OrLargeSmall
                | BitmapMixedCase::OrSmallLarge
                | BitmapMixedCase::OrSmallSmall => "bitmap_or(a, b)",
                BitmapMixedCase::AndLargeSmall
                | BitmapMixedCase::AndSmallLarge
                | BitmapMixedCase::AndSmallSmall => "bitmap_and(a, b)",
                BitmapMixedCase::XorLargeSmall
                | BitmapMixedCase::XorSmallLarge
                | BitmapMixedCase::XorSmallSmall => "bitmap_xor(a, b)",
                BitmapMixedCase::NotLargeSmall
                | BitmapMixedCase::NotSmallLarge
                | BitmapMixedCase::NotSmallSmall => "bitmap_not(a, b)",
            }
        }

        fn pair(self) -> (HybridBitmap, HybridBitmap) {
            let (small, large) = (small_bitmap(), large_bitmap());
            match self {
                BitmapMixedCase::OrLargeSmall
                | BitmapMixedCase::AndLargeSmall
                | BitmapMixedCase::XorLargeSmall
                | BitmapMixedCase::NotLargeSmall => (large, small),
                BitmapMixedCase::OrSmallLarge
                | BitmapMixedCase::AndSmallLarge
                | BitmapMixedCase::XorSmallLarge
                | BitmapMixedCase::NotSmallLarge => (small, large),
                BitmapMixedCase::OrSmallSmall
                | BitmapMixedCase::AndSmallSmall
                | BitmapMixedCase::XorSmallSmall
                | BitmapMixedCase::NotSmallSmall => (small_bitmap(), overlap_small_bitmap()),
            }
        }
    }

    #[divan::bench(args = BitmapMixedCase::ALL)]
    fn bitmap_op_mixed(bencher: divan::Bencher, case: BitmapMixedCase) {
        let (a, b) = case.pair();
        let block = DataBlock::new(vec![bitmap_entry(&a), bitmap_entry(&b)], 1);
        let expr = build_expr(case.sql(), C2);
        bencher.bench(|| eval_block(&expr, &block));
    }

    #[divan::bench]
    fn bitmap_has_any_large_large(bencher: divan::Bencher) {
        let a = large_bitmap();
        let b = overlap_large_bitmap();
        let block = DataBlock::new(vec![bitmap_entry(&a), bitmap_entry(&b)], 1);
        let expr = build_expr("bitmap_has_any(a, b)", C2);
        bencher.bench(|| eval_block(&expr, &block));
    }

    #[divan::bench]
    fn bitmap_has_any_small_small(bencher: divan::Bencher) {
        let a = small_bitmap();
        let b = overlap_small_bitmap();
        let block = DataBlock::new(vec![bitmap_entry(&a), bitmap_entry(&b)], 1);
        let expr = build_expr("bitmap_has_any(a, b)", C2);
        bencher.bench(|| eval_block(&expr, &block));
    }

    #[divan::bench]
    fn bitmap_has_any_large_small(bencher: divan::Bencher) {
        let a = large_bitmap();
        let b = small_bitmap();
        let block = DataBlock::new(vec![bitmap_entry(&a), bitmap_entry(&b)], 1);
        let expr = build_expr("bitmap_has_any(a, b)", C2);
        bencher.bench(|| eval_block(&expr, &block));
    }

    #[divan::bench]
    fn bitmap_has_any_small_large(bencher: divan::Bencher) {
        let a = small_bitmap();
        let b = large_bitmap();
        let block = DataBlock::new(vec![bitmap_entry(&a), bitmap_entry(&b)], 1);
        let expr = build_expr("bitmap_has_any(a, b)", C2);
        bencher.bench(|| eval_block(&expr, &block));
    }

    #[divan::bench]
    fn bitmap_has_any_large_large_disjoint(bencher: divan::Bencher) {
        let a = large_bitmap();
        let b = disjoint_large_bitmap();
        let block = DataBlock::new(vec![bitmap_entry(&a), bitmap_entry(&b)], 1);
        let expr = build_expr("bitmap_has_any(a, b)", C2);
        bencher.bench(|| eval_block(&expr, &block));
    }

    #[divan::bench]
    fn bitmap_has_all_large_large(bencher: divan::Bencher) {
        let a = large_bitmap();
        let b = subset_large_bitmap();
        let block = DataBlock::new(vec![bitmap_entry(&a), bitmap_entry(&b)], 1);
        let expr = build_expr("bitmap_has_all(a, b)", C2);
        bencher.bench(|| eval_block(&expr, &block));
    }

    #[divan::bench]
    fn bitmap_has_all_small_small(bencher: divan::Bencher) {
        let a = small_bitmap();
        let b = subset_small_bitmap();
        let block = DataBlock::new(vec![bitmap_entry(&a), bitmap_entry(&b)], 1);
        let expr = build_expr("bitmap_has_all(a, b)", C2);
        bencher.bench(|| eval_block(&expr, &block));
    }

    #[divan::bench]
    fn bitmap_has_all_large_small(bencher: divan::Bencher) {
        let a = large_bitmap();
        let b = small_bitmap();
        let block = DataBlock::new(vec![bitmap_entry(&a), bitmap_entry(&b)], 1);
        let expr = build_expr("bitmap_has_all(a, b)", C2);
        bencher.bench(|| eval_block(&expr, &block));
    }

    #[divan::bench]
    fn bitmap_has_all_small_large(bencher: divan::Bencher) {
        let a = small_bitmap();
        let b = large_bitmap();
        let block = DataBlock::new(vec![bitmap_entry(&a), bitmap_entry(&b)], 1);
        let expr = build_expr("bitmap_has_all(a, b)", C2);
        bencher.bench(|| eval_block(&expr, &block));
    }

    #[divan::bench]
    fn bitmap_has_all_large_large_disjoint(bencher: divan::Bencher) {
        let a = large_bitmap();
        let b = disjoint_large_bitmap();
        let block = DataBlock::new(vec![bitmap_entry(&a), bitmap_entry(&b)], 1);
        let expr = build_expr("bitmap_has_all(a, b)", C2);
        bencher.bench(|| eval_block(&expr, &block));
    }
}

#[divan::bench_group(max_time = 0.5)]
mod datetime_fast_path {
    use std::sync::LazyLock;

    use databend_common_expression::BlockEntry;
    use databend_common_expression::Column;
    use databend_common_expression::DataBlock;
    use databend_common_expression::Evaluator;
    use databend_common_expression::Expr;
    use databend_common_expression::FunctionContext;
    use databend_common_expression::type_check;
    use databend_common_expression::types::DataType;
    use databend_common_expression::types::string::StringColumn;
    use databend_common_expression::types::string::StringColumnBuilder;
    use databend_common_expression::types::timestamp::microseconds_to_days;
    use databend_common_expression::types::timestamp::timestamp_from_micros;
    use databend_common_expression::types::timestamp::timestamp_to_string;
    use databend_common_expression_test_support as parser;
    use databend_common_functions::BUILTIN_FUNCTIONS;
    use jiff::civil::date;
    use jiff::tz::TimeZone;
    use rand::Rng;
    use rand::SeedableRng;
    use rand::rngs::StdRng;

    const ROWS: usize = 100_000;
    const SPECIAL_EVERY: usize = 20_000;

    static SAMPLES: LazyLock<DateTimeSamples> =
        LazyLock::new(|| DateTimeSamples::new(ROWS, SPECIAL_EVERY));

    struct DateTimeSamples {
        timestamps: databend_common_column::buffer::Buffer<i64>,
        dates: databend_common_column::buffer::Buffer<i32>,
        timestamp_strings: StringColumn,
        standard_timestamp_strings: StringColumn,
    }

    impl DateTimeSamples {
        fn new(rows: usize, interval: usize) -> Self {
            let timestamps = generate_timestamp_values(rows, interval);
            let dates: Vec<i32> = timestamps
                .iter()
                .map(|&micros| microseconds_to_days(micros))
                .collect();
            let tz_sh = TimeZone::get("Asia/Shanghai").unwrap();
            let mut string_builder = StringColumnBuilder::with_capacity(rows);
            let mut standard_builder = StringColumnBuilder::with_capacity(rows);
            for &micros in timestamps.iter() {
                let formatted = timestamp_to_string(micros, &tz_sh).to_string();
                string_builder.put_and_commit(formatted);

                let zoned = timestamp_from_micros(micros, &tz_sh);
                let offset_secs = zoned.offset().seconds();
                let offset_hours = offset_secs / 3600;
                let offset_minutes = (offset_secs.abs() % 3600) / 60;
                let standard = format!(
                    "{:04}-{:02}-{:02} {:02}:{:02}:{:02}.{:06}{:+03}:{:02}",
                    zoned.date().year(),
                    zoned.date().month(),
                    zoned.date().day(),
                    zoned.time().hour(),
                    zoned.time().minute(),
                    zoned.time().second(),
                    zoned.time().nanosecond() / 1_000,
                    offset_hours,
                    offset_minutes
                );
                standard_builder.put_and_commit(standard);
            }
            Self {
                timestamps: timestamps.into(),
                dates: dates.into(),
                timestamp_strings: string_builder.build(),
                standard_timestamp_strings: standard_builder.build(),
            }
        }

        fn rows(&self) -> usize {
            self.timestamps.len()
        }

        fn timestamp_entry(&self) -> BlockEntry {
            BlockEntry::Column(Column::Timestamp(self.timestamps.clone()))
        }

        fn date_entry(&self) -> BlockEntry {
            BlockEntry::Column(Column::Date(self.dates.clone()))
        }

        fn string_entry(&self) -> BlockEntry {
            BlockEntry::Column(Column::String(self.timestamp_strings.clone()))
        }

        fn standard_string_entry(&self) -> BlockEntry {
            BlockEntry::Column(Column::String(self.standard_timestamp_strings.clone()))
        }
    }

    #[divan::bench]
    fn timestamp_extract_components(bencher: divan::Bencher) {
        let expr = build_expr(
            "tuple(to_year(ts), to_month(ts), to_day_of_year(ts), to_hour(ts))",
            &[("ts", DataType::Timestamp)],
        );
        let data = &*SAMPLES;
        let block = DataBlock::new(vec![data.timestamp_entry()], data.rows());
        let func_ctx = FunctionContext {
            tz: TimeZone::get("Asia/Shanghai").unwrap(),
            ..Default::default()
        };
        let evaluator = Evaluator::new(&block, &func_ctx, &BUILTIN_FUNCTIONS);

        bencher.bench(|| {
            let value = evaluator.run(&expr).unwrap();
            divan::black_box(value);
        });
    }

    #[divan::bench]
    fn timestamp_add_months(bencher: divan::Bencher) {
        let expr = build_expr("add_months(ts, 1)", &[("ts", DataType::Timestamp)]);
        let data = &*SAMPLES;
        let block = DataBlock::new(vec![data.timestamp_entry()], data.rows());
        let func_ctx = FunctionContext {
            tz: TimeZone::get("Asia/Shanghai").unwrap(),
            ..Default::default()
        };
        let evaluator = Evaluator::new(&block, &func_ctx, &BUILTIN_FUNCTIONS);

        bencher.bench(|| {
            let value = evaluator.run(&expr).unwrap();
            divan::black_box(value);
        });
    }

    #[divan::bench]
    fn date_add_days(bencher: divan::Bencher) {
        let expr = build_expr("add_days(d, 7)", &[("d", DataType::Date)]);
        let data = &*SAMPLES;
        let block = DataBlock::new(vec![data.date_entry()], data.rows());
        let func_ctx = FunctionContext {
            tz: TimeZone::get("Asia/Shanghai").unwrap(),
            ..Default::default()
        };
        let evaluator = Evaluator::new(&block, &func_ctx, &BUILTIN_FUNCTIONS);

        bencher.bench(|| {
            let value = evaluator.run(&expr).unwrap();
            divan::black_box(value);
        });
    }

    #[divan::bench]
    fn string_parse_to_date(bencher: divan::Bencher) {
        let expr = build_expr("to_date(to_timestamp(s))", &[("s", DataType::String)]);
        let data = &*SAMPLES;
        let block = DataBlock::new(vec![data.string_entry()], data.rows());
        let func_ctx = FunctionContext {
            tz: TimeZone::get("Asia/Shanghai").unwrap(),
            ..Default::default()
        };
        let evaluator = Evaluator::new(&block, &func_ctx, &BUILTIN_FUNCTIONS);

        bencher.bench(|| {
            let value = evaluator.run(&expr).unwrap();
            divan::black_box(value);
        });
    }

    #[divan::bench]
    fn string_parse_standard_to_date(bencher: divan::Bencher) {
        let expr = build_expr("to_date(to_timestamp(s))", &[("s", DataType::String)]);
        let data = &*SAMPLES;
        let block = DataBlock::new(vec![data.standard_string_entry()], data.rows());
        let func_ctx = FunctionContext {
            tz: TimeZone::get("Asia/Shanghai").unwrap(),
            ..Default::default()
        };
        let evaluator = Evaluator::new(&block, &func_ctx, &BUILTIN_FUNCTIONS);

        bencher.bench(|| {
            let value = evaluator.run(&expr).unwrap();
            divan::black_box(value);
        });
    }

    #[divan::bench]
    fn string_parse_to_timestamptz(bencher: divan::Bencher) {
        let expr = build_expr("to_timestamp_tz(s)", &[("s", DataType::String)]);
        let data = &*SAMPLES;
        let block = DataBlock::new(vec![data.standard_string_entry()], data.rows());
        let func_ctx = FunctionContext {
            tz: TimeZone::get("Asia/Shanghai").unwrap(),
            ..Default::default()
        };
        let evaluator = Evaluator::new(&block, &func_ctx, &BUILTIN_FUNCTIONS);

        bencher.bench(|| {
            let value = evaluator.run(&expr).unwrap();
            divan::black_box(value);
        });
    }

    #[divan::bench]
    fn convert_timezone(bencher: divan::Bencher) {
        let expr = build_expr("convert_timezone('America/Los_Angeles', ts)", &[(
            "ts",
            DataType::Timestamp,
        )]);
        let data = &*SAMPLES;
        let block = DataBlock::new(vec![data.timestamp_entry()], data.rows());
        let func_ctx = FunctionContext {
            tz: TimeZone::get("Asia/Shanghai").unwrap(),
            ..Default::default()
        };
        let evaluator = Evaluator::new(&block, &func_ctx, &BUILTIN_FUNCTIONS);

        bencher.bench(|| {
            let value = evaluator.run(&expr).unwrap();
            divan::black_box(value);
        });
    }

    fn build_expr(sql: &str, columns: &[(&str, DataType)]) -> Expr {
        let raw_expr = parser::parse_raw_expr(sql, columns, &BUILTIN_FUNCTIONS);
        type_check::check(&raw_expr, &BUILTIN_FUNCTIONS).unwrap()
    }

    fn generate_timestamp_values(rows: usize, interval: usize) -> Vec<i64> {
        let tz_sh = TimeZone::get("Asia/Shanghai").unwrap();
        let tz_alg = TimeZone::get("Africa/Algiers").unwrap();
        let specials = [
            local_micros(&tz_sh, 1941, 3, 14, 23, 55, 0),
            local_micros(&tz_sh, 1941, 3, 15, 1, 5, 0),
            local_micros(&tz_sh, 1941, 11, 1, 0, 30, 0),
            local_micros(&tz_sh, 1941, 11, 1, 1, 30, 0),
            local_micros(&tz_alg, 1939, 11, 18, 23, 30, 0),
            local_micros(&tz_alg, 1939, 11, 19, 0, 0, 30),
        ];

        let mut rng = StdRng::seed_from_u64(0x5453_5450);
        let mut values = Vec::with_capacity(rows);
        for i in 0..rows {
            if (i % interval) < specials.len() {
                values.push(specials[i % specials.len()]);
            } else {
                let secs = rng.gen_range(-2_208_988_800_i64..4_102_444_800_i64);
                let micros = secs * 1_000_000 + rng.gen_range(0..1_000_000) as i64;
                values.push(micros);
            }
        }
        values
    }

    fn local_micros(
        tz: &TimeZone,
        year: i32,
        month: u8,
        day: u8,
        hour: u8,
        minute: u8,
        second: u8,
    ) -> i64 {
        let dt =
            date(year as i16, month as i8, day as i8).at(hour as i8, minute as i8, second as i8, 0);
        tz.to_ambiguous_zoned(dt)
            .later()
            .unwrap()
            .timestamp()
            .as_microsecond()
    }
}
