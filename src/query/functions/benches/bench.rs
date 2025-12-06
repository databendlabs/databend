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

#[divan::bench_group(max_time = 0.5)]
mod datetime_fast_path {
    use std::sync::LazyLock;

    use databend_common_expression::date_helper::DateConverter;
    use databend_common_expression::type_check;
    use databend_common_expression::types::string::StringColumn;
    use databend_common_expression::types::string::StringColumnBuilder;
    use databend_common_expression::types::timestamp::microseconds_to_days;
    use databend_common_expression::types::timestamp::timestamp_to_string;
    use databend_common_expression::types::DataType;
    use databend_common_expression::BlockEntry;
    use databend_common_expression::Column;
    use databend_common_expression::DataBlock;
    use databend_common_expression::Evaluator;
    use databend_common_expression::Expr;
    use databend_common_expression::FunctionContext;
    use databend_common_functions::test_utils as parser;
    use databend_common_functions::BUILTIN_FUNCTIONS;
    use jiff::civil::date;
    use jiff::tz::TimeZone;
    use rand::rngs::StdRng;
    use rand::Rng;
    use rand::SeedableRng;

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

                let zoned = micros.to_timestamp(&tz_sh);
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
        let raw_expr = parser::parse_raw_expr(sql, columns);
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
