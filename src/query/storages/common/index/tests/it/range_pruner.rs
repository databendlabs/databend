// Copyright 2022 Datafuse Labs.
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

use std::io::Write;
use std::sync::Arc;

use databend_common_exception::Result;
use databend_common_expression::types::ArgType;
use databend_common_expression::types::Int32Type;
use databend_common_expression::types::NumberDataType;
use databend_common_expression::types::DataType;
use databend_common_expression::FunctionContext;
use databend_common_expression::Scalar;
use databend_common_expression::TableDataType;
use databend_common_expression::TableField;
use databend_common_expression::TableSchema;
use databend_storages_common_index::RangeIndex;
use databend_storages_common_table_meta::meta::ColumnStatistics;
use databend_storages_common_table_meta::meta::StatisticsOfColumns;
use goldenfile::Mint;

use super::eliminate_cast::parse_expr;

#[test]
fn test_range_index() -> Result<()> {
    let mut mint = Mint::new("tests/it/testdata");
    let file = &mut mint.new_goldenfile("test_range_indexs.txt").unwrap();

    fn n(n: i32) -> Scalar {
        Scalar::Number(n.into())
    }

    let domains = &[("a", n(-2), n(3)), ("b", n(-2), n(3))];

    run_text(file, "a = 2 and b = 41", domains);
    run_text(file, "a = 2 and b < 7", domains);

    // test not keep
    run_text(file, "a > 2 and a < 1", domains);
    run_text(file, "a = 2 and a < 1", domains);
    run_text(file, "a < 2 and a > 2", domains);

    // test not keep - more mutual exclusion cases
    run_text(file, "a >= 3 and a < 2", domains);
    run_text(file, "a <= 1 and a > 3", domains);
    run_text(file, "a = 5 and a = 7", domains);
    run_text(file, "a > 10 and a < 5", domains);
    run_text(file, "a >= 5 and a <= 2", domains);

    // test keep - non-mutual exclusion cases
    run_text(file, "a > 1 and a < 3", domains);
    run_text(file, "a >= 2 and a <= 2", domains);
    run_text(file, "a >= 1 and a < 3", domains);
    run_text(file, "a > 1 and a <= 2", domains);

    // test complex expressions with multiple columns
    run_text(file, "a > 2 and a < 1 and b = 1", domains);
    run_text(file, "a = 2 and b > 5 and b < 3", domains);

    // test edge cases
    run_text(file, "a > 2 and a <= 2", domains);
    run_text(file, "a < 2 and a >= 2", domains);

    run_text(file, "to_string(a) = '4'", domains);
    run_text(file, "to_string(a) = 'a'", domains);
    run_text(file, "to_int8(a) = 3", domains);
    run_text(file, "to_int8(a) = 4", domains);
    run_text(file, "to_uint8(a) = 3::int64", domains);
    run_text(file, "to_uint8(a) = 4::int64", domains);
    run_text(file, "to_int16(a::int8) = 3", domains);
    run_text(file, "to_int16(a::int8) = 4", domains);

    Ok(())
}

#[test]
fn test_range_index_dates() -> Result<()> {
    let mut mint = Mint::new("tests/it/testdata");
    let file = &mut mint.new_goldenfile("test_range_index_dates.txt").unwrap();

    // Date scalar helper
    fn date_scalar(days: i32) -> Scalar {
        Scalar::Date(days)
    }

    // Timestamp scalar helper
    fn timestamp_scalar(micros: i64) -> Scalar {
        Scalar::Timestamp(micros)
    }

    // Test date ranges
    let date_domains = &[
        ("dt", date_scalar(18000), date_scalar(18010)), // ~2019-04-26 to 2019-05-06
    ];

    run_text_with_schema(
        file,
        "dt > '2019-05-01' and dt < '2019-04-30'",
        date_domains,
        vec![TableField::new("dt", TableDataType::Date)]
    );

    run_text_with_schema(
        file,
        "dt = '2019-05-01' and dt < '2019-04-30'",
        date_domains,
        vec![TableField::new("dt", TableDataType::Date)]
    );

    run_text_with_schema(
        file,
        "dt >= '2019-05-02' and dt <= '2019-05-01'",
        date_domains,
        vec![TableField::new("dt", TableDataType::Date)]
    );

    // Non-mutual exclusion cases
    run_text_with_schema(
        file,
        "dt >= '2019-04-28' and dt <= '2019-05-03'",
        date_domains,
        vec![TableField::new("dt", TableDataType::Date)]
    );

    // Test timestamp ranges
    let ts_domains = &[
        ("ts", timestamp_scalar(1556668800000000), timestamp_scalar(1556755200000000)), // 2019-05-01 to 2019-05-02
    ];

    run_text_with_schema(
        file,
        "ts > '2019-05-01 12:00:00' and ts < '2019-05-01 06:00:00'",
        ts_domains,
        vec![TableField::new("ts", TableDataType::Timestamp)]
    );

    run_text_with_schema(
        file,
        "ts = '2019-05-01 12:00:00' and ts != '2019-05-01 12:00:00'",
        ts_domains,
        vec![TableField::new("ts", TableDataType::Timestamp)]
    );

    Ok(())
}

fn run_text_with_schema(
    file: &mut impl Write,
    text: &str,
    domains: &[(&str, Scalar, Scalar)],
    fields: Vec<TableField>
) {
    let func_ctx = FunctionContext::default();
    let schema = Arc::new(TableSchema::new(fields.clone()));
    let stats = create_stats(domains, &schema);

    let columns: Vec<(&str, DataType)> = fields.iter()
        .map(|f| (f.name().as_str(), f.data_type().into()))
        .collect();

    let expr = parse_expr(text, &columns);
    let index = RangeIndex::try_create(func_ctx, &expr, schema, Default::default()).unwrap();

    writeln!(file, "text      : {text}").unwrap();
    writeln!(file, "expr      : {expr}").unwrap();

    match index.apply(&stats, |_| false) {
        Err(err) => {
            writeln!(file, "err       : {err}").unwrap();
        }
        Ok(keep) => {
            writeln!(file, "keep      : {keep}").unwrap();
        }
    };
    writeln!(file).unwrap();
}

fn create_stats(domains: &[(&str, Scalar, Scalar)], schema: &TableSchema) -> StatisticsOfColumns {
    domains
        .iter()
        .map(|(name, min, max)| {
            (
                schema.leaf_columns_of(&name.to_string())[0],
                ColumnStatistics {
                    min: min.clone(),
                    max: max.clone(),
                    null_count: 0,
                    in_memory_size: 1000,
                    distinct_of_values: None,
                },
            )
        })
        .collect()
}

fn run_text(file: &mut impl Write, text: &str, domains: &[(&str, Scalar, Scalar)]) {
    let func_ctx = FunctionContext::default();

    let schema = Arc::new(TableSchema::new(vec![
        TableField::new("a", TableDataType::Number(NumberDataType::Int32)),
        TableField::new("b", TableDataType::Number(NumberDataType::Int32)),
    ]));

    let stats = create_stats(domains, &schema);

    let columns = [("a", Int32Type::data_type()), ("b", Int32Type::data_type())];
    let expr = parse_expr(text, &columns);

    let index = RangeIndex::try_create(func_ctx, &expr, schema, Default::default()).unwrap();

    writeln!(file, "text      : {text}").unwrap();
    writeln!(file, "expr      : {expr}").unwrap();

    match index.apply(&stats, |_| false) {
        Err(err) => {
            writeln!(file, "err       : {err}").unwrap();
        }

        Ok(keep) => {
            writeln!(file, "keep      : {keep}").unwrap();
        }
    };
    writeln!(file).unwrap();
}
