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

use std::collections::HashMap;
use std::io::Write;
use std::sync::Arc;

use databend_common_base::base::OrderedFloat;
use databend_common_expression::ConstantFolder;
use databend_common_expression::FunctionContext;
use databend_common_expression::Scalar;
use databend_common_expression::TableDataType;
use databend_common_expression::TableField;
use databend_common_expression::TableSchema;
use databend_common_expression::types::ArgType;
use databend_common_expression::types::DataType;
use databend_common_expression::types::Int32Type;
use databend_common_expression::types::NumberDataType;
use databend_common_functions::BUILTIN_FUNCTIONS;
use databend_storages_common_index::RangeIndex;
use databend_storages_common_table_meta::meta::ColumnStatistics;
use databend_storages_common_table_meta::meta::SpatialStatistics;
use databend_storages_common_table_meta::meta::StatisticsOfColumns;
use databend_storages_common_table_meta::meta::StatisticsOfSpatialColumns;
use goldenfile::Mint;

use super::eliminate_cast::parse_expr;

#[test]
fn test_range_index() -> anyhow::Result<()> {
    let mut mint = Mint::new("tests/it/testdata");
    let file = &mut mint.new_goldenfile("test_range_indies.txt").unwrap();

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
    run_text(file, "a > 2 and a = 2", domains);
    run_text(file, "a < 5 and a = 2", domains);
    run_text(file, "a = 2 and a < 5", domains);
    run_text(file, "a > 1 and a = 2", domains);

    // test complex expressions with multiple columns
    run_text(file, "a > 2 and a < 1 and b = 1", domains);
    run_text(file, "a = 2 and b > 5 and b < 3", domains);

    // test edge cases
    run_text(file, "a > 2 and a <= 2", domains);
    run_text(file, "a < 2 and a >= 2", domains);
    run_text(file, "a = 5 and a != 5", domains);

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
fn test_range_index_dates() -> anyhow::Result<()> {
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
        vec![TableField::new("dt", TableDataType::Date)],
    );

    run_text_with_schema(
        file,
        "dt = '2019-05-01' and dt < '2019-04-30'",
        date_domains,
        vec![TableField::new("dt", TableDataType::Date)],
    );

    run_text_with_schema(
        file,
        "dt >= '2019-05-02' and dt <= '2019-05-01'",
        date_domains,
        vec![TableField::new("dt", TableDataType::Date)],
    );

    // Non-mutual exclusion cases
    run_text_with_schema(
        file,
        "dt >= '2019-04-28' and dt <= '2019-05-03'",
        date_domains,
        vec![TableField::new("dt", TableDataType::Date)],
    );

    // Test timestamp ranges
    let ts_domains = &[
        (
            "ts",
            timestamp_scalar(1556668800000000),
            timestamp_scalar(1556755200000000),
        ), // 2019-05-01 to 2019-05-02
    ];

    run_text_with_schema(
        file,
        "ts > '2019-05-01 12:00:00' and ts < '2019-05-01 06:00:00'",
        ts_domains,
        vec![TableField::new("ts", TableDataType::Timestamp)],
    );

    run_text_with_schema(
        file,
        "ts = '2019-05-01 12:00:00' and ts != '2019-05-01 12:00:00'",
        ts_domains,
        vec![TableField::new("ts", TableDataType::Timestamp)],
    );

    Ok(())
}

#[test]
fn test_range_index_strings() -> anyhow::Result<()> {
    let mut mint = Mint::new("tests/it/testdata");
    let file = &mut mint.new_goldenfile("test_range_index_strings.txt").unwrap();

    // String scalar helper
    fn string_scalar(s: &str) -> Scalar {
        Scalar::String(s.to_string())
    }

    // Test date ranges
    let string_domains = &[("s", string_scalar("aaefg"), string_scalar("zzefg"))];
    run_text_with_schema(file, "s > 'efg' and s = 'efg'", string_domains, vec![
        TableField::new("s", TableDataType::String),
    ]);

    run_text_with_schema(file, "s > 'aaefg' and s < 'zzefg'", string_domains, vec![
        TableField::new("s", TableDataType::String),
    ]);
    Ok(())
}

#[test]
fn test_range_index_spatial() -> anyhow::Result<()> {
    let mut mint = Mint::new("tests/it/testdata");
    let file = &mut mint.new_goldenfile("test_range_index_spatial.txt").unwrap();

    let schema = Arc::new(TableSchema::new(vec![TableField::new(
        "g",
        TableDataType::Geometry,
    )]));
    let spatial_stats = build_spatial_stats(&schema, 0.0, 0.0, 10.0, 10.0, 0, false);

    run_text_spatial(
        file,
        "st_intersects(g, to_geometry('POLYGON((20 20, 20 30, 30 30, 30 20, 20 20))'))",
        schema.clone(),
        Some(spatial_stats.clone()),
    );
    run_text_spatial(
        file,
        "st_intersects(g, to_geometry('POLYGON((5 5, 5 15, 15 15, 15 5, 5 5))'))",
        schema.clone(),
        Some(spatial_stats.clone()),
    );
    run_text_spatial(
        file,
        "st_contains(g, to_geometry('POLYGON((-1 -1, -1 12, 12 12, 12 -1, -1 -1))'))",
        schema.clone(),
        Some(spatial_stats.clone()),
    );
    run_text_spatial(
        file,
        "st_contains(g, to_geometry('POLYGON((1 1, 1 2, 2 2, 2 1, 1 1))'))",
        schema.clone(),
        Some(spatial_stats.clone()),
    );
    run_text_spatial(
        file,
        "st_within(g, to_geometry('POLYGON((-1 -1, -1 12, 12 12, 12 -1, -1 -1))'))",
        schema.clone(),
        Some(spatial_stats.clone()),
    );
    run_text_spatial(
        file,
        "st_within(g, to_geometry('POLYGON((1 1, 1 12, 12 12, 12 1, 1 1))'))",
        schema.clone(),
        Some(spatial_stats.clone()),
    );
    run_text_spatial(
        file,
        "st_within(g, to_geometry('POLYGON((0 0, 0 10, 10 10, 10 0, 0 0))'))",
        schema.clone(),
        Some(spatial_stats.clone()),
    );
    run_text_spatial(
        file,
        "st_within(g, to_geometry('POINT(1 1)'))",
        schema.clone(),
        Some(spatial_stats.clone()),
    );
    run_text_spatial(
        file,
        "st_dwithin(g, to_geometry('POINT(20 20)'), 9.9)",
        schema.clone(),
        Some(spatial_stats.clone()),
    );
    run_text_spatial(
        file,
        "st_dwithin(g, to_geometry('POINT(20 20)'), 10.2)",
        schema.clone(),
        Some(spatial_stats.clone()),
    );
    run_text_spatial(
        file,
        "st_dwithin(g, to_geometry('LINESTRING(12 2, 12 8)'), 1.9)",
        schema.clone(),
        Some(spatial_stats.clone()),
    );
    run_text_spatial(
        file,
        "st_dwithin(g, to_geometry('LINESTRING(12 2, 12 8)'), 2.0)",
        schema.clone(),
        Some(spatial_stats.clone()),
    );
    run_text_spatial(
        file,
        "st_dwithin(g, to_geometry('POLYGON((20 0, 22 0, 22 2, 20 2, 20 0))'), 9.9)",
        schema.clone(),
        Some(spatial_stats.clone()),
    );
    run_text_spatial(
        file,
        "st_dwithin(g, to_geometry('POLYGON((20 0, 22 0, 22 2, 20 2, 20 0))'), 10.0)",
        schema.clone(),
        Some(spatial_stats.clone()),
    );

    let spatial_stats_4326 = build_spatial_stats(&schema, 0.0, 0.0, 10.0, 10.0, 4326, false);
    run_text_spatial(
        file,
        "st_intersects(g, to_geometry('POINT(1 1)'))",
        schema.clone(),
        Some(spatial_stats_4326),
    );

    Ok(())
}

fn run_text_with_schema(
    file: &mut impl Write,
    text: &str,
    domains: &[(&str, Scalar, Scalar)],
    fields: Vec<TableField>,
) {
    let func_ctx = FunctionContext::default();
    let schema = Arc::new(TableSchema::new(fields.clone()));
    let stats = create_stats(domains, &schema);

    let columns: Vec<(&str, DataType)> = fields
        .iter()
        .map(|f| (f.name().as_str(), f.data_type().into()))
        .collect();

    let expr = parse_expr(text, &columns);
    let index = RangeIndex::try_create(func_ctx, &expr, schema, Default::default()).unwrap();

    writeln!(file, "text      : {text}").unwrap();
    writeln!(file, "expr      : {expr}").unwrap();

    match index.apply(&stats, None, |_| false) {
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

    match index.apply(&stats, None, |_| false) {
        Err(err) => {
            writeln!(file, "err       : {err}").unwrap();
        }

        Ok(keep) => {
            writeln!(file, "keep      : {keep}").unwrap();
        }
    };
    writeln!(file).unwrap();
}

fn run_text_spatial(
    file: &mut impl Write,
    text: &str,
    schema: Arc<TableSchema>,
    spatial_stats: Option<StatisticsOfSpatialColumns>,
) {
    let func_ctx = FunctionContext::default();
    let stats = HashMap::new();

    let columns = [("g", DataType::Geometry)];
    let expr = parse_expr(text, &columns);
    let (folded_expr, _) = ConstantFolder::fold(&expr, &func_ctx, &BUILTIN_FUNCTIONS);
    let index = RangeIndex::try_create(func_ctx, &folded_expr, schema, Default::default()).unwrap();

    writeln!(file, "text      : {text}").unwrap();
    writeln!(file, "expr      : {expr}").unwrap();
    writeln!(file, "spatial   : {spatial_stats:?}").unwrap();

    match index.apply(&stats, spatial_stats.as_ref(), |_| false) {
        Err(err) => {
            writeln!(file, "err       : {err}").unwrap();
        }
        Ok(keep) => {
            writeln!(file, "keep      : {keep}").unwrap();
        }
    };
    writeln!(file).unwrap();
}

fn build_spatial_stats(
    schema: &TableSchema,
    min_x: f64,
    min_y: f64,
    max_x: f64,
    max_y: f64,
    srid: i32,
    has_null: bool,
) -> StatisticsOfSpatialColumns {
    let column_id = schema.column_id_of("g").unwrap();
    let stats = SpatialStatistics {
        min_x: OrderedFloat(min_x),
        min_y: OrderedFloat(min_y),
        max_x: OrderedFloat(max_x),
        max_y: OrderedFloat(max_y),
        srid,
        has_null,
        has_empty_rect: false,
        is_valid: true,
    };
    [(column_id, stats)].into_iter().collect()
}
