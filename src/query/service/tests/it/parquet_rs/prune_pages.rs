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

use std::sync::Arc;

use common_base::base::tokio;
use common_catalog::plan::ParquetReadOptions;
use common_expression::FunctionContext;
use common_expression::TableSchema;
use common_storages_parquet::ParquetRSPruner;
use parquet::arrow::arrow_reader::ArrowReaderMetadata;
use parquet::arrow::arrow_reader::ArrowReaderOptions;
use parquet::arrow::arrow_reader::RowSelection;
use parquet::arrow::arrow_reader::RowSelector;

use crate::parquet_rs::data::make_test_file_page;
use crate::parquet_rs::data::Scenario;
use crate::parquet_rs::utils::create_test_fixture;
use crate::parquet_rs::utils::get_data_source_plan;

async fn test(scenario: Scenario, predicate: &str, expected_selection: RowSelection) {
    let (file, arrow_schema) = make_test_file_page(scenario).await;
    let file_path = file.path().to_string_lossy();
    let sql = format!("select * from 'fs://{file_path}' where {predicate}");

    let fixture = create_test_fixture().await;
    let plan = get_data_source_plan(fixture.ctx(), &sql).await.unwrap();
    let metadata = ArrowReaderMetadata::load(
        file.as_file(),
        ArrowReaderOptions::new()
            .with_page_index(true)
            .with_skip_arrow_metadata(true),
    )
    .unwrap();
    let parquet_meta = metadata.metadata();
    let schema = TableSchema::try_from(arrow_schema.as_ref()).unwrap();
    let leaf_fields = Arc::new(schema.leaf_fields());

    let pruner = ParquetRSPruner::try_create(
        FunctionContext::default(),
        Arc::new(schema),
        leaf_fields,
        &plan.push_downs,
        ParquetReadOptions::default()
            .with_prune_row_groups(false)
            .with_prune_pages(true),
    )
    .unwrap();

    let row_groups = (0..parquet_meta.num_row_groups()).collect::<Vec<_>>();
    let selection = pruner
        .prune_pages(parquet_meta, &row_groups)
        .unwrap()
        .unwrap();

    assert_eq!(
        expected_selection, selection,
        "Expected {:?}, got {:?}. Scenario: {:?}, predicate: {}",
        expected_selection, selection, scenario, predicate
    );
}

#[tokio::test]
//                         null count  min                                       max
// page-0                         1  2020-01-01T01:01:01.000000                2020-01-02T01:01:01.000000
// page-1                         1  2020-01-01T01:01:11.000000                2020-01-02T01:01:11.000000
// page-2                         1  2020-01-01T01:11:01.000000                2020-01-02T01:11:01.000000
// page-3                         1  2020-01-11T01:01:01.000000                2020-01-12T01:01:01.000000
async fn test_timestamp() {
    test(
        Scenario::Timestamp,
        "micros < to_timestamp('2020-01-02 01:01:11Z')",
        RowSelection::from(vec![RowSelector::select(15), RowSelector::skip(5)]),
    )
    .await;
}

#[tokio::test]
//                       null count  min                                       max
// page-0                         1  2020-01-01                                2020-01-04
// page-1                         1  2020-01-11                                2020-01-14
// page-2                         1  2020-10-27                                2020-10-30
// page-3                         1  2029-11-09                                2029-11-12
async fn test_date() {
    test(
        Scenario::Date,
        "date32 < to_date('2020-01-02')",
        RowSelection::from(vec![RowSelector::select(5), RowSelector::skip(15)]),
    )
    .await;
}

#[tokio::test]
//                      null count  min                                       max
// page-0                         0  -5                                        -1
// page-1                         0  -4                                        0
// page-2                         0  0                                         4
// page-3                         0  5                                         9
async fn test_int32_lt() {
    test(
        Scenario::Int32,
        "i < 1",
        RowSelection::from(vec![RowSelector::select(15), RowSelector::skip(5)]),
    )
    .await;
    // result of sql "SELECT * FROM t where i < 1" is same as
    // "SELECT * FROM t where -i > -1"
    test(
        Scenario::Int32,
        "-i > -1",
        RowSelection::from(vec![RowSelector::select(15), RowSelector::skip(5)]),
    )
    .await;
}

#[tokio::test]
async fn test_int32_gt() {
    test(
        Scenario::Int32,
        "i > 8",
        RowSelection::from(vec![RowSelector::skip(15), RowSelector::select(5)]),
    )
    .await;

    test(
        Scenario::Int32,
        "-i < -8",
        RowSelection::from(vec![RowSelector::skip(15), RowSelector::select(5)]),
    )
    .await;
}

#[tokio::test]
async fn test_int32_eq() {
    test(
        Scenario::Int32,
        "i = 1",
        RowSelection::from(vec![
            RowSelector::skip(10),
            RowSelector::select(5),
            RowSelector::skip(5),
        ]),
    )
    .await;
}
#[tokio::test]
async fn test_int32_scalar_fun_and_eq() {
    test(
        Scenario::Int32,
        "abs(i) = 1 and i = 1",
        RowSelection::from(vec![
            RowSelector::skip(10),
            RowSelector::select(5),
            RowSelector::skip(5),
        ]),
    )
    .await;
}

#[tokio::test]
async fn test_int32_scalar_fun() {
    test(
        Scenario::Int32,
        "abs(i) = 1",
        RowSelection::from(vec![RowSelector::select(15), RowSelector::skip(5)]),
    )
    .await;
}

#[tokio::test]
async fn test_int32_complex_expr() {
    test(
        Scenario::Int32,
        "i+1 = 1",
        RowSelection::from(vec![
            RowSelector::skip(5),
            RowSelector::select(10),
            RowSelector::skip(5),
        ]),
    )
    .await;
}

#[tokio::test]
async fn test_int32_complex_expr_subtract() {
    test(
        Scenario::Int32,
        "1-i > 1",
        RowSelection::from(vec![RowSelector::select(10), RowSelector::skip(10)]),
    )
    .await;
}

#[tokio::test]
//                      null count  min                                       max
// page-0                         0  -5.0                                      -1.0
// page-1                         0  -4.0                                      0.0
// page-2                         0  0.0                                       4.0
// page-3                         0  5.0                                       9.0
async fn test_f64_lt() {
    test(
        Scenario::Float64,
        "f < 1",
        RowSelection::from(vec![RowSelector::select(15), RowSelector::skip(5)]),
    )
    .await;
    test(
        Scenario::Float64,
        "-f > -1",
        RowSelection::from(vec![RowSelector::select(15), RowSelector::skip(5)]),
    )
    .await;
}

#[tokio::test]
async fn test_f64_scalar_fun_and_gt() {
    test(
        Scenario::Float64,
        "abs(f - 1) <= 0.000001  and f >= 0.1",
        RowSelection::from(vec![
            RowSelector::skip(10),
            RowSelector::select(5),
            RowSelector::skip(5),
        ]),
    )
    .await;
}

#[tokio::test]
async fn test_f64_scalar_fun() {
    test(
        Scenario::Float64,
        "abs(f-1) <= 0.000001",
        RowSelection::from(vec![
            RowSelector::skip(10),
            RowSelector::select(5),
            RowSelector::skip(5),
        ]),
    )
    .await;
}

#[tokio::test]
async fn test_f64_complex_expr() {
    test(
        Scenario::Float64,
        "f+1 > 1.1",
        RowSelection::from(vec![RowSelector::skip(10), RowSelector::select(10)]),
    )
    .await;
}

#[tokio::test]
async fn test_f64_complex_expr_subtract() {
    test(
        Scenario::Float64,
        "1-f > 1",
        RowSelection::from(vec![RowSelector::select(10), RowSelector::skip(10)]),
    )
    .await;
}

#[tokio::test]
//                      null count  min                                       max
// page-0                         0  -5                                        -1
// page-1                         0  -4                                        0
// page-2                         0  0                                         4
// page-3                         0  5                                         9
async fn test_int32_eq_in_list() {
    test(
        Scenario::Int32,
        "i in (1)",
        RowSelection::from(vec![
            RowSelector::skip(10),
            RowSelector::select(5),
            RowSelector::skip(5),
        ]),
    )
    .await;
}

#[tokio::test]
async fn test_int32_eq_in_list_2() {
    test(
        Scenario::Int32,
        "i in (100)",
        RowSelection::from(vec![RowSelector::skip(20)]),
    )
    .await;
}

#[tokio::test]
async fn test_int32_eq_in_list_negated() {
    test(
        Scenario::Int32,
        "i not in (1)",
        RowSelection::from(vec![RowSelector::select(20)]),
    )
    .await;
}

#[tokio::test]
async fn test_decimal_lt() {
    // The data type of decimal_col is decimal(9,2)
    // There are three pages each 5 rows:
    // [1.00, 6.00], [-5.00,6.00], [20.00,60.00]
    test(
        Scenario::Decimal,
        "decimal_col < 4",
        RowSelection::from(vec![RowSelector::select(10), RowSelector::skip(5)]),
    )
    .await;
    // compare with the casted decimal value
    test(
        Scenario::Decimal,
        "decimal_col < cast(4.55 as decimal(20,2))",
        RowSelection::from(vec![RowSelector::select(10), RowSelector::skip(5)]),
    )
    .await;

    // The data type of decimal_col is decimal(38,2)
    test(
        Scenario::DecimalLargePrecision,
        "decimal_col < 4",
        RowSelection::from(vec![RowSelector::select(10), RowSelector::skip(5)]),
    )
    .await;
    // compare with the casted decimal value
    test(
        Scenario::DecimalLargePrecision,
        "decimal_col < cast(4.55 as decimal(20,2))",
        RowSelection::from(vec![RowSelector::select(10), RowSelector::skip(5)]),
    )
    .await;
}

#[tokio::test]
async fn test_decimal_eq() {
    // The data type of decimal_col is decimal(9,2)
    // There are three pages:
    // [1.00, 6.00], [-5.00,6.00], [20.00,60.00]
    test(
        Scenario::Decimal,
        "decimal_col = 4",
        RowSelection::from(vec![RowSelector::select(10), RowSelector::skip(5)]),
    )
    .await;
    test(
        Scenario::Decimal,
        "decimal_col = 4.00",
        RowSelection::from(vec![RowSelector::select(10), RowSelector::skip(5)]),
    )
    .await;

    // The data type of decimal_col is decimal(38,2)
    test(
        Scenario::DecimalLargePrecision,
        "decimal_col = 4",
        RowSelection::from(vec![RowSelector::select(10), RowSelector::skip(5)]),
    )
    .await;
    test(
        Scenario::DecimalLargePrecision,
        "decimal_col = 4.00",
        RowSelection::from(vec![RowSelector::select(10), RowSelector::skip(5)]),
    )
    .await;
    test(
        Scenario::DecimalLargePrecision,
        "decimal_col = 30.00",
        RowSelection::from(vec![RowSelector::skip(10), RowSelector::select(5)]),
    )
    .await;
}

#[tokio::test]
async fn test_decimal_in_list() {
    // The data type of decimal_col is decimal(9,2)
    // There are three pages:
    // [1.00, 6.00], [-5.00,6.00], [20.00,60.00]
    test(
        Scenario::Decimal,
        "decimal_col in (4,3,123456789123)",
        RowSelection::from(vec![RowSelector::select(10), RowSelector::skip(5)]),
    )
    .await;
    test(
        Scenario::Decimal,
        "decimal_col in (4.00,3.00,11.2345)",
        RowSelection::from(vec![RowSelector::select(10), RowSelector::skip(5)]),
    )
    .await;

    // The data type of decimal_col is decimal(38,2)
    test(
        Scenario::DecimalLargePrecision,
        "decimal_col in (4,3,123456789123)",
        RowSelection::from(vec![RowSelector::select(10), RowSelector::skip(5)]),
    )
    .await;
    test(
        Scenario::DecimalLargePrecision,
        "decimal_col in (4.00,3.00,11.2345,1)",
        RowSelection::from(vec![RowSelector::select(10), RowSelector::skip(5)]),
    )
    .await;
}

#[tokio::test]
async fn test_periods_in_column_names() {
    // There are three row groups for "service.name", each with 5 rows = 15 rows total
    // name = "HTTP GET / DISPATCH", service.name = ['frontend', 'frontend'],
    // name = "HTTP PUT / DISPATCH", service.name = ['backend',  'frontend'],
    // name = "HTTP GET / DISPATCH", service.name = ['backend',  'backend' ],
    test(
        Scenario::PeriodsInColumnNames,
        // use double quotes to use column named "service.name"
        "\"service.name\" = 'frontend'",
        RowSelection::from(vec![RowSelector::select(10), RowSelector::skip(5)]),
    )
    .await;
    test(
        Scenario::PeriodsInColumnNames,
        "name <> 'HTTP GET / DISPATCH'",
        RowSelection::from(vec![
            RowSelector::skip(5),
            RowSelector::select(5),
            RowSelector::skip(5),
        ]),
    )
    .await;
    test(
        Scenario::PeriodsInColumnNames,
        "\"service.name\" = 'frontend' AND name = 'HTTP GET / DISPATCH'",
        RowSelection::from(vec![RowSelector::select(5), RowSelector::skip(10)]),
    )
    .await;
}
