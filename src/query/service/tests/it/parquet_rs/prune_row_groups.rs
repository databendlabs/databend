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

use super::data::make_test_file_rg;
use super::data::Scenario;
use super::utils::get_data_source_plan;
use crate::parquet_rs::utils::create_test_fixture;

/// Enable row groups pruning and test.
async fn test(scenario: Scenario, predicate: &str, expected_rgs: Vec<usize>) {
    test_impl(scenario, predicate, expected_rgs, true).await
}

// Disable row groups pruning and test.
async fn test_without_prune(scenario: Scenario, predicate: &str, expected_rgs: Vec<usize>) {
    test_impl(scenario, predicate, expected_rgs, false).await
}

async fn test_impl(scenario: Scenario, predicate: &str, expected_rgs: Vec<usize>, prune: bool) {
    let (file, arrow_schema) = make_test_file_rg(scenario).await;
    let file_path = file.path().to_string_lossy();
    let sql = format!("select * from 'fs://{file_path}' where {predicate}");

    let fixture = create_test_fixture().await;
    let plan = get_data_source_plan(fixture.ctx(), &sql).await.unwrap();
    let parquet_meta = parquet::file::footer::parse_metadata(file.as_file()).unwrap();
    let schema = TableSchema::try_from(arrow_schema.as_ref()).unwrap();
    let leaf_fields = Arc::new(schema.leaf_fields());

    let pruner = ParquetRSPruner::try_create(
        FunctionContext::default(),
        Arc::new(schema),
        leaf_fields,
        &plan.push_downs,
        ParquetReadOptions::default()
            .with_prune_row_groups(prune)
            .with_prune_pages(false),
    )
    .unwrap();

    let (rgs, _) = pruner.prune_row_groups(&parquet_meta, None).unwrap();

    assert_eq!(
        expected_rgs, rgs,
        "Expected {:?}, got {:?}. Scenario: {:?}, predicate: {}",
        expected_rgs, rgs, scenario, predicate
    );
}

#[tokio::test]
async fn test_timestamp() {
    test(
        Scenario::Timestamp,
        "micros < to_timestamp('2020-01-02 01:01:11Z')",
        vec![0, 1, 2],
    )
    .await;
}

#[tokio::test]
async fn test_date() {
    test(Scenario::Date, "date32 < to_date('2020-01-02')", vec![0]).await;
}

#[tokio::test]
async fn test_disabled() {
    test(
        Scenario::Timestamp,
        "micros < to_timestamp('2020-01-02 01:01:11Z')",
        vec![0, 1, 2],
    )
    .await;

    test_without_prune(
        Scenario::Timestamp,
        "micros < to_timestamp('2020-01-02 01:01:11Z')",
        vec![0, 1, 2, 3],
    )
    .await;
}

#[tokio::test]
async fn test_int32_lt() {
    test(Scenario::Int32, "i < 1", vec![0, 1, 2]).await;
    // result of sql "SELECT * FROM t where i < 1" is same as
    // "SELECT * FROM t where -i > -1"
    test(Scenario::Int32, " -i > -1", vec![0, 1, 2]).await
}

#[tokio::test]
async fn test_int32_eq() {
    test(Scenario::Int32, "i = 1", vec![2]).await;
}
#[tokio::test]
async fn test_int32_scalar_fun_and_eq() {
    test(Scenario::Int32, "abs(i) = 1 and i = 1", vec![2]).await;
}

#[tokio::test]
async fn test_int32_scalar_fun() {
    test(Scenario::Int32, "abs(i) = 1", vec![0, 1, 2]).await;
}

#[tokio::test]
async fn test_int32_complex_expr() {
    test(Scenario::Int32, "i+1 = 1", vec![1, 2]).await;
}

#[tokio::test]
async fn test_int32_complex_expr_subtract() {
    test(Scenario::Int32, "1-i > 1", vec![0, 1]).await;
}

#[tokio::test]
async fn test_f64_lt() {
    test(Scenario::Float64, "f < 1", vec![0, 1, 2]).await;
    test(Scenario::Float64, "-f > -1", vec![0, 1, 2]).await;
}

#[tokio::test]
async fn test_f64_scalar_fun_and_gt() {
    test(
        Scenario::Float64,
        "abs(f - 1) <= 0.000001 and f >= 0.1",
        vec![2],
    )
    .await;
}

#[tokio::test]
async fn test_f64_scalar_fun() {
    test(Scenario::Float64, "abs(f-1) <= 0.000001", vec![2]).await;
}

#[tokio::test]
async fn test_f64_complex_expr() {
    test(Scenario::Float64, "f+1 > 1.1", vec![2, 3]).await;
}

#[tokio::test]
async fn test_f64_complex_expr_subtract() {
    test(Scenario::Float64, "1-f > 1", vec![0, 1]).await;
}

#[tokio::test]
async fn test_int32_eq_in_list() {
    test(Scenario::Int32, "i in (1)", vec![2]).await;
}

#[tokio::test]
async fn test_int32_eq_in_list_2() {
    test(Scenario::Int32, "i in (1000)", vec![]).await;
}

#[tokio::test]
async fn test_int32_eq_in_list_negated() {
    test(Scenario::Int32, "i not in (1)", vec![0, 1, 2, 3]).await;
}

#[tokio::test]
async fn test_decimal_lt() {
    // The data type of decimal_col is decimal(9,2)
    // There are three row groups:
    // [1.00, 6.00], [-5.00,6.00], [20.00,60.00]
    test(Scenario::Decimal, "decimal_col < 4", vec![0, 1]).await;
    // compare with the casted decimal value
    test(
        Scenario::Decimal,
        "decimal_col < cast(4.55 as decimal(20,2))",
        vec![0, 1],
    )
    .await;

    // The data type of decimal_col is decimal(38,2)
    test(Scenario::DecimalLargePrecision, "decimal_col < 4", vec![
        0, 1,
    ])
    .await;
    // compare with the casted decimal value
    test(
        Scenario::DecimalLargePrecision,
        "decimal_col < cast(4.55 as decimal(20,2))",
        vec![0, 1],
    )
    .await;
}

#[tokio::test]
async fn test_decimal_eq() {
    // The data type of decimal_col is decimal(9,2)
    // There are three row groups:
    // [1.00, 6.00], [-5.00,6.00], [20.00,60.00]
    test(Scenario::Decimal, "decimal_col = 4", vec![0, 1]).await;
    test(Scenario::Decimal, "decimal_col = 4.00", vec![0, 1]).await;

    // The data type of decimal_col is decimal(38,2)
    test(Scenario::DecimalLargePrecision, "decimal_col = 4", vec![
        0, 1,
    ])
    .await;
    test(Scenario::DecimalLargePrecision, "decimal_col = 4.00", vec![
        0, 1,
    ])
    .await;
}

#[tokio::test]
async fn test_decimal_in_list() {
    // The data type of decimal_col is decimal(9,2)
    // There are three row groups:
    // [1.00, 6.00], [-5.00,6.00], [20.00,60.00]
    test(
        Scenario::Decimal,
        "decimal_col in (4,3,123456789123)",
        vec![0, 1],
    )
    .await;
    test(
        Scenario::Decimal,
        "decimal_col in (4.00,3.00,11.2345)",
        vec![0, 1],
    )
    .await;

    // The data type of decimal_col is decimal(38,2)
    test(
        Scenario::DecimalLargePrecision,
        "decimal_col in (4,3,123456789123)",
        vec![0, 1],
    )
    .await;
    test(
        Scenario::DecimalLargePrecision,
        "decimal_col in (4.00,3.00,11.2345)",
        vec![0, 1],
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
        vec![0, 1],
    )
    .await;
    test(
        Scenario::PeriodsInColumnNames,
        "name <> 'HTTP GET / DISPATCH'",
        vec![1],
    )
    .await;
    test(
        Scenario::PeriodsInColumnNames,
        "\"service.name\" = 'frontend' AND name = 'HTTP GET / DISPATCH'",
        vec![0],
    )
    .await;
}
