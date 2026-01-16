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

use databend_common_catalog::plan::ParquetReadOptions;
use databend_common_expression::FunctionContext;
use databend_common_expression::TableSchema;
use databend_common_storages_parquet::ParquetPruner;
use parquet::file::metadata::ParquetMetaDataReader;

use super::data::Scenario;
use super::data::make_test_file_rg;
use super::utils::get_data_source_plan;
use crate::parquet_rs::utils::create_parquet_test_fixture;

/// Enable row groups pruning and test.
async fn test_batch(args: &[(Scenario, &str, Vec<usize>)]) {
    test_impl_batch(args, true).await
}

// Disable row groups pruning and test.
async fn test_batch_without_prune(args: &[(Scenario, &str, Vec<usize>)]) {
    test_impl_batch(args, false).await
}

async fn test_impl_batch(args: &[(Scenario, &str, Vec<usize>)], prune: bool) {
    let fixture = create_parquet_test_fixture().await;

    for (scenario, predicate, expected_rgs) in args {
        let (file, arrow_schema) = make_test_file_rg(*scenario).await;
        let file_path = file.path().to_string_lossy();
        let sql = format!("select * from 'fs://{file_path}' where {predicate}");

        let plan = get_data_source_plan(fixture.new_query_ctx().await.unwrap(), &sql)
            .await
            .unwrap();
        let parquet_meta = ParquetMetaDataReader::new()
            .parse_and_finish(file.as_file())
            .unwrap();
        let schema = TableSchema::try_from(arrow_schema.as_ref()).unwrap();
        let leaf_fields = Arc::new(schema.leaf_fields());

        let pruner = ParquetPruner::try_create(
            FunctionContext::default(),
            Arc::new(schema),
            leaf_fields,
            &plan.push_downs,
            ParquetReadOptions::default()
                .with_prune_row_groups(prune)
                .with_prune_pages(false),
            vec![],
        )
        .unwrap();

        let (rgs, _, _) = pruner.prune_row_groups(&parquet_meta, None, None).unwrap();

        assert_eq!(
            expected_rgs.to_vec(),
            rgs,
            "Expected {:?}, got {:?}. Scenario: {:?}, predicate: {}",
            expected_rgs,
            rgs,
            scenario,
            predicate
        );
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 1)]
async fn test_disabled() {
    test_batch(&[(
        Scenario::Timestamp,
        "micros < to_timestamp('2020-01-02 01:01:11Z')",
        vec![0, 1, 2],
    )])
    .await;
    test_batch_without_prune(&[(
        Scenario::Timestamp,
        "micros < to_timestamp('2020-01-02 01:01:11Z')",
        vec![0, 1, 2, 3],
    )])
    .await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 1)]
async fn test_various_rg_scenarios() {
    let test_cases = vec![
        (
            Scenario::Timestamp,
            "micros < to_timestamp('2020-01-02 01:01:11Z')",
            vec![0, 1, 2],
        ),
        // Date scenario
        (Scenario::Date, "date32 < to_date('2020-01-02')", vec![0]),
        // Int32 scenarios
        (Scenario::Int32, "i < 1", vec![0, 1, 2]),
        // result of sql "SELECT * FROM t where i < 1" is same as
        // "SELECT * FROM t where -i > -1"
        (Scenario::Int32, " -i > -1", vec![0, 1, 2]),
        (Scenario::Int32, "i = 1", vec![2]),
        (Scenario::Int32, "abs(i) = 1 and i = 1", vec![2]),
        (Scenario::Int32, "abs(i) = 1", vec![0, 1, 2]),
        (Scenario::Int32, "i+1 = 1", vec![1, 2]),
        (Scenario::Int32, "1-i > 1", vec![0, 1]),
        (Scenario::Int32, "i in (1)", vec![2]),
        (Scenario::Int32, "i in (1000)", vec![]),
        (Scenario::Int32, "i not in (1)", vec![0, 1, 2, 3]),
        // Float64 scenarios
        (Scenario::Float64, "f < 1", vec![0, 1, 2]),
        (Scenario::Float64, "-f > -1", vec![0, 1, 2]),
        (
            Scenario::Float64,
            "abs(f - 1) <= 0.000001 and f >= 0.1",
            vec![2],
        ),
        (Scenario::Float64, "abs(f-1) <= 0.000001", vec![2]),
        (Scenario::Float64, "f+1 > 1.1", vec![2, 3]),
        (Scenario::Float64, "1-f > 1", vec![0, 1]),
        // Decimal scenarios
        // The data type of decimal_col is decimal(9,2)
        // There are three row groups:
        // [1.00, 6.00], [-5.00,6.00], [20.00,60.00]
        (Scenario::Decimal, "decimal_col < 4", vec![0, 1]),
        // compare with the casted decimal value
        (
            Scenario::Decimal,
            "decimal_col < cast(4.55 as decimal(20,2))",
            vec![0, 1],
        ),
        (Scenario::Decimal, "decimal_col = 4", vec![0, 1]),
        (Scenario::Decimal, "decimal_col = 4.00", vec![0, 1]),
        (
            Scenario::Decimal,
            "decimal_col in (4,3,123456789123)",
            vec![0, 1],
        ),
        (
            Scenario::Decimal,
            "decimal_col in (4.00,3.00,11.2345)",
            vec![0, 1],
        ),
        // DecimalLargePrecision scenarios
        // The data type of decimal_col is decimal(38,2)
        (Scenario::DecimalLargePrecision, "decimal_col < 4", vec![
            0, 1,
        ]),
        (
            Scenario::DecimalLargePrecision,
            "decimal_col < cast(4.55 as decimal(20,2))",
            vec![0, 1],
        ),
        (Scenario::DecimalLargePrecision, "decimal_col = 4", vec![
            0, 1,
        ]),
        (Scenario::DecimalLargePrecision, "decimal_col = 4.00", vec![
            0, 1,
        ]),
        (
            Scenario::DecimalLargePrecision,
            "decimal_col in (4,3,123456789123)",
            vec![0, 1],
        ),
        (
            Scenario::DecimalLargePrecision,
            "decimal_col in (4.00,3.00,11.2345)",
            vec![0, 1],
        ),
        // PeriodsInColumnNames scenarios
        // There are three row groups for "service.name", each with 5 rows = 15 rows total
        // name = "HTTP GET / DISPATCH", service.name = ['frontend', 'frontend'],
        // name = "HTTP PUT / DISPATCH", service.name = ['backend',  'frontend'],
        // name = "HTTP GET / DISPATCH", service.name = ['backend',  'backend' ],
        // use double quotes to use column named "service.name"
        (
            Scenario::PeriodsInColumnNames,
            "\"service.name\" = 'frontend'",
            vec![0, 1],
        ),
        (
            Scenario::PeriodsInColumnNames,
            "name <> 'HTTP GET / DISPATCH'",
            vec![1],
        ),
        (
            Scenario::PeriodsInColumnNames,
            "\"service.name\" = 'frontend' AND name = 'HTTP GET / DISPATCH'",
            vec![0],
        ),
    ];

    test_batch(&test_cases).await;
}
