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

use databend_common_catalog::BasicColumnStatistics;
use databend_common_catalog::TableStatistics;
use databend_common_exception::Result;
use databend_query::sessions::QueryContext;
use databend_query::test_kits::TestFixture;

use crate::sql::planner::optimizer::test_utils::execute_sql;
use crate::sql::planner::optimizer::test_utils::optimize_plan;
use crate::sql::planner::optimizer::test_utils::raw_plan;
use crate::sql::planner::optimizer::test_utils::PlanStatisticsExt;
use crate::sql::planner::optimizer::test_utils::TestCase;

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn test_tpcds_optimizer() -> Result<()> {
    // Create test fixture
    let fixture = TestFixture::setup().await?;
    let ctx = fixture.new_query_ctx().await?;

    // Setup tables needed for TPC-DS queries
    setup_tpcds_tables(&ctx).await?;

    // Define test cases
    let tests = vec![TestCase {
        name: "Q3",
        sql: r#"
                SELECT dt.d_year, item.i_brand_id brand_id, item.i_brand brand, 
                       SUM(ss_ext_sales_price) AS sum_agg
                FROM date_dim dt, store_sales, item
                WHERE dt.d_date_sk = store_sales.ss_sold_date_sk
                  AND store_sales.ss_item_sk = item.i_item_sk
                  AND item.i_manufact_id = 128
                  AND dt.d_moy = 11
                GROUP BY dt.d_year, item.i_brand, item.i_brand_id
                ORDER BY dt.d_year, sum_agg DESC, brand_id
                LIMIT 100
            "#,
        stats_setup: |plan| {
            plan.set_table_statistics(
                "date_dim",
                Some(TableStatistics {
                    num_rows: Some(1000),
                    ..Default::default()
                }),
            )?
            .set_column_statistics(
                "date_dim",
                "d_year",
                Some(BasicColumnStatistics {
                    ndv: Some(10),
                    ..Default::default()
                }),
            )?
            .set_table_statistics(
                "store_sales",
                Some(TableStatistics {
                    num_rows: Some(10000),
                    ..Default::default()
                }),
            )?
            .set_column_statistics(
                "store_sales",
                "ss_ext_sales_price",
                Some(BasicColumnStatistics {
                    ndv: Some(1000),
                    ..Default::default()
                }),
            )?
            .set_table_statistics(
                "item",
                Some(TableStatistics {
                    num_rows: Some(500),
                    ..Default::default()
                }),
            )?
            .set_column_statistics(
                "item",
                "i_brand_id",
                Some(BasicColumnStatistics {
                    ndv: Some(50),
                    ..Default::default()
                }),
            )?;
            Ok(())
        },
        raw_plan: r#"Limit
├── limit: [100]
├── offset: [0]
└── Sort
    ├── sort keys: [default.date_dim.d_year (#6) ASC, derived.SUM(ss_ext_sales_price) (#73) DESC, default.item.i_brand_id (#58) ASC]
    ├── limit: [NONE]
    └── EvalScalar
        ├── scalars: [dt.d_year (#6) AS (#6), item.i_brand_id (#58) AS (#58), item.i_brand (#59) AS (#59), SUM(ss_ext_sales_price) (#73) AS (#73)]
        └── Aggregate(Initial)
            ├── group items: [dt.d_year (#6), item.i_brand (#59), item.i_brand_id (#58)]
            ├── aggregate functions: [SUM(ss_ext_sales_price) (#73)]
            └── EvalScalar
                ├── scalars: [dt.d_year (#6) AS (#6), store_sales.ss_ext_sales_price (#43) AS (#43), item.i_brand_id (#58) AS (#58), item.i_brand (#59) AS (#59)]
                └── Filter
                    ├── filters: [eq(dt.d_date_sk (#0), store_sales.ss_sold_date_sk (#28)), eq(store_sales.ss_item_sk (#30), item.i_item_sk (#51)), eq(item.i_manufact_id (#64), 128), eq(dt.d_moy (#8), 11)]
                    └── Join(Cross)
                        ├── build keys: []
                        ├── probe keys: []
                        ├── other filters: []
                        ├── Join(Cross)
                        │   ├── build keys: []
                        │   ├── probe keys: []
                        │   ├── other filters: []
                        │   ├── Scan
                        │   │   ├── table: default.date_dim
                        │   │   ├── filters: []
                        │   │   ├── order by: []
                        │   │   └── limit: NONE
                        │   └── Scan
                        │       ├── table: default.store_sales
                        │       ├── filters: []
                        │       ├── order by: []
                        │       └── limit: NONE
                        └── Scan
                            ├── table: default.item
                            ├── filters: []
                            ├── order by: []
                            └── limit: NONE"#,
        expected_plan: r#"Limit
├── limit: [100]
├── offset: [0]
└── Sort
    ├── sort keys: [default.date_dim.d_year (#6) ASC, derived.SUM(ss_ext_sales_price) (#73) DESC, default.item.i_brand_id (#58) ASC]
    ├── limit: [100]
    └── Aggregate(Final)
        ├── group items: [dt.d_year (#6), item.i_brand (#59), item.i_brand_id (#58)]
        ├── aggregate functions: [SUM(ss_ext_sales_price) (#73)]
        └── Aggregate(Partial)
            ├── group items: [dt.d_year (#6), item.i_brand (#59), item.i_brand_id (#58)]
            ├── aggregate functions: [SUM(ss_ext_sales_price) (#73)]
            └── EvalScalar
                ├── scalars: [dt.d_year (#6) AS (#6), store_sales.ss_ext_sales_price (#43) AS (#43), item.i_brand_id (#58) AS (#58), item.i_brand (#59) AS (#59), dt.d_date_sk (#0) AS (#74), store_sales.ss_sold_date_sk (#28) AS (#75), store_sales.ss_item_sk (#30) AS (#76), item.i_item_sk (#51) AS (#77), item.i_manufact_id (#64) AS (#78), dt.d_moy (#8) AS (#79)]
                └── Join(Inner)
                    ├── build keys: [store_sales.ss_sold_date_sk (#28)]
                    ├── probe keys: [dt.d_date_sk (#0)]
                    ├── other filters: []
                    ├── Scan
                    │   ├── table: default.date_dim
                    │   ├── filters: [eq(date_dim.d_moy (#8), 11)]
                    │   ├── order by: []
                    │   └── limit: NONE
                    └── Join(Inner)
                        ├── build keys: [item.i_item_sk (#51)]
                        ├── probe keys: [store_sales.ss_item_sk (#30)]
                        ├── other filters: []
                        ├── Scan
                        │   ├── table: default.store_sales
                        │   ├── filters: []
                        │   ├── order by: []
                        │   └── limit: NONE
                        └── Scan
                            ├── table: default.item
                            ├── filters: [eq(item.i_manufact_id (#64), 128)]
                            ├── order by: []
                            └── limit: NONE"#,
    }];

    // Run all test cases
    for test in tests {
        println!("\n\n========== Testing: {} ==========", test.name);

        // Parse SQL to get raw plan
        let mut raw_plan = raw_plan(&ctx, test.sql).await?;

        // Set statistics for the plan
        (test.stats_setup)(&mut raw_plan)?;

        // Print and verify raw plan
        let raw_plan_str = raw_plan.format_indent(false)?;
        println!("Raw plan:\n{}", raw_plan_str);

        // Verify raw plan matches expected
        let actual_raw = raw_plan_str.trim();
        let expected_raw = test.raw_plan.trim();
        if actual_raw != expected_raw {
            println!("Raw plan difference detected for test {}:\n", test.name);
            println!("Expected raw plan:\n{}\n", expected_raw);
            println!("Actual raw plan:\n{}\n", actual_raw);
            // Update the expected output in the test case
            println!(
                "To fix the test, update the raw_plan in the test case to match the actual output."
            );
        }
        assert_eq!(
            actual_raw, expected_raw,
            "Test {} failed: raw plan does not match expected output",
            test.name
        );

        // Optimize the plan
        let optimized_plan = optimize_plan(&ctx, raw_plan).await?;
        let optimized_plan_str = optimized_plan.format_indent(false)?;
        println!("Optimized plan:\n{}", optimized_plan_str);

        // Verify the optimized plan matches expected output
        let actual_optimized = optimized_plan_str.trim();
        let expected_optimized = test.expected_plan.trim();
        if actual_optimized != expected_optimized {
            println!(
                "Optimized plan difference detected for test {}:\n",
                test.name
            );
            println!("Expected optimized plan:\n{}\n", expected_optimized);
            println!("Actual optimized plan:\n{}\n", actual_optimized);
            // Update the expected output in the test case
            println!("To fix the test, update the expected_plan in the test case to match the actual output.");
        }
        assert_eq!(
            actual_optimized, expected_optimized,
            "Test {} failed: optimized plan does not match expected output",
            test.name
        );

        println!("✅ {} test passed!", test.name);
    }

    Ok(())
}

/// Setup TPC-DS tables with required schema
async fn setup_tpcds_tables(ctx: &Arc<QueryContext>) -> Result<()> {
    // Create date_dim table
    execute_sql(
        ctx,
        r#"
        CREATE TABLE date_dim (
            d_date_sk INTEGER,
            d_date_id CHAR(16),
            d_date DATE NULL,
            d_month_seq INTEGER NULL,
            d_week_seq INTEGER NULL,
            d_quarter_seq INTEGER NULL,
            d_year INTEGER NULL,
            d_dow INTEGER NULL,
            d_moy INTEGER NULL,
            d_dom INTEGER NULL,
            d_qoy INTEGER NULL,
            d_fy_year INTEGER NULL,
            d_fy_quarter_seq INTEGER NULL,
            d_fy_week_seq INTEGER NULL,
            d_day_name CHAR(9) NULL,
            d_quarter_name CHAR(6) NULL,
            d_holiday CHAR(1) NULL,
            d_weekend CHAR(1) NULL,
            d_following_holiday CHAR(1) NULL,
            d_first_dom INTEGER NULL,
            d_last_dom INTEGER NULL,
            d_same_day_ly INTEGER NULL,
            d_same_day_lq INTEGER NULL,
            d_current_day CHAR(1) NULL,
            d_current_week CHAR(1) NULL,
            d_current_month CHAR(1) NULL,
            d_current_quarter CHAR(1) NULL,
            d_current_year CHAR(1) NULL
        )
    "#,
    )
    .await?;

    // Create store_sales table
    execute_sql(
        ctx,
        r#"
        CREATE TABLE store_sales (
            ss_sold_date_sk INTEGER NULL,
            ss_sold_time_sk INTEGER NULL,
            ss_item_sk INTEGER,
            ss_customer_sk INTEGER NULL,
            ss_cdemo_sk INTEGER NULL,
            ss_hdemo_sk INTEGER NULL,
            ss_addr_sk INTEGER NULL,
            ss_store_sk INTEGER NULL,
            ss_promo_sk INTEGER NULL,
            ss_ticket_number INTEGER,
            ss_quantity INTEGER NULL,
            ss_wholesale_cost DECIMAL(7,2) NULL,
            ss_list_price DECIMAL(7,2) NULL,
            ss_sales_price DECIMAL(7,2) NULL,
            ss_ext_discount_amt DECIMAL(7,2) NULL,
            ss_ext_sales_price DECIMAL(7,2) NULL,
            ss_ext_wholesale_cost DECIMAL(7,2) NULL,
            ss_ext_list_price DECIMAL(7,2) NULL,
            ss_ext_tax DECIMAL(7,2) NULL,
            ss_coupon_amt DECIMAL(7,2) NULL,
            ss_net_paid DECIMAL(7,2) NULL,
            ss_net_paid_inc_tax DECIMAL(7,2) NULL,
            ss_net_profit DECIMAL(7,2) NULL
        )
    "#,
    )
    .await?;

    // Create item table
    execute_sql(
        ctx,
        r#"
        CREATE TABLE item (
            i_item_sk INTEGER,
            i_item_id CHAR(16),
            i_rec_start_date DATE NULL,
            i_rec_end_date DATE NULL,
            i_item_desc VARCHAR(200) NULL,
            i_current_price DECIMAL(7,2) NULL,
            i_wholesale_cost DECIMAL(7,2) NULL,
            i_brand_id INTEGER NULL,
            i_brand CHAR(50) NULL,
            i_class_id INTEGER NULL,
            i_class CHAR(50) NULL,
            i_category_id INTEGER NULL,
            i_category CHAR(50) NULL,
            i_manufact_id INTEGER NULL,
            i_manufact CHAR(50) NULL,
            i_size CHAR(20) NULL,
            i_formulation CHAR(20) NULL,
            i_color CHAR(20) NULL,
            i_units CHAR(10) NULL,
            i_container CHAR(10) NULL,
            i_manager_id INTEGER NULL,
            i_product_name CHAR(50) NULL
        )
    "#,
    )
    .await?;

    Ok(())
}
