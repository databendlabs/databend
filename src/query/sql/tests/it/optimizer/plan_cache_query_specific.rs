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

use std::collections::HashMap;

use databend_common_catalog::table_context::TableContextSettings;
use databend_common_exception::Result;

use super::column_stat;
use super::table_statistics;
use crate::framework::LiteTableContext;

/// Plans each query with the binder and the production optimizer, and checks whether planning
/// computed a value valid only for this query. The planner cache does not store such plans.
#[tokio::test(flavor = "multi_thread", worker_threads = 1)]
async fn test_planning_records_query_specific_values() -> Result<()> {
    let cases = [
        (false, "SELECT k FROM l WHERE ts > now() - INTERVAL 1 HOUR"),
        (false, "SELECT rand(), k FROM l"),
        (
            false,
            "SELECT l.k FROM l LEFT JOIN r ON l.k = r.k WHERE l.ts > now()",
        ),
        (
            true,
            "SELECT l.k FROM l LEFT JOIN r ON l.k = r.k WHERE r.ts > now()",
        ),
        (true, "SELECT * FROM (VALUES (now())) AS v(n)"),
        (true, "SELECT k < (SELECT rand()) FROM l"),
    ];

    for (expected, sql) in cases {
        let ctx = LiteTableContext::create().await?;
        for table in ["l", "r"] {
            // Statistics on `ts` make cardinality estimation fold `now()`.
            ctx.register_table_sql_with_stats(
                &format!("CREATE TABLE {table} (k INTEGER, ts TIMESTAMP)"),
                Some(table_statistics(1000)),
                HashMap::from([(
                    "ts".to_string(),
                    column_stat(
                        r#"{"min": 0, "max": 86400000000, "ndv": 1000, "null_count": 0}"#,
                        1000,
                    )?,
                )]),
                HashMap::new(),
            )
            .await?;
        }
        let plan = ctx.bind_sql(sql).await?;
        ctx.optimize_plan(plan).await?;
        let recorded = ctx.get_function_context()?.query_specific_values.recorded();
        assert_eq!(recorded, expected, "{sql}");
    }

    Ok(())
}
