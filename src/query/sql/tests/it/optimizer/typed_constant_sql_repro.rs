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

use databend_common_exception::Result;

use crate::framework::golden::SqlTestCase;
use crate::framework::golden::setup_context;

#[tokio::test(flavor = "multi_thread", worker_threads = 1)]
async fn test_infer_filter_handles_typed_constant_expr_from_scalar_subquery() -> Result<()> {
    // A scalar subquery can fold to a typed constant. Keep SQL coverage in
    // addition to the local missing-lookup invariant test.
    let case = SqlTestCase {
        name: "infer_filter_typed_constant_expr_from_scalar_subquery",
        description: "A scalar subquery folded to a typed constant must not break infer-filter expression lookups.",
        setup_sqls: &[
            "CREATE TABLE l(a BIGINT NOT NULL)",
            "CREATE TABLE r(a BIGINT NOT NULL)",
        ],
        sql: "SELECT * FROM l LEFT JOIN r ON l.a = r.a WHERE l.a + (SELECT 1) > 5",
    };
    let ctx = setup_context(&case).await?;
    let raw_plan = ctx.bind_sql(case.sql).await?;
    ctx.optimize_plan(raw_plan).await?;
    Ok(())
}
