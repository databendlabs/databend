// Copyright 2023 Datafuse Labs.
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

use common_base::base::tokio;
use databend_query::test_kits::TestFixture;
use common_exception::Result;
use crate::sql::planner::optimizer::agg_index_query_rewrite::plan_sql;

#[tokio::test(flavor = "multi_thread")]
async fn test_subquery() -> Result<()> {
    let fixture = TestFixture::new().await?;
    let ctx = fixture.new_query_ctx().await?;
    let query = "SELECT (SELECT (SELECT customer_id FROM c1 WHERE c2.segment = c1.segment) from sales) FROM c2;";
    let (query, _, metadata) = plan_sql(ctx.clone(), query, true).await?;
    Ok(())
}