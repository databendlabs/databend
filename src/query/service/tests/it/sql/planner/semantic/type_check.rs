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

use databend_common_base::base::tokio;
use databend_common_catalog::table_context::TableContext;
use databend_common_exception::Result;
use databend_common_sql::parse_exprs;
use databend_query::test_kits::TestFixture;

#[tokio::test(flavor = "multi_thread")]
async fn test_query_overflow() -> Result<()> {
    // Construct the SQL query with many OR conditions
    let mut query = String::from("1 = 1 AND (");
    let condition = "(timestamp = '2024-05-05 18:05:20' AND type = '1' AND id = 'xx')";

    for _ in 0..299 {
        // Adjust the count based on your specific test needs
        query.push_str(condition);
        query.push_str(" OR ");
    }
    query.push_str(condition);
    query.push_str(");");

    let fixture = TestFixture::setup().await?;
    let ctx = fixture.new_query_ctx().await?;

    fixture
        .execute_command("CREATE table default.t1(timestamp timestamp, id int, type string);")
        .await?;
    let catalog = ctx.get_catalog("default").await?;
    let table = catalog
        .get_table(&fixture.default_tenant(), "default", "t1")
        .await?;

    parse_exprs(ctx.clone(), table, query.as_str())?;
    Ok(())
}
