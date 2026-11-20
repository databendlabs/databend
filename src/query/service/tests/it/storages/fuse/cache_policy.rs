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

use databend_common_catalog::plan::Projection;
use databend_common_exception::Result;
use databend_common_expression::DataBlock;
use databend_common_storages_fuse::FuseTable;
use databend_query::sessions::TableContext;
use databend_query::sessions::TableContextCachePolicy;
use databend_query::test_kits::*;
use databend_storages_common_cache::CacheFill;
use databend_storages_common_cache::CachePolicy;
use futures::TryStreamExt;

/// A maintenance statement sets the policy of its context to `CachePolicy::MAINTENANCE`
/// before planning its reads; a query keeps the default.
#[tokio::test(flavor = "multi_thread")]
async fn test_maintenance_statements_set_their_policy() -> Result<()> {
    let fixture = TestFixture::setup().await?;
    fixture.create_default_database().await?;
    fixture.create_default_table().await?;
    let table = format!(
        "{}.{}",
        fixture.default_db_name(),
        fixture.default_table_name()
    );
    for i in 0..3 {
        fixture
            .execute_command(&format!("insert into {table}(id) values({i})"))
            .await?;
    }
    for (sql, expected) in [
        (format!("select * from {table}"), CachePolicy::DEFAULT),
        (format!("analyze table {table}"), CachePolicy::MAINTENANCE),
        (
            format!("optimize table {table} compact segment"),
            CachePolicy::MAINTENANCE,
        ),
        (
            format!("optimize table {table} compact"),
            CachePolicy::MAINTENANCE,
        ),
    ] {
        let ctx = fixture.new_query_ctx().await?;
        TestFixture::execute_query_with_ctx(ctx.clone(), &sql)
            .await?
            .try_collect::<Vec<DataBlock>>()
            .await?;
        assert_eq!(ctx.cache_policy(), expected, "{sql}");
    }
    Ok(())
}

/// Readers built from a context read under the policy of its statement as well as their own:
/// a call can narrow what its statement allows, never widen it.
#[tokio::test(flavor = "multi_thread")]
async fn test_readers_take_the_policy_of_their_statement() -> Result<()> {
    let fixture = TestFixture::setup().await?;
    fixture.create_default_database().await?;
    fixture.create_default_table().await?;
    let table = fixture.latest_default_table().await?;
    let fuse = FuseTable::try_from_table(table.as_ref())?;
    let ctx = fixture.new_query_ctx().await?;
    let background_maintenance = CachePolicy {
        memory: CacheFill::ReadOnly,
        disk: CacheFill::Bypass,
    };
    for (statement, call, expected) in [
        (
            CachePolicy::DEFAULT,
            CachePolicy::DEFAULT,
            CachePolicy::DEFAULT,
        ),
        (
            CachePolicy::DEFAULT,
            CachePolicy::READ_ONLY,
            CachePolicy::READ_ONLY,
        ),
        (
            CachePolicy::MAINTENANCE,
            CachePolicy::DEFAULT,
            CachePolicy::MAINTENANCE,
        ),
        (
            CachePolicy::MAINTENANCE,
            CachePolicy::READ_ONLY,
            background_maintenance,
        ),
        (
            CachePolicy::BYPASS,
            CachePolicy::DEFAULT,
            CachePolicy::BYPASS,
        ),
    ] {
        ctx.set_cache_policy(statement);
        let table_ctx: Arc<dyn TableContext> = ctx.clone();
        let reader = fuse.create_block_reader(table_ctx, Projection::Columns(vec![0]), call)?;
        assert_eq!(reader.policy, expected, "{statement:?} and {call:?}");
    }
    Ok(())
}
