//  Copyright 2021 Datafuse Labs.
//
//  Licensed under the Apache License, Version 2.0 (the "License");
//  you may not use this file except in compliance with the License.
//  You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
//  Unless required by applicable law or agreed to in writing, software
//  distributed under the License is distributed on an "AS IS" BASIS,
//  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
//  See the License for the specific language governing permissions and
//  limitations under the License.
//

use std::sync::Arc;

use common_base::tokio;
use common_exception::Result;

use crate::catalogs::Catalog;
use crate::datasources::table::fuse::table_test_fixture::TestFixture;

#[tokio::test]
async fn test_fuse_table_append_retry() -> Result<()> {
    let fixture = TestFixture::new();
    let ctx = fixture.ctx();

    let crate_table_plan = fixture.default_crate_table_plan();
    let catalog = ctx.get_catalog();
    catalog.create_table(crate_table_plan)?;

    let table = catalog.get_table(
        fixture.default_db().as_str(),
        fixture.default_table().as_str(),
    )?;

    let retried_appender = table.clone();
    assert_eq!(0, retried_appender.get_table_info().version);

    let io_ctx = Arc::new(ctx.get_single_node_table_io_context()?);

    // "insert" a append operation before we truncate the table with an old version
    {
        let insert_into_plan = fixture.insert_plan_of_table(table.as_ref(), 10);
        table.append_data(io_ctx.clone(), insert_into_plan).await?;
    }

    // append data with staled version
    let insert_into_plan = fixture.insert_plan_of_table(table.as_ref(), 10);
    let r = retried_appender
        .append_data(io_ctx.clone(), insert_into_plan)
        .await;
    assert!(r.is_ok());

    // get the latest table
    let table = catalog.get_table(
        fixture.default_db().as_str(),
        fixture.default_table().as_str(),
    )?;
    let latest_version = table.get_table_info().version;

    // version has been bumped to 2
    assert_eq!(
        latest_version,
        retried_appender.get_table_info().version + 2
    );
    let (stats, parts) = table.read_partitions(io_ctx.clone(), None, None)?;
    assert_eq!(parts.len(), 2 * 10);
    assert_eq!(stats.read_rows, 3 * (10 + 10));

    Ok(())
}
