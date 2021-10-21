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
use common_planners::TruncateTablePlan;

use crate::catalogs::Catalog;
use crate::datasources::table::fuse::table_test_fixture::TestFixture;

#[tokio::test]
async fn test_fuse_table_truncate_retry() -> Result<()> {
    let fixture = TestFixture::new();
    let ctx = fixture.ctx();

    let crate_table_plan = fixture.default_crate_table_plan();
    let catalog = ctx.get_catalog();
    catalog.create_table(crate_table_plan)?;

    let table = catalog.get_table(
        fixture.default_db().as_str(),
        fixture.default_table().as_str(),
    )?;

    let retried_truncator = table.clone();
    assert_eq!(0, retried_truncator.get_table_info().version);

    let io_ctx = Arc::new(ctx.get_single_node_table_io_context()?);
    let truncate_plan = TruncateTablePlan {
        db: "".to_string(),
        table: "".to_string(),
    };

    // "insert" a truncation operation before we truncate the table with an old version
    {
        let insert_into_plan = fixture.insert_plan_of_table(table.as_ref(), 10);
        table.append_data(io_ctx.clone(), insert_into_plan).await?;

        // get the lasted table
        let table = catalog.get_table_by_id(table.get_id(), None)?;
        // no retry here, since we are using the latest table
        table
            .truncate(io_ctx.clone(), truncate_plan.clone())
            .await?;
    }

    // truncate with test table, retry do occur
    let r = retried_truncator
        .truncate(io_ctx.clone(), truncate_plan)
        .await;
    assert!(r.is_ok());

    // get the latest table
    let table = catalog.get_table_by_id(table.get_id(), None)?;
    let latest_version = table.get_table_info().version;

    // version has been bumped to at least 3 : one "insert", one "truncate" and another "truncate"
    assert!((latest_version >= retried_truncator.get_table_info().version + 3));
    let (stats, parts) = table.read_partitions(io_ctx.clone(), None, None)?;
    assert_eq!(parts.len(), 0);
    assert_eq!(stats.read_rows, 0);

    Ok(())
}
