// Copyright 2021 Datafuse Labs.
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

use chrono::DateTime;
use chrono::NaiveDateTime;
use chrono::Utc;
use common_base::tokio;
use common_exception::Result;
use common_meta_types::WarehouseInfo;
use common_meta_types::WarehouseMeta;
use databend_query::storages::system::WarehousesTable;
use databend_query::storages::ToReadDataSourcePlan;
use futures::TryStreamExt;
use pretty_assertions::assert_eq;

#[tokio::test(flavor = "multi_thread", worker_threads = 1)]
async fn test_warehouses_table() -> Result<()> {
    let ctx = crate::tests::create_query_context()?;
    let tenant = ctx.get_tenant();
    ctx.get_settings().set_max_threads(2)?;
    let test_id = "testsasa121id-1888-12-12-11-21-897991";
    let test_name = "fakse&*())(FFH🦀🦀🦀";
    let test_size = "Small";
    let test_instance: u64 = 31;
    let time = DateTime::<Utc>::from_utc(NaiveDateTime::from_timestamp(61, 0), Utc);
    ctx.get_user_manager()
        .create_warehouse(
            &tenant.clone(),
            WarehouseInfo {
                warehouse_id: test_id.to_string(),
                tenant: tenant.clone(),
                meta: WarehouseMeta {
                    warehouse_name: test_name.to_string(),
                    size: test_size.to_string(),
                    instance: test_instance,
                    created_on: time,
                },
            },
            false,
        )
        .await?;

    let table = WarehousesTable::create(1);
    let source_plan = table.read_plan(ctx.clone(), None).await?;

    let stream = table.read(ctx, &source_plan).await?;
    let result = stream.try_collect::<Vec<_>>().await?;
    let block = &result[0];
    assert_eq!(block.num_columns(), 6);

    let expected = vec![
        "+----------------------+-----------+---------------------------------------+----------------+--------------------+-------------------------+",
        "| warehouse_name       | tenant_id | warehouse_id                          | warehouse_size | warehouse_instance | warehouse_creation_time |",
        "+----------------------+-----------+---------------------------------------+----------------+--------------------+-------------------------+",
        "| fakse&*())(FFH🦀🦀🦀 | test      | testsasa121id-1888-12-12-11-21-897991 | Small          | 31                 | 61                      |",
        "+----------------------+-----------+---------------------------------------+----------------+--------------------+-------------------------+",    ];
    common_datablocks::assert_blocks_sorted_eq(expected, result.as_slice());
    Ok(())
}
