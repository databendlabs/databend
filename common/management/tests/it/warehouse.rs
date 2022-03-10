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

use std::sync::Arc;

use common_base::tokio;
use common_exception::Result;
use common_management::*;
use common_meta_api::KVApi;
use common_meta_embedded::MetaEmbedded;
use common_meta_types::SeqV;
use common_meta_types::WarehouseInfo;

#[tokio::test(flavor = "multi_thread", worker_threads = 1)]
async fn test_create_warehouse() -> Result<()> {
    let (kv_api, warehouse_api) = new_warehouse_api().await?;

    let warehouse_info = create_test_warehouse_info();
    warehouse_api
        .create_warehouse(warehouse_info.clone())
        .await?;
    let value = kv_api.get_kv("__fd_warehouses/tenant1/test_warehouseHB(YYYO&%^$*_*^^&*%&^$%^#$%))*&*~!?? 🐮  🐮  🐮 ").await?;
    match value {
        Some(SeqV {
            seq: 1,
            meta: _,
            data: value,
        }) => {
            assert_eq!(value, serde_json::to_vec(&warehouse_info)?);
        }
        catch => panic!("GetKVActionReply{:?}", catch),
    }

    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 1)]
async fn test_already_exists_create_warehouse() -> Result<()> {
    let (_, warehouse_api) = new_warehouse_api().await?;

    let warehouse_info = create_test_warehouse_info();
    warehouse_api
        .create_warehouse(warehouse_info.clone())
        .await?;

    match warehouse_api.create_warehouse(warehouse_info.clone()).await {
        Ok(_) => panic!("Already exists create warehouse must be return Err."),
        // 2902 represents warehouse already exists error
        Err(cause) => assert_eq!(cause.code(), 2902),
    }

    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 1)]
async fn test_successfully_get_warehouses() -> Result<()> {
    let (_, warehouse_api) = new_warehouse_api().await?;

    let warehouses = warehouse_api.get_warehouses().await?;
    assert_eq!(warehouses, vec![]);
    let warehouse_info = create_test_warehouse_info();
    warehouse_api
        .create_warehouse(warehouse_info.clone())
        .await?;

    let warehouses = warehouse_api.get_warehouses().await?;
    assert_eq!(warehouses[0].data, warehouse_info);
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 1)]
async fn test_successfully_get_warehouse_without_seq() -> Result<()> {
    let (_, warehouse_api) = new_warehouse_api().await?;

    let warehouses = warehouse_api.get_warehouses().await?;
    assert_eq!(warehouses, vec![]);
    let warehouse_info = create_test_warehouse_info();
    warehouse_api
        .create_warehouse(warehouse_info.clone())
        .await?;

    let warehouse = warehouse_api
        .get_warehouse(warehouse_info.meta.warehouse_name.as_str(), None)
        .await?;
    assert_eq!(warehouse.data, warehouse_info);
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 1)]
async fn test_get_not_exist_warehouse() -> Result<()> {
    let (_, warehouse_api) = new_warehouse_api().await?;

    let warehouses = warehouse_api.get_warehouses().await?;
    assert_eq!(warehouses, vec![]);
    let warehouse_info = create_test_warehouse_info();

    match warehouse_api
        .get_warehouse(warehouse_info.meta.warehouse_name.as_str(), None)
        .await
    {
        Ok(_) => panic!("There is no warehouse exists"),
        // 2901 represents warehouse not exists error
        Err(cause) => assert_eq!(cause.code(), 2901),
    }
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 1)]
async fn test_successfully_update_and_warehouse_without_seq() -> Result<()> {
    let (_, warehouse_api) = new_warehouse_api().await?;

    let warehouses = warehouse_api.get_warehouses().await?;
    assert_eq!(warehouses, vec![]);
    let warehouse_info = create_test_warehouse_info();
    warehouse_api
        .create_warehouse(warehouse_info.clone())
        .await?;

    let warehouse = warehouse_api
        .get_warehouse(warehouse_info.meta.warehouse_name.as_str(), None)
        .await?;
    assert_eq!(warehouse.data, warehouse_info);
    let ack = warehouse_api
        .update_warehouse_instances(warehouse_info.meta.warehouse_name.as_str(), 3, None)
        .await?;
    assert_eq!(ack, 2);
    let warehouse = warehouse_api
        .get_warehouse(warehouse_info.meta.warehouse_name.as_str(), None)
        .await?;
    assert_eq!(warehouse.data.meta.instance, 3);
    let ack = warehouse_api
        .update_warehouse_instances(warehouse_info.meta.warehouse_name.as_str(), 0, None)
        .await?;
    assert_eq!(ack, 3);
    let warehouse = warehouse_api
        .get_warehouse(warehouse_info.meta.warehouse_name.as_str(), None)
        .await?;
    assert_eq!(warehouse.data.meta.instance, 0);
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 1)]
async fn test_drop_warehouse() -> Result<()> {
    let (_, warehouse_api) = new_warehouse_api().await?;

    let warehouses = warehouse_api.get_warehouses().await?;
    assert_eq!(warehouses, vec![]);
    let warehouse_info = create_test_warehouse_info();
    warehouse_api
        .create_warehouse(warehouse_info.clone())
        .await?;
    warehouse_api
        .drop_warehouse(warehouse_info.meta.warehouse_name.as_str(), None)
        .await?;
    let warehouses = warehouse_api.get_warehouses().await?;
    assert_eq!(warehouses, vec![]);

    Ok(())
}

fn create_test_warehouse_info() -> WarehouseInfo {
    WarehouseInfo::new(
        "tenant1",
        "test_warehouseHB(YYYO&%^$*_*^^&*%&^$%^#$%))*&*~!?? 🐮  🐮  🐮 ",
        "Small",
        12,
    )
}

async fn new_warehouse_api() -> Result<(Arc<MetaEmbedded>, WarehouseMgr)> {
    let test_api = Arc::new(MetaEmbedded::new_temp().await?);
    let mgr = WarehouseMgr::create(test_api.clone(), "tenant1")?;
    Ok((test_api, mgr))
}
