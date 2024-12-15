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
use std::time::Duration;

use databend_common_base::base::tokio;
use databend_common_base::base::GlobalUniqName;
use databend_common_exception::Result;
use databend_common_management::*;
use databend_common_meta_embedded::MemMeta;
use databend_common_meta_kvapi::kvapi::KVApi;
use databend_common_meta_store::MetaStore;
use databend_common_meta_types::seq_value::SeqV;
use databend_common_meta_types::NodeInfo;

#[tokio::test(flavor = "multi_thread", worker_threads = 1)]
async fn test_successfully_add_node() -> Result<()> {
    let now_ms = SeqV::<()>::now_ms();
    let (kv_api, cluster_api) = new_cluster_api(Duration::from_secs(60)).await?;

    let mut node_info = create_test_node_info();
    cluster_api.add_node(node_info.clone()).await?;
    let online_node = kv_api
        .get_kv("__fd_clusters_v5/test%2dtenant%2did/online_nodes/test_node")
        .await?;

    match online_node {
        Some(SeqV {
            seq: 1,
            meta,
            data: value,
        }) => {
            assert!(meta.unwrap().get_expire_at_ms().unwrap() - now_ms >= 59_000);
            assert_eq!(value, serde_json::to_vec(&node_info)?);
        }
        catch => panic!("GetKVActionReply{:?}", catch),
    }

    let online_cluster = kv_api.get_kv("__fd_clusters_v5/test%2dtenant%2did/online_clusters/test%2dcluster%2did/test%2dcluster%2did/test_node").await?;

    match online_cluster {
        Some(SeqV {
            meta, data: value, ..
        }) => {
            assert!(meta.unwrap().get_expire_at_ms().unwrap() - now_ms >= 59_000);
            node_info.cluster_id = String::new();
            node_info.warehouse_id = String::new();
            assert_eq!(value, serde_json::to_vec(&node_info)?);
        }
        catch => panic!("GetKVActionReply{:?}", catch),
    }

    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 1)]
async fn test_already_exists_add_node() -> Result<()> {
    let (_, cluster_api) = new_cluster_api(Duration::from_secs(60)).await?;

    let node_info = create_test_node_info();
    cluster_api.add_node(node_info.clone()).await?;

    match cluster_api.add_node(node_info.clone()).await {
        Ok(_) => panic!("Already exists add node must be return Err."),
        Err(cause) => assert_eq!(cause.code(), 2402),
    }

    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 1)]
async fn test_successfully_get_nodes() -> Result<()> {
    let (_, cluster_api) = new_cluster_api(Duration::from_secs(60)).await?;

    let nodes = cluster_api
        .get_nodes("test-cluster-id", "test-cluster-id")
        .await?;
    assert_eq!(nodes, vec![]);

    let node_info = create_test_node_info();
    cluster_api.add_node(node_info.clone()).await?;

    let nodes = cluster_api
        .get_nodes("test-cluster-id", "test-cluster-id")
        .await?;
    assert_eq!(nodes, vec![node_info]);
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 1)]
async fn test_successfully_drop_node() -> Result<()> {
    let (_, cluster_api) = new_cluster_api(Duration::from_secs(60)).await?;

    let node_info = create_test_node_info();
    cluster_api.add_node(node_info.clone()).await?;

    let nodes = cluster_api
        .get_nodes("test-cluster-id", "test-cluster-id")
        .await?;
    assert_eq!(nodes, vec![node_info.clone()]);

    cluster_api.drop_node(node_info.id).await?;

    let nodes = cluster_api
        .get_nodes("test-cluster-id", "test-cluster-id")
        .await?;
    assert_eq!(nodes, vec![]);
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 1)]
async fn test_unknown_node_drop_node() -> Result<()> {
    let (_, cluster_api) = new_cluster_api(Duration::from_secs(60)).await?;

    match cluster_api.drop_node(String::from("UNKNOWN_ID")).await {
        Ok(_) => { /*panic!("Unknown node drop node must be return Err.")*/ }
        Err(cause) => assert_eq!(cause.code(), 2401),
    }

    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 1)]
async fn test_successfully_heartbeat_node() -> Result<()> {
    let now_ms = SeqV::<()>::now_ms();
    let (kv_api, cluster_api) = new_cluster_api(Duration::from_secs(60)).await?;

    let mut node_info = create_test_node_info();
    let res = cluster_api.add_node(node_info.clone()).await?;

    for key in [
        "__fd_clusters_v5/test%2dtenant%2did/online_nodes/test_node",
        "__fd_clusters_v5/test%2dtenant%2did/online_clusters/test%2dcluster%2did/test%2dcluster%2did/test_node",
    ] {
        let value = kv_api.get_kv(key).await?;
        let meta = value.unwrap().meta.unwrap();
        let expire_ms = meta.get_expire_at_ms().unwrap();
        assert!(expire_ms - now_ms >= 59_000);
    }

    let now_ms = SeqV::<()>::now_ms();
    cluster_api.heartbeat(&mut node_info, res).await?;

    for key in [
        "__fd_clusters_v5/test%2dtenant%2did/online_nodes/test_node",
        "__fd_clusters_v5/test%2dtenant%2did/online_clusters/test%2dcluster%2did/test%2dcluster%2did/test_node",
    ] {
        let value = kv_api.get_kv(key).await?;
        assert!(value.unwrap().meta.unwrap().get_expire_at_ms().unwrap() - now_ms >= 59_000);
    }

    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 1)]
async fn test_successfully_create_warehouse() -> Result<()> {
    let (_, cluster_mgr, nodes) = nodes(Duration::from_mins(30), 2).await?;

    let create_warehouse = cluster_mgr.create_warehouse("test_warehouse".to_string(), vec![
        SelectedNode::Random(None),
        SelectedNode::Random(None),
    ]);

    create_warehouse.await?;

    let get_warehouse_nodes = cluster_mgr.get_nodes("test_warehouse", "test_warehouse");

    let warehouse_nodes = get_warehouse_nodes.await?;

    assert_eq!(warehouse_nodes.len(), 2);
    assert_eq!(warehouse_nodes.last().map(|x| &x.id), nodes.last());
    assert_eq!(warehouse_nodes.first().map(|x| &x.id), nodes.first());

    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 1)]
async fn test_create_duplicated_warehouse() -> Result<()> {
    let (_, cluster_mgr, _nodes) = nodes(Duration::from_mins(30), 2).await?;

    let create_warehouse =
        cluster_mgr.create_warehouse("test_warehouse".to_string(), vec![SelectedNode::Random(
            None,
        )]);

    create_warehouse.await?;

    let create_warehouse =
        cluster_mgr.create_warehouse("test_warehouse".to_string(), vec![SelectedNode::Random(
            None,
        )]);

    let res = create_warehouse.await;
    assert!(res.is_err());
    assert_eq!(res.unwrap_err().code(), 2405);

    let create_warehouse =
        cluster_mgr.create_warehouse("test_warehouse_2".to_string(), vec![SelectedNode::Random(
            None,
        )]);

    create_warehouse.await?;

    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 1)]
async fn test_create_warehouse_with_no_resources() -> Result<()> {
    let (_, cluster_mgr, _nodes) = nodes(Duration::from_mins(30), 2).await?;

    let create_warehouse =
        cluster_mgr.create_warehouse("test_warehouse_1".to_string(), vec![SelectedNode::Random(
            None,
        )]);

    create_warehouse.await?;

    let create_warehouse =
        cluster_mgr.create_warehouse("test_warehouse_2".to_string(), vec![SelectedNode::Random(
            None,
        )]);

    create_warehouse.await?;

    let create_warehouse =
        cluster_mgr.create_warehouse("test_warehouse_3".to_string(), vec![SelectedNode::Random(
            None,
        )]);

    let res = create_warehouse.await;

    assert!(res.is_err());
    assert_eq!(res.unwrap_err().code(), 2404);

    Ok(())
}

fn empty_node(id: &str) -> NodeInfo {
    NodeInfo {
        id: id.to_string(),
        secret: "".to_string(),
        cpu_nums: 0,
        version: 0,
        http_address: "".to_string(),
        flight_address: "".to_string(),
        discovery_address: "".to_string(),
        binary_version: "".to_string(),
        cluster_id: "".to_string(),
        warehouse_id: "".to_string(),
    }
}

fn create_test_node_info() -> NodeInfo {
    NodeInfo {
        id: String::from("test_node"),
        secret: "".to_string(),
        cpu_nums: 0,
        version: 0,
        http_address: "ip3:port".to_string(),
        flight_address: String::from("ip:port"),
        discovery_address: "ip2:port".to_string(),
        binary_version: "binary_version".to_string(),
        cluster_id: "test-cluster-id".to_string(),
        warehouse_id: "test-cluster-id".to_string(),
    }
}

async fn nodes(lift: Duration, size: usize) -> Result<(MetaStore, WarehouseMgr, Vec<String>)> {
    let (kv_api, cluster_manager) = new_cluster_api(lift).await?;

    let mut nodes = Vec::with_capacity(size);
    for _index in 0..size {
        let name = GlobalUniqName::unique();
        cluster_manager.add_node(empty_node(&name)).await?;
        nodes.push(name);
    }

    Ok((kv_api, cluster_manager, nodes))
}

async fn new_cluster_api(lift: Duration) -> Result<(MetaStore, WarehouseMgr)> {
    let test_api = MetaStore::L(Arc::new(MemMeta::default()));
    let cluster_manager = WarehouseMgr::create(test_api.clone(), "test-tenant-id", lift)?;
    Ok((test_api, cluster_manager))
}
