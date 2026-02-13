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

#![allow(clippy::collapsible_if)]

use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

use databend_base::uniq_id::GlobalUniq;
use databend_common_base::runtime::Runtime;
use databend_common_exception::Result;
use databend_common_management::*;
use databend_common_meta_store::LocalMetaService;
use databend_common_meta_store::MetaStore;
use databend_common_version::BUILD_INFO;
use databend_meta_kvapi::kvapi::KvApiExt;
use databend_meta_runtime::DatabendRuntime;
use databend_meta_types::MatchSeq;
use databend_meta_types::MatchSeqExt;
use databend_meta_types::NodeInfo;
use databend_meta_types::NodeType;
use databend_meta_types::SeqV;
use tokio::sync::Barrier;

#[tokio::test(flavor = "multi_thread", worker_threads = 1)]
async fn test_empty_id_with_self_managed() -> anyhow::Result<()> {
    let (_kv, warehouse_manager, _nodes) = nodes(Duration::from_secs(60), 0).await?;

    let mut node = system_managed_node(&GlobalUniq::unique());
    node.node_type = NodeType::SelfManaged;
    node.warehouse_id = String::new();
    node.cluster_id = String::from("test_cluster_id");
    let res = warehouse_manager.start_node(node).await;

    assert!(res.is_err());
    assert_eq!(res.unwrap_err().code(), 2403);

    let mut node = system_managed_node(&GlobalUniq::unique());
    node.node_type = NodeType::SelfManaged;
    node.cluster_id = String::new();
    node.warehouse_id = String::from("test_cluster_id");
    let res = warehouse_manager.start_node(node).await;

    assert!(res.is_err());
    assert_eq!(res.unwrap_err().code(), 2403);

    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 1)]
async fn test_successfully_add_self_managed_node() -> anyhow::Result<()> {
    let (kv, warehouse_manager, _nodes) = nodes(Duration::from_secs(60), 0).await?;

    let mut node_info_1 = self_managed_node("test_node_1");
    warehouse_manager.start_node(node_info_1.clone()).await?;
    let node_key = "__fd_clusters_v6/test%2dtenant%2did/online_nodes/test_node_1";
    assert_key_value(&kv, node_key, serde_json::to_vec(&node_info_1)?).await;

    let warehouse_key = "__fd_clusters_v6/test%2dtenant%2did/online_clusters/test%2dcluster%2did/test%2dcluster%2did/test_node_1";

    node_info_1.cluster_id = String::new();
    node_info_1.warehouse_id = String::new();

    assert_key_value(&kv, warehouse_key, serde_json::to_vec(&node_info_1)?).await;

    let info_key = "__fd_warehouses/v1/test%2dtenant%2did/test%2dcluster%2did";
    assert_key_value(
        &kv,
        info_key,
        serde_json::to_vec(&WarehouseInfo::SelfManaged(String::from("test-cluster-id")))?,
    )
    .await;

    let mut node_info_2 = self_managed_node("test_node_2");
    warehouse_manager.start_node(node_info_2.clone()).await?;

    let node_key = "__fd_clusters_v6/test%2dtenant%2did/online_nodes/test_node_2";
    assert_key_value(&kv, node_key, serde_json::to_vec(&node_info_2)?).await;

    let warehouse_key = "__fd_clusters_v6/test%2dtenant%2did/online_clusters/test%2dcluster%2did/test%2dcluster%2did/test_node_2";

    node_info_2.cluster_id = String::new();
    node_info_2.warehouse_id = String::new();

    assert_key_value(&kv, warehouse_key, serde_json::to_vec(&node_info_2)?).await;

    let info_key = "__fd_warehouses/v1/test%2dtenant%2did/test%2dcluster%2did";
    assert_key_value(
        &kv,
        info_key,
        serde_json::to_vec(&WarehouseInfo::SelfManaged(String::from("test-cluster-id")))?,
    )
    .await;

    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 1)]
async fn test_already_exists_add_self_managed_node() -> anyhow::Result<()> {
    let (kv, warehouse_manager, nodes) = nodes(Duration::from_secs(60), 1).await?;

    let node_info = self_managed_node("test_node_1");
    warehouse_manager.start_node(node_info.clone()).await?;

    let node_key = "__fd_clusters_v6/test%2dtenant%2did/online_nodes/test_node_1";
    assert_key_value(&kv, node_key, serde_json::to_vec(&node_info)?).await;

    // add already exists self-managed node and get failure
    match warehouse_manager.start_node(node_info.clone()).await {
        Ok(_) => panic!("Already exists add node must be return Err."),
        Err(cause) => assert_eq!(cause.code(), 2402),
    }

    // add already exists system-managed node and get failure
    match warehouse_manager
        .start_node(self_managed_node(&nodes[0]))
        .await
    {
        Ok(_) => panic!("Already exists add node must be return Err."),
        Err(cause) => assert_eq!(cause.code(), 2402),
    }

    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 1)]
async fn test_successfully_get_self_managed_nodes() -> anyhow::Result<()> {
    let (_kv, warehouse_manager, _nodes) = nodes(Duration::from_mins(60), 0).await?;

    let get_nodes =
        warehouse_manager.list_warehouse_cluster_nodes("test-cluster-id", "test-cluster-id");

    assert_eq!(get_nodes.await?, vec![]);

    let node_1 = self_managed_node("node_1");
    warehouse_manager.start_node(node_1.clone()).await?;

    let get_nodes =
        warehouse_manager.list_warehouse_cluster_nodes("test-cluster-id", "test-cluster-id");

    assert_eq!(get_nodes.await?, vec![node_1.clone()]);

    let node_2 = self_managed_node("node_2");
    warehouse_manager.start_node(node_2.clone()).await?;

    let get_nodes =
        warehouse_manager.list_warehouse_cluster_nodes("test-cluster-id", "test-cluster-id");

    assert_eq!(get_nodes.await?, vec![node_1, node_2]);
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 1)]
async fn test_successfully_drop_self_managed_node() -> anyhow::Result<()> {
    let (_, warehouse_manager, _nodes) = nodes(Duration::from_mins(60), 0).await?;

    let node_info = self_managed_node("test_node");
    warehouse_manager.start_node(node_info.clone()).await?;

    let get_nodes =
        warehouse_manager.list_warehouse_cluster_nodes("test-cluster-id", "test-cluster-id");

    assert_eq!(get_nodes.await?, vec![node_info.clone()]);

    warehouse_manager.shutdown_node(node_info.id).await?;

    let get_nodes =
        warehouse_manager.list_warehouse_cluster_nodes("test-cluster-id", "test-cluster-id");

    assert_eq!(get_nodes.await?, vec![]);
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 1)]
async fn test_unknown_node_drop_self_managed_node() -> anyhow::Result<()> {
    let (_, warehouse_manager, _nodes) = nodes(Duration::from_mins(60), 0).await?;

    match warehouse_manager
        .shutdown_node(String::from("UNKNOWN_ID"))
        .await
    {
        Ok(_) => { /*panic!("Unknown node drop node must be return Err.")*/ }
        Err(cause) => assert_eq!(cause.code(), 2401),
    }

    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 1)]
async fn test_drop_self_managed_warehouse() -> anyhow::Result<()> {
    let (_, warehouse_manager, _nodes) = nodes(Duration::from_mins(60), 0).await?;

    let node_info = self_managed_node("test_node");
    warehouse_manager.start_node(node_info.clone()).await?;

    let drop_warehouse = warehouse_manager.drop_warehouse(String::from("test-cluster-id"));

    assert_eq!(drop_warehouse.await.unwrap_err().code(), 2403);

    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 1)]
async fn test_heartbeat_not_exists_self_managed_node() -> anyhow::Result<()> {
    let (_kv, warehouse_manager, _nodes) = nodes(Duration::from_mins(60), 0).await?;

    let node_info = self_managed_node("test_node");

    // heartbeat not exists node info
    let mut heartbeat_node = node_info.clone();
    let seq = warehouse_manager
        .heartbeat_node(&mut heartbeat_node, 34234)
        .await?;

    assert_eq!(seq, 0);
    assert_eq!(heartbeat_node, node_info);

    let mut heartbeat_node = node_info.clone();
    let seq = warehouse_manager
        .heartbeat_node(&mut heartbeat_node, seq)
        .await?;

    assert_ne!(seq, 0);
    assert_eq!(heartbeat_node, node_info);

    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 1)]
async fn test_successfully_heartbeat_self_managed_node() -> anyhow::Result<()> {
    let (kv, warehouse_manager, _nodes) = nodes(Duration::from_mins(60), 0).await?;

    let mut node_info = self_managed_node("test_node");
    let node = warehouse_manager.start_node(node_info.clone()).await?;

    let info_key = "__fd_clusters_v6/test%2dtenant%2did/online_nodes/test_node";
    assert_key_value(&kv, info_key, serde_json::to_vec(&node_info)?).await;
    assert_key_expire(&kv, info_key, Duration::from_mins(50)).await;

    let warehouse_key = "__fd_clusters_v6/test%2dtenant%2did/online_clusters/test%2dcluster%2did/test%2dcluster%2did/test_node";
    let mut warehouse_node = node_info.clone();
    warehouse_node.cluster_id = String::new();
    warehouse_node.warehouse_id = String::new();
    assert_key_value(&kv, warehouse_key, serde_json::to_vec(&warehouse_node)?).await;
    assert_key_expire(&kv, warehouse_key, Duration::from_mins(50)).await;

    let warehouse_info_key = "__fd_warehouses/v1/test%2dtenant%2did/test%2dcluster%2did";
    let info = serde_json::to_vec(&WarehouseInfo::SelfManaged(String::from("test-cluster-id")))?;
    assert_key_value(&kv, warehouse_info_key, info.clone()).await;
    assert_key_expire(&kv, warehouse_info_key, Duration::from_mins(50)).await;

    warehouse_manager
        .heartbeat_node(&mut node_info, node.seq)
        .await?;
    assert_key_value(&kv, warehouse_info_key, info.clone()).await;
    assert_key_value(&kv, info_key, serde_json::to_vec(&node_info)?).await;
    assert_key_value(&kv, warehouse_key, serde_json::to_vec(&warehouse_node)?).await;
    assert_key_seq(&kv, info_key, MatchSeq::GE(node.seq + 3)).await;
    assert_key_seq(&kv, warehouse_key, MatchSeq::GE(node.seq + 3)).await;
    assert_key_expire(&kv, info_key, Duration::from_mins(50)).await;
    assert_key_expire(&kv, warehouse_key, Duration::from_mins(50)).await;
    assert_key_expire(&kv, warehouse_info_key, Duration::from_mins(50)).await;

    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 1)]
async fn test_empty_system_managed_warehouse() -> anyhow::Result<()> {
    let (_, cluster_mgr, _nodes) = nodes(Duration::from_mins(30), 2).await?;

    let create_warehouse = cluster_mgr.create_warehouse(String::new(), vec![]);

    assert_eq!(create_warehouse.await.unwrap_err().code(), 2403);

    let create_warehouse = cluster_mgr.create_warehouse(String::from("test"), vec![]);

    assert_eq!(create_warehouse.await.unwrap_err().code(), 2408);

    let create_warehouse =
        cluster_mgr.create_warehouse(String::from("test"), vec![SelectedNode::Random(Some(
            String::from("XLargeNode"),
        ))]);

    assert_eq!(create_warehouse.await.unwrap_err().code(), 2404);
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 1)]
async fn test_successfully_create_system_managed_warehouse() -> anyhow::Result<()> {
    let (kv, warehouse_manager, nodes) = nodes(Duration::from_mins(30), 2).await?;

    for node in &nodes {
        let online_node = format!("__fd_clusters_v6/test%2dtenant%2did/online_nodes/{}", node);
        assert_key_seq(&kv, &online_node, MatchSeq::GE(1)).await;
        let warehouse_node = format!(
            "__fd_clusters_v6/test%2dtenant%2did/online_clusters/test%2dcluster%2did/default/{}",
            node
        );
        assert_no_key(&kv, &warehouse_node).await;
    }

    let create_warehouse = warehouse_manager.create_warehouse("test_warehouse".to_string(), vec![
        SelectedNode::Random(None),
        SelectedNode::Random(None),
    ]);

    let cw = create_warehouse.await?;

    assert!(
        !matches!(cw, WarehouseInfo::SelfManaged(_)),
        "Expected WarehouseInfo to not be SelfManaged"
    );
    if let WarehouseInfo::SystemManaged(sw) = cw {
        assert_eq!(sw.id, "test_warehouse");
        for warehouse in warehouse_manager.list_warehouses().await? {
            if let WarehouseInfo::SystemManaged(w) = warehouse {
                if w.id == sw.id {
                    assert_eq!(w.role_id, sw.role_id)
                }
            }
        }
    }

    for node in &nodes {
        let online_node = format!("__fd_clusters_v6/test%2dtenant%2did/online_nodes/{}", node);
        assert_key_seq(&kv, &online_node, MatchSeq::GE(1)).await;
        let warehouse_node = format!(
            "__fd_clusters_v6/test%2dtenant%2did/online_clusters/test_warehouse/default/{}",
            node
        );
        assert_key_seq(&kv, &warehouse_node, MatchSeq::GE(1)).await;
    }

    let get_warehouse_nodes =
        warehouse_manager.list_warehouse_cluster_nodes("test_warehouse", "default");

    let warehouse_nodes = get_warehouse_nodes.await?;

    assert_eq!(warehouse_nodes.len(), 2);

    for warehouse_node in &warehouse_nodes {
        assert!(nodes.contains(&warehouse_node.id));
        assert_eq!(warehouse_node.cluster_id, "default");
        assert_eq!(warehouse_node.warehouse_id, "test_warehouse");
    }

    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 1)]
async fn test_create_system_managed_warehouse_with_offline_node() -> anyhow::Result<()> {
    let (_, warehouse_manager, mut nodes) = nodes(Duration::from_mins(30), 4).await?;

    // mock node offline
    warehouse_manager
        .shutdown_node(nodes[0].to_string())
        .await?;

    let create_warehouse = warehouse_manager.create_warehouse("test_warehouse".to_string(), vec![
        SelectedNode::Random(None),
        SelectedNode::Random(None),
        SelectedNode::Random(None),
        SelectedNode::Random(None),
    ]);

    // no resources available
    assert_eq!(create_warehouse.await.unwrap_err().code(), 2404);

    let create_warehouse = warehouse_manager.create_warehouse("test_warehouse".to_string(), vec![
        SelectedNode::Random(None),
        SelectedNode::Random(None),
        SelectedNode::Random(None),
    ]);

    create_warehouse.await?;

    let get_warehouse_nodes =
        warehouse_manager.list_warehouse_cluster_nodes("test_warehouse", "default");

    let warehouse_nodes = get_warehouse_nodes.await?;

    assert_eq!(warehouse_nodes.len(), 3);

    nodes.remove(0);
    for warehouse_node in &warehouse_nodes {
        assert!(nodes.contains(&warehouse_node.id));
        assert_eq!(warehouse_node.cluster_id, "default");
        assert_eq!(warehouse_node.warehouse_id, "test_warehouse");
    }

    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 1)]
async fn test_create_system_managed_warehouse_with_online_node() -> anyhow::Result<()> {
    let (_, warehouse_manager, mut nodes) = nodes(Duration::from_mins(30), 3).await?;

    let create_warehouse = warehouse_manager.create_warehouse("test_warehouse".to_string(), vec![
        SelectedNode::Random(None),
        SelectedNode::Random(None),
        SelectedNode::Random(None),
        SelectedNode::Random(None),
    ]);

    // no resources available
    assert_eq!(create_warehouse.await.unwrap_err().code(), 2404);

    // mock node online
    let new_node = GlobalUniq::unique();
    warehouse_manager
        .start_node(system_managed_node(&new_node))
        .await?;

    let create_warehouse = warehouse_manager.create_warehouse("test_warehouse".to_string(), vec![
        SelectedNode::Random(None),
        SelectedNode::Random(None),
        SelectedNode::Random(None),
        SelectedNode::Random(None),
    ]);

    create_warehouse.await?;

    let get_warehouse_nodes =
        warehouse_manager.list_warehouse_cluster_nodes("test_warehouse", "default");

    let warehouse_nodes = get_warehouse_nodes.await?;

    assert_eq!(warehouse_nodes.len(), 4);

    nodes.push(new_node);
    for warehouse_node in &warehouse_nodes {
        assert!(nodes.contains(&warehouse_node.id));
        assert_eq!(warehouse_node.cluster_id, "default");
        assert_eq!(warehouse_node.warehouse_id, "test_warehouse");
    }

    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 1)]
async fn test_concurrent_create_warehouse() -> anyhow::Result<()> {
    let (_, warehouse_manager, _nodes) = nodes(Duration::from_mins(30), 9).await?;

    let barrier = Arc::new(Barrier::new(10));
    let warehouse_manager = Arc::new(warehouse_manager);

    let mut runtimes = Vec::with_capacity(10);
    let mut join_handler = Vec::with_capacity(10);
    for idx in 0..10 {
        let runtime = Arc::new(Runtime::with_worker_threads(2, None)?);

        runtimes.push(runtime.clone());

        join_handler.push(runtime.spawn({
            let barrier = barrier.clone();
            let warehouse_manager = warehouse_manager.clone();
            async move {
                let _ = barrier.wait().await;

                let create_warehouse = warehouse_manager.create_warehouse(
                    format!("warehouse_{}", idx),
                    vec![SelectedNode::Random(None); 1],
                );

                create_warehouse.await.is_ok()
            }
        }));
    }

    let create_res = futures::future::try_join_all(join_handler).await?;

    assert_eq!(create_res.len(), 10);
    assert_eq!(create_res.iter().filter(|x| **x).count(), 9);

    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 1)]
async fn test_create_duplicated_warehouse() -> anyhow::Result<()> {
    let (_, warehouse_manager, _nodes) = nodes(Duration::from_mins(30), 2).await?;

    let create_warehouse = warehouse_manager.create_warehouse("test_warehouse".to_string(), vec![
        SelectedNode::Random(None),
    ]);

    create_warehouse.await?;

    let create_warehouse = warehouse_manager.create_warehouse("test_warehouse".to_string(), vec![
        SelectedNode::Random(None),
    ]);

    let res = create_warehouse.await;
    assert!(res.is_err());
    assert_eq!(res.unwrap_err().code(), 2405);

    let create_warehouse = warehouse_manager
        .create_warehouse("test_warehouse_2".to_string(), vec![SelectedNode::Random(
            None,
        )]);

    create_warehouse.await?;

    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 1)]
async fn test_create_warehouse_with_self_manage() -> anyhow::Result<()> {
    let (_, warehouse_manager, _nodes) = nodes(Duration::from_mins(30), 2).await?;

    // Self manage node online
    let mut self_manage_node_1 = self_managed_node("self_manage_node_1");
    self_manage_node_1.cluster_id = String::from("test_warehouse");
    self_manage_node_1.warehouse_id = String::from("test_warehouse");
    warehouse_manager
        .start_node(self_manage_node_1.clone())
        .await?;

    let create_warehouse = warehouse_manager
        .create_warehouse(String::from("test_warehouse"), vec![SelectedNode::Random(
            None,
        )]);

    assert_eq!(create_warehouse.await.unwrap_err().code(), 2405);

    warehouse_manager
        .shutdown_node(self_manage_node_1.id.clone())
        .await?;

    let create_warehouse = warehouse_manager
        .create_warehouse(String::from("test_warehouse"), vec![SelectedNode::Random(
            None,
        )]);

    assert!(create_warehouse.await.is_ok());
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 1)]
async fn test_create_warehouse_with_no_resources() -> anyhow::Result<()> {
    let (_, warehouse_manager, _nodes) = nodes(Duration::from_mins(30), 2).await?;

    let create_warehouse = warehouse_manager
        .create_warehouse("test_warehouse_1".to_string(), vec![SelectedNode::Random(
            None,
        )]);

    create_warehouse.await?;

    let create_warehouse = warehouse_manager
        .create_warehouse("test_warehouse_2".to_string(), vec![SelectedNode::Random(
            None,
        )]);

    create_warehouse.await?;

    let create_warehouse = warehouse_manager
        .create_warehouse("test_warehouse_3".to_string(), vec![SelectedNode::Random(
            None,
        )]);

    let res = create_warehouse.await;

    assert!(res.is_err());
    assert_eq!(res.unwrap_err().code(), 2404);

    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 1)]
async fn test_recovery_with_suspended_warehouse() -> anyhow::Result<()> {
    let (_, warehouse_manager, nodes) = nodes(Duration::from_mins(30), 2).await?;

    let create_warehouse = warehouse_manager.create_warehouse(
        String::from("test_warehouse"),
        vec![SelectedNode::Random(None); 2],
    );
    create_warehouse.await?;

    let list_warehouse_nodes =
        warehouse_manager.list_warehouse_nodes(String::from("test_warehouse"));

    assert_eq!(list_warehouse_nodes.await?.len(), 2);

    warehouse_manager
        .suspend_warehouse(String::from("test_warehouse"))
        .await?;

    let shutdown_node = warehouse_manager.shutdown_node(nodes[0].clone());
    shutdown_node.await?;

    let shutdown_node = warehouse_manager.shutdown_node(nodes[1].clone());
    shutdown_node.await?;

    let list_warehouse_nodes =
        warehouse_manager.list_warehouse_nodes(String::from("test_warehouse"));

    assert_eq!(list_warehouse_nodes.await?.len(), 0);

    let node_1 = GlobalUniq::unique();
    let start_node_1 = warehouse_manager.start_node(system_managed_node(&node_1));
    assert!(start_node_1.await.is_ok());

    let list_warehouse_nodes =
        warehouse_manager.list_warehouse_nodes(String::from("test_warehouse"));

    let nodes = list_warehouse_nodes.await?;
    assert_eq!(nodes.len(), 0);
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 1)]
async fn test_recovery_create_warehouse() -> anyhow::Result<()> {
    let (_, warehouse_manager, nodes) = nodes(Duration::from_mins(30), 2).await?;

    let create_warehouse = warehouse_manager.create_warehouse(
        String::from("test_warehouse"),
        vec![SelectedNode::Random(None); 2],
    );
    create_warehouse.await?;

    let list_warehouse_nodes =
        warehouse_manager.list_warehouse_nodes(String::from("test_warehouse"));

    assert_eq!(list_warehouse_nodes.await?.len(), 2);

    let shutdown_node = warehouse_manager.shutdown_node(nodes[0].clone());
    shutdown_node.await?;

    let shutdown_node = warehouse_manager.shutdown_node(nodes[1].clone());
    shutdown_node.await?;

    let list_warehouse_nodes =
        warehouse_manager.list_warehouse_nodes(String::from("test_warehouse"));

    assert_eq!(list_warehouse_nodes.await?.len(), 0);

    let node_1 = GlobalUniq::unique();
    let start_node_1 = warehouse_manager.start_node(system_managed_node(&node_1));
    assert!(start_node_1.await.is_ok());

    let list_warehouse_nodes =
        warehouse_manager.list_warehouse_nodes(String::from("test_warehouse"));

    let nodes = list_warehouse_nodes
        .await?
        .into_iter()
        .map(|x| x.id)
        .collect::<Vec<_>>();
    assert_eq!(nodes.len(), 1);
    assert!(nodes.contains(&node_1));

    let node_2 = GlobalUniq::unique();
    let mut node_info_2 = system_managed_node(&node_2);
    node_info_2.node_group = Some(String::from("test_group"));
    let start_node_2 = warehouse_manager.start_node(node_info_2);
    assert!(start_node_2.await.is_ok());

    let list_warehouse_nodes =
        warehouse_manager.list_warehouse_nodes(String::from("test_warehouse"));

    let nodes = list_warehouse_nodes
        .await?
        .into_iter()
        .map(|x| x.id)
        .collect::<Vec<_>>();
    assert_eq!(nodes.len(), 2);
    assert!(nodes.contains(&node_1));
    assert!(nodes.contains(&node_2));

    // warehouse is fixed
    let node_3 = GlobalUniq::unique();
    let start_node_3 = warehouse_manager.start_node(system_managed_node(&node_3));
    assert!(start_node_3.await.is_ok());

    let list_warehouse_nodes =
        warehouse_manager.list_warehouse_nodes(String::from("test_warehouse"));

    let nodes = list_warehouse_nodes
        .await?
        .into_iter()
        .map(|x| x.id)
        .collect::<Vec<_>>();
    assert_eq!(nodes.len(), 2);
    assert!(nodes.contains(&node_1));
    assert!(nodes.contains(&node_2));

    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 1)]
async fn test_assign_nodes_for_invalid_warehouse() -> anyhow::Result<()> {
    let (_, warehouse_manager, _nodes) = nodes(Duration::from_mins(30), 2).await?;

    let assign_warehouse_nodes =
        warehouse_manager.assign_warehouse_nodes(String::from(""), HashMap::new());

    assert_eq!(assign_warehouse_nodes.await.unwrap_err().code(), 2403);

    let assign_warehouse_nodes =
        warehouse_manager.assign_warehouse_nodes(String::from("test_warehouse"), HashMap::new());

    assert_eq!(assign_warehouse_nodes.await.unwrap_err().code(), 2408);

    let assign_warehouse_nodes = warehouse_manager.assign_warehouse_nodes(
        String::from("test_warehouse"),
        HashMap::from([(String::new(), vec![])]),
    );

    assert_eq!(assign_warehouse_nodes.await.unwrap_err().code(), 1006);

    let assign_warehouse_nodes = warehouse_manager.assign_warehouse_nodes(
        String::from("test_warehouse"),
        HashMap::from([(String::from("test"), vec![])]),
    );

    assert_eq!(assign_warehouse_nodes.await.unwrap_err().code(), 1006);

    let assign_warehouse_nodes = warehouse_manager.assign_warehouse_nodes(
        String::from("test_warehouse"),
        HashMap::from([(String::from("test"), vec![SelectedNode::Random(None)])]),
    );

    assert_eq!(assign_warehouse_nodes.await.unwrap_err().code(), 2406);

    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 1)]
async fn test_unassign_nodes_for_invalid_warehouse() -> anyhow::Result<()> {
    let (_, warehouse_manager, _nodes) = nodes(Duration::from_mins(30), 2).await?;

    let unassign_warehouse_nodes = warehouse_manager.unassign_warehouse_nodes("", HashMap::new());

    assert_eq!(unassign_warehouse_nodes.await.unwrap_err().code(), 2403);

    let unassign_warehouse_nodes =
        warehouse_manager.unassign_warehouse_nodes("test_warehouse", HashMap::new());

    assert_eq!(unassign_warehouse_nodes.await.unwrap_err().code(), 1006);

    let unassign_warehouse_nodes = warehouse_manager
        .unassign_warehouse_nodes("test_warehouse", HashMap::from([(String::new(), vec![])]));

    assert_eq!(unassign_warehouse_nodes.await.unwrap_err().code(), 1006);

    let unassign_warehouse_nodes = warehouse_manager.unassign_warehouse_nodes(
        "test_warehouse",
        HashMap::from([(String::from("test"), vec![])]),
    );

    assert_eq!(unassign_warehouse_nodes.await.unwrap_err().code(), 1006);

    let unassign_warehouse_nodes = warehouse_manager.unassign_warehouse_nodes(
        "test_warehouse",
        HashMap::from([(String::from("test"), vec![SelectedNode::Random(None)])]),
    );

    assert_eq!(unassign_warehouse_nodes.await.unwrap_err().code(), 2406);

    warehouse_manager
        .create_warehouse(String::from("test_warehouse"), vec![
            SelectedNode::Random(None),
            SelectedNode::Random(None),
        ])
        .await?;

    let unassign_warehouse_nodes = warehouse_manager.unassign_warehouse_nodes(
        "test_warehouse",
        HashMap::from([(String::from("test"), vec![SelectedNode::Random(None)])]),
    );

    assert_eq!(unassign_warehouse_nodes.await.unwrap_err().code(), 2410);

    let unassign_warehouse_nodes = warehouse_manager.unassign_warehouse_nodes(
        "test_warehouse",
        HashMap::from([(String::from("default"), vec![SelectedNode::Random(Some(
            String::from("unknown"),
        ))])]),
    );

    assert_eq!(unassign_warehouse_nodes.await.unwrap_err().code(), 2401);

    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 1)]
async fn test_unassign_all_nodes_for_warehouse() -> anyhow::Result<()> {
    let (_, warehouse_manager, _nodes) = nodes(Duration::from_mins(30), 2).await?;
    warehouse_manager
        .create_warehouse(String::from("test_warehouse"), vec![SelectedNode::Random(
            None,
        )])
        .await?;

    let unassign_warehouse_nodes = warehouse_manager.unassign_warehouse_nodes(
        "test_warehouse",
        HashMap::from([(String::from("default"), vec![
            SelectedNode::Random(None),
            SelectedNode::Random(Some(String::from("test_node_group"))),
        ])]),
    );

    assert_eq!(unassign_warehouse_nodes.await.unwrap_err().code(), 2401);
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 1)]
async fn test_unassign_nodes_for_warehouse() -> anyhow::Result<()> {
    let (_, warehouse_manager, _nodes) = nodes(Duration::from_mins(30), 2).await?;
    let create_warehouse = warehouse_manager
        .create_warehouse(String::from("test_warehouse"), vec![SelectedNode::Random(
            None,
        )]);

    create_warehouse.await?;

    let mut node_1 = system_managed_node(&GlobalUniq::unique());
    node_1.node_group = Some(String::from("test_node_group"));
    warehouse_manager.start_node(node_1.clone()).await?;

    let mut node_2 = system_managed_node(&GlobalUniq::unique());
    node_2.node_group = Some(String::from("test_node_group"));
    warehouse_manager.start_node(node_2.clone()).await?;

    let add_warehouse_cluster = warehouse_manager.add_warehouse_cluster(
        String::from("test_warehouse"),
        String::from("cluster_name"),
        vec![
            SelectedNode::Random(Some(String::from("test_node_group"))),
            SelectedNode::Random(None),
            SelectedNode::Random(Some(String::from("test_node_group"))),
        ],
    );

    add_warehouse_cluster.await?;

    let unassign_warehouse_nodes = warehouse_manager.unassign_warehouse_nodes(
        "test_warehouse",
        HashMap::from([(String::from("cluster_name"), vec![
            SelectedNode::Random(None),
            SelectedNode::Random(Some(String::from("test_node_group"))),
        ])]),
    );

    unassign_warehouse_nodes.await?;

    let nodes = warehouse_manager
        .list_warehouse_cluster_nodes("test_warehouse", "cluster_name")
        .await?;

    assert_eq!(nodes.len(), 1);
    assert!(nodes[0].id == node_1.id || nodes[0].id == node_2.id);

    let add_warehouse_cluster = warehouse_manager.add_warehouse_cluster(
        String::from("test_warehouse"),
        String::from("cluster_name_1"),
        vec![SelectedNode::Random(None), SelectedNode::Random(None)],
    );

    add_warehouse_cluster.await?;

    let unassign_warehouse_nodes = warehouse_manager.unassign_warehouse_nodes(
        "test_warehouse",
        HashMap::from([(String::from("cluster_name_1"), vec![SelectedNode::Random(
            Some(String::from("test_node_group")),
        )])]),
    );

    unassign_warehouse_nodes.await?;

    let nodes = warehouse_manager
        .list_warehouse_cluster_nodes("test_warehouse", "cluster_name_1")
        .await?;

    assert_eq!(nodes.len(), 1);

    let mut node_3 = system_managed_node(&GlobalUniq::unique());
    node_3.node_group = Some(String::from("test_node_group_1"));
    warehouse_manager.start_node(node_3.clone()).await?;

    let mut node_4 = system_managed_node(&GlobalUniq::unique());
    node_4.node_group = Some(String::from("test_node_group_1"));
    warehouse_manager.start_node(node_4.clone()).await?;

    // eprintln!("{:?}", warehouse_manager.list_online_nodes().await?);
    let add_warehouse_cluster = warehouse_manager.add_warehouse_cluster(
        String::from("test_warehouse"),
        String::from("cluster_name_2"),
        vec![
            SelectedNode::Random(Some(String::from("test_node_group_1"))),
            SelectedNode::Random(Some(String::from("test_node_group_1"))),
        ],
    );

    add_warehouse_cluster.await?;

    let unassign_warehouse_nodes = warehouse_manager.unassign_warehouse_nodes(
        "test_warehouse",
        HashMap::from([(String::from("cluster_name_2"), vec![SelectedNode::Random(
            None,
        )])]),
    );

    unassign_warehouse_nodes.await?;
    let nodes = warehouse_manager
        .list_warehouse_cluster_nodes("test_warehouse", "cluster_name_2")
        .await?;

    assert_eq!(nodes.len(), 1);

    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 1)]
async fn test_concurrent_recovery_create_warehouse() -> anyhow::Result<()> {
    let (_, warehouse_manager, nodes) = nodes(Duration::from_mins(30), 2).await?;

    let create_warehouse = warehouse_manager.create_warehouse(
        String::from("test_warehouse"),
        vec![SelectedNode::Random(None); 2],
    );
    create_warehouse.await?;

    let list_warehouse_nodes =
        warehouse_manager.list_warehouse_nodes(String::from("test_warehouse"));

    assert_eq!(list_warehouse_nodes.await?.len(), 2);

    let shutdown_node = warehouse_manager.shutdown_node(nodes[0].clone());
    shutdown_node.await?;

    let shutdown_node = warehouse_manager.shutdown_node(nodes[1].clone());
    shutdown_node.await?;

    let barrier = Arc::new(Barrier::new(10));
    let warehouse_manager = Arc::new(warehouse_manager);

    let mut runtimes = Vec::with_capacity(10);
    let mut join_handler = Vec::with_capacity(10);
    for _idx in 0..10 {
        let runtime = Arc::new(Runtime::with_worker_threads(2, None)?);

        runtimes.push(runtime.clone());

        join_handler.push(runtime.spawn({
            let barrier = barrier.clone();
            let warehouse_manager = warehouse_manager.clone();
            async move {
                let _ = barrier.wait().await;

                let node_id = GlobalUniq::unique();
                let start_node = warehouse_manager.start_node(system_managed_node(&node_id));

                let seq_node = start_node.await.unwrap();
                seq_node.id.clone()
            }
        }));
    }

    let start_res = futures::future::try_join_all(join_handler).await?;

    assert_eq!(start_res.len(), 10);
    let list_warehouse_nodes =
        warehouse_manager.list_warehouse_nodes(String::from("test_warehouse"));
    assert_eq!(list_warehouse_nodes.await?.len(), 2);

    let list_online_nodes = warehouse_manager.list_online_nodes();
    assert_eq!(list_online_nodes.await?.len(), 10);

    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 1)]
async fn test_drop_empty_warehouse() -> anyhow::Result<()> {
    let (_, warehouse_manager, _nodes) = nodes(Duration::from_mins(30), 2).await?;
    let drop_warehouse = warehouse_manager.drop_warehouse(String::new());

    assert_eq!(drop_warehouse.await.unwrap_err().code(), 2403);
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 1)]
async fn test_drop_not_exists_warehouse() -> anyhow::Result<()> {
    let (_, warehouse_manager, _nodes) = nodes(Duration::from_mins(30), 2).await?;
    let drop_warehouse = warehouse_manager.drop_warehouse(String::from("not_exists"));

    assert_eq!(drop_warehouse.await.unwrap_err().code(), 2406);
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 1)]
async fn test_drop_system_managed_warehouse() -> anyhow::Result<()> {
    let (_, warehouse_manager, mut nodes) = nodes(Duration::from_mins(30), 2).await?;
    let create_warehouse =
        warehouse_manager.create_warehouse(String::from("test_warehouse"), vec![
            SelectedNode::Random(None),
            SelectedNode::Random(None),
        ]);

    create_warehouse.await?;

    let drop_warehouse = warehouse_manager.drop_warehouse(String::from("test_warehouse"));

    let cw = drop_warehouse.await?;

    assert!(
        !matches!(cw, WarehouseInfo::SelfManaged(_)),
        "Expected WarehouseInfo to not be SelfManaged"
    );
    if let WarehouseInfo::SystemManaged(sw) = cw {
        assert_eq!(sw.id, "test_warehouse");
        for warehouse in warehouse_manager.list_warehouses().await? {
            if let WarehouseInfo::SystemManaged(w) = warehouse {
                if w.id == sw.id {
                    assert_eq!(w.role_id, sw.role_id)
                }
            }
        }
    }

    let create_warehouse =
        warehouse_manager.create_warehouse(String::from("test_warehouse"), vec![
            SelectedNode::Random(None),
            SelectedNode::Random(None),
        ]);

    // create same name warehouse is successfully
    create_warehouse.await?;

    // mock partial node offline
    warehouse_manager.shutdown_node(nodes.remove(0)).await?;

    let drop_warehouse = warehouse_manager.drop_warehouse(String::from("test_warehouse"));
    drop_warehouse.await?;

    // online node
    let online_node_id = GlobalUniq::unique();
    warehouse_manager
        .start_node(system_managed_node(&online_node_id))
        .await?;
    nodes.push(online_node_id);
    let create_warehouse =
        warehouse_manager.create_warehouse(String::from("test_warehouse"), vec![
            SelectedNode::Random(None),
            SelectedNode::Random(None),
        ]);

    // create same name warehouse is successfully
    create_warehouse.await?;

    // mock all node offline
    warehouse_manager.shutdown_node(nodes.remove(0)).await?;
    warehouse_manager.shutdown_node(nodes.remove(0)).await?;

    let drop_warehouse = warehouse_manager.drop_warehouse(String::from("test_warehouse"));
    drop_warehouse.await?;

    let online_node_id = GlobalUniq::unique();
    warehouse_manager
        .start_node(system_managed_node(&online_node_id))
        .await?;
    nodes.push(online_node_id);

    // create same name warehouse is successfully
    let create_warehouse = warehouse_manager
        .create_warehouse(String::from("test_warehouse"), vec![SelectedNode::Random(
            None,
        )]);
    create_warehouse.await?;

    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 1)]
async fn test_list_warehouses() -> anyhow::Result<()> {
    let (_, warehouse_manager, _nodes) = nodes(Duration::from_mins(30), 10).await?;

    assert_eq!(warehouse_manager.list_warehouses().await?, vec![]);

    let self_managed_node_1 = GlobalUniq::unique();
    warehouse_manager
        .start_node(self_managed_node(&self_managed_node_1))
        .await?;

    let list_warehouse_1 = warehouse_manager.list_warehouses().await?;
    assert_eq!(list_warehouse_1, vec![WarehouseInfo::SelfManaged(
        String::from("test-cluster-id")
    )]);

    let create_warehouse = warehouse_manager
        .create_warehouse(String::from("test_warehouse_1"), vec![
            SelectedNode::Random(None),
        ]);

    create_warehouse.await?;

    let list_warehouses_2 = warehouse_manager.list_warehouses().await?;

    assert_eq!(list_warehouses_2.len(), 2);
    assert_eq!(list_warehouses_2[0], list_warehouse_1[0]);
    let WarehouseInfo::SystemManaged(system_managed_info) = &list_warehouses_2[1] else {
        unreachable!();
    };
    assert!(!system_managed_info.role_id.is_empty());
    assert_eq!(system_managed_info.status, "Running");
    assert_eq!(system_managed_info.id, "test_warehouse_1");
    assert_eq!(
        system_managed_info.clusters,
        HashMap::from([(String::from("default"), SystemManagedCluster {
            nodes: vec![SelectedNode::Random(None)]
        })])
    );

    let self_managed_node_2 = GlobalUniq::unique();
    let mut self_managed_node = self_managed_node(&self_managed_node_2);
    self_managed_node.warehouse_id = String::from("test_warehouse_2");
    warehouse_manager.start_node(self_managed_node).await?;

    let list_warehouses_3 = warehouse_manager.list_warehouses().await?;

    assert_eq!(list_warehouses_3.len(), 3);
    assert_eq!(list_warehouses_3[0], list_warehouses_2[0]);
    assert_eq!(list_warehouses_3[1], list_warehouses_2[1]);
    assert_eq!(
        list_warehouses_3[2],
        WarehouseInfo::SelfManaged(String::from("test_warehouse_2"))
    );

    let create_warehouse = warehouse_manager
        .create_warehouse(String::from("test_warehouse_3"), vec![
            SelectedNode::Random(None),
        ]);

    create_warehouse.await?;

    let mut list_warehouses_4 = warehouse_manager.list_warehouses().await?;

    assert_eq!(list_warehouses_4.len(), 4);
    assert_eq!(list_warehouses_4[0], list_warehouses_3[0]);
    assert_eq!(list_warehouses_4[1], list_warehouses_3[1]);
    assert_eq!(list_warehouses_4[2], list_warehouses_3[2]);

    let WarehouseInfo::SystemManaged(system_managed_info) = &list_warehouses_4[3] else {
        unreachable!();
    };
    assert!(!system_managed_info.role_id.is_empty());
    assert_eq!(system_managed_info.status, "Running");
    assert_eq!(system_managed_info.id, "test_warehouse_3");
    assert_eq!(
        system_managed_info.clusters,
        HashMap::from([(String::from("default"), SystemManagedCluster {
            nodes: vec![SelectedNode::Random(None)]
        })])
    );

    warehouse_manager.shutdown_node(self_managed_node_1).await?;
    list_warehouses_4.remove(0);
    assert_eq!(
        warehouse_manager.list_warehouses().await?,
        list_warehouses_4
    );

    warehouse_manager.shutdown_node(self_managed_node_2).await?;
    list_warehouses_4.remove(1);
    assert_eq!(
        warehouse_manager.list_warehouses().await?,
        list_warehouses_4
    );

    warehouse_manager
        .drop_warehouse(String::from("test_warehouse_1"))
        .await?;
    list_warehouses_4.remove(0);
    assert_eq!(
        warehouse_manager.list_warehouses().await?,
        list_warehouses_4
    );

    // keep show warehouse if all node offline
    let nodes = warehouse_manager
        .list_warehouse_cluster_nodes("test_warehouse_3", "default")
        .await?;
    warehouse_manager.shutdown_node(nodes[0].id.clone()).await?;
    assert_eq!(
        warehouse_manager.list_warehouses().await?,
        list_warehouses_4
    );
    warehouse_manager
        .drop_warehouse(String::from("test_warehouse_3"))
        .await?;
    list_warehouses_4.remove(0);
    assert_eq!(
        warehouse_manager.list_warehouses().await?,
        list_warehouses_4
    );

    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 1)]
async fn test_rename_not_exists_warehouses() -> anyhow::Result<()> {
    let (_, warehouse_manager, _) = nodes(Duration::from_mins(30), 1).await?;
    let rename_warehouse =
        warehouse_manager.rename_warehouse(String::from("test_warehouse"), String::from("aa"));

    assert_eq!(rename_warehouse.await.unwrap_err().code(), 2406);
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 1)]
async fn test_rename_warehouses() -> anyhow::Result<()> {
    let (kv, warehouse_manager, nodes) = nodes(Duration::from_mins(30), 1).await?;

    let self_managed_node_1 = GlobalUniq::unique();
    warehouse_manager
        .start_node(self_managed_node(&self_managed_node_1))
        .await?;

    let rename_warehouse = warehouse_manager.rename_warehouse(
        String::from("test-cluster-id"),
        String::from("test_warehouse"),
    );

    assert_eq!(rename_warehouse.await.unwrap_err().code(), 2403);

    warehouse_manager.shutdown_node(self_managed_node_1).await?;

    let warehouse_node_key = format!(
        "__fd_clusters_v6/test%2dtenant%2did/online_clusters/test_warehouse/default/{}",
        &nodes[0]
    );

    let create_warehouse = warehouse_manager
        .create_warehouse(String::from("test_warehouse"), vec![SelectedNode::Random(
            None,
        )]);

    create_warehouse.await?;

    assert_key_seq(&kv, &warehouse_node_key, MatchSeq::GE(1)).await;
    let list_warehouses = warehouse_manager.list_warehouses().await?;

    let WarehouseInfo::SystemManaged(system_managed_info) = &list_warehouses[0] else {
        unreachable!();
    };

    assert!(!system_managed_info.role_id.is_empty());
    assert_eq!(system_managed_info.status, "Running");
    assert_eq!(system_managed_info.id, "test_warehouse");
    assert_eq!(
        system_managed_info.clusters,
        HashMap::from([(String::from("default"), SystemManagedCluster {
            nodes: vec![SelectedNode::Random(None)]
        })])
    );

    let rename_warehouse = warehouse_manager.rename_warehouse(
        String::from("test_warehouse"),
        String::from("new_test_warehouse"),
    );

    rename_warehouse.await?;

    assert_no_key(&kv, &warehouse_node_key).await;

    let list_warehouses = warehouse_manager.list_warehouses().await?;

    let WarehouseInfo::SystemManaged(system_managed_info) = &list_warehouses[0] else {
        unreachable!();
    };

    assert!(!system_managed_info.role_id.is_empty());
    assert_eq!(system_managed_info.status, "Running");
    assert_eq!(system_managed_info.id, "new_test_warehouse");
    assert_eq!(
        system_managed_info.clusters,
        HashMap::from([(String::from("default"), SystemManagedCluster {
            nodes: vec![SelectedNode::Random(None)]
        })])
    );

    let system_managed_node_2 = GlobalUniq::unique();
    warehouse_manager
        .start_node(system_managed_node(&system_managed_node_2))
        .await?;

    let create_warehouse = warehouse_manager
        .create_warehouse(String::from("test_warehouse"), vec![SelectedNode::Random(
            None,
        )]);

    create_warehouse.await?;

    let list_warehouses_2 = warehouse_manager.list_warehouses().await?;

    assert_eq!(list_warehouses_2[0], list_warehouses[0]);
    let WarehouseInfo::SystemManaged(system_managed_info) = &list_warehouses_2[1] else {
        unreachable!();
    };

    assert!(!system_managed_info.role_id.is_empty());
    assert_eq!(system_managed_info.status, "Running");
    assert_eq!(system_managed_info.id, "test_warehouse");
    assert_eq!(
        system_managed_info.clusters,
        HashMap::from([(String::from("default"), SystemManagedCluster {
            nodes: vec![SelectedNode::Random(None)]
        })])
    );

    let rename_warehouse = warehouse_manager.rename_warehouse(
        String::from("new_test_warehouse"),
        String::from("test_warehouse"),
    );

    assert_eq!(rename_warehouse.await.unwrap_err().code(), 2405);

    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 1)]
async fn test_concurrent_rename_to_warehouse() -> anyhow::Result<()> {
    let (_, warehouse_manager, _nodes) = nodes(Duration::from_mins(30), 9).await?;

    let create_warehouse = warehouse_manager.create_warehouse(
        String::from("test_warehouse"),
        vec![SelectedNode::Random(None); 9],
    );
    create_warehouse.await?;

    let barrier = Arc::new(Barrier::new(10));
    let warehouse_manager = Arc::new(warehouse_manager);

    let mut runtimes = Vec::with_capacity(10);
    let mut join_handler = Vec::with_capacity(10);
    for idx in 0..10 {
        let runtime = Arc::new(Runtime::with_worker_threads(2, None)?);

        runtimes.push(runtime.clone());

        join_handler.push(runtime.spawn({
            let barrier = barrier.clone();
            let warehouse_manager = warehouse_manager.clone();
            async move {
                let _ = barrier.wait().await;

                let rename_warehouse = warehouse_manager.rename_warehouse(
                    String::from("test_warehouse"),
                    format!("test_warehouse_{}", idx),
                );

                rename_warehouse.await.is_ok()
            }
        }));
    }

    let rename_res = futures::future::try_join_all(join_handler).await?;

    assert_eq!(rename_res.len(), 10);
    assert_eq!(rename_res.iter().filter(|x| **x).count(), 1);

    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 1)]
async fn test_concurrent_rename_from_warehouse() -> anyhow::Result<()> {
    let (_, warehouse_manager, _nodes) = nodes(Duration::from_mins(30), 10).await?;

    let barrier = Arc::new(Barrier::new(10));
    let warehouse_manager = Arc::new(warehouse_manager);

    let mut runtimes = Vec::with_capacity(10);
    let mut join_handler = Vec::with_capacity(10);
    for idx in 0..10 {
        let runtime = Arc::new(Runtime::with_worker_threads(2, None)?);

        runtimes.push(runtime.clone());

        join_handler.push(runtime.spawn({
            let barrier = barrier.clone();
            let warehouse_manager = warehouse_manager.clone();
            async move {
                let create_warehouse = warehouse_manager.create_warehouse(
                    format!("test_warehouse_{}", idx),
                    vec![SelectedNode::Random(None); 1],
                );
                create_warehouse.await.unwrap();

                let _ = barrier.wait().await;

                let rename_warehouse = warehouse_manager.rename_warehouse(
                    format!("test_warehouse_{}", idx),
                    String::from("test_warehouse"),
                );

                rename_warehouse.await.is_ok()
            }
        }));
    }

    let rename_res = futures::future::try_join_all(join_handler).await?;

    assert_eq!(rename_res.len(), 10);
    assert_eq!(rename_res.iter().filter(|x| **x).count(), 1);

    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 1)]
async fn test_concurrent_drop_warehouse() -> anyhow::Result<()> {
    let (_, warehouse_manager, _nodes) = nodes(Duration::from_mins(30), 9).await?;

    let create_warehouse = warehouse_manager.create_warehouse(
        String::from("test_warehouse"),
        vec![SelectedNode::Random(None); 9],
    );
    create_warehouse.await?;

    let barrier = Arc::new(Barrier::new(10));
    let warehouse_manager = Arc::new(warehouse_manager);

    let mut runtimes = Vec::with_capacity(10);
    let mut join_handler = Vec::with_capacity(10);
    for _idx in 0..10 {
        let runtime = Arc::new(Runtime::with_worker_threads(2, None)?);

        runtimes.push(runtime.clone());

        join_handler.push(runtime.spawn({
            let barrier = barrier.clone();
            let warehouse_manager = warehouse_manager.clone();
            async move {
                let _ = barrier.wait().await;

                let drop_warehouse =
                    warehouse_manager.drop_warehouse(String::from("test_warehouse"));

                drop_warehouse.await.is_ok()
            }
        }));
    }

    let drop_res = futures::future::try_join_all(join_handler).await?;

    assert_eq!(drop_res.len(), 10);
    assert_eq!(drop_res.iter().filter(|x| **x).count(), 1);

    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 1)]
async fn test_drop_warehouse_cluster_failure() -> anyhow::Result<()> {
    let (_, warehouse_manager, _nodes) = nodes(Duration::from_mins(30), 1).await?;
    let drop_warehouse_cluster = warehouse_manager
        .drop_warehouse_cluster(String::from("test_warehouse"), String::from("test_cluster"));

    assert_eq!(drop_warehouse_cluster.await.unwrap_err().code(), 2406);

    let create_warehouse = warehouse_manager
        .create_warehouse(String::from("test_warehouse"), vec![SelectedNode::Random(
            None,
        )]);

    create_warehouse.await?;

    let drop_warehouse_cluster =
        warehouse_manager.drop_warehouse_cluster(String::from(""), String::from("test_cluster"));

    assert_eq!(drop_warehouse_cluster.await.unwrap_err().code(), 2403);

    let drop_warehouse_cluster =
        warehouse_manager.drop_warehouse_cluster(String::from("test_warehouse"), String::from(""));

    assert_eq!(drop_warehouse_cluster.await.unwrap_err().code(), 2403);

    let drop_warehouse_cluster = warehouse_manager
        .drop_warehouse_cluster(String::from("test_warehouse"), String::from("test_cluster"));

    assert_eq!(drop_warehouse_cluster.await.unwrap_err().code(), 2410);

    let drop_warehouse_cluster = warehouse_manager
        .drop_warehouse_cluster(String::from("test_warehouse"), String::from("default"));

    assert_eq!(drop_warehouse_cluster.await.unwrap_err().code(), 2408);

    warehouse_manager
        .start_node(self_managed_node(&GlobalUniq::unique()))
        .await?;

    let drop_warehouse_cluster = warehouse_manager.drop_warehouse_cluster(
        String::from("test-cluster-id"),
        String::from("test-cluster-id"),
    );

    assert_eq!(drop_warehouse_cluster.await.unwrap_err().code(), 2403);

    Ok(())
}

fn system_managed_node(id: &str) -> NodeInfo {
    NodeInfo {
        id: id.to_string(),
        secret: "".to_string(),
        cpu_nums: 0,
        version: 0,
        http_address: "".to_string(),
        flight_address: "".to_string(),
        discovery_address: "".to_string(),
        binary_version: "".to_string(),
        node_type: NodeType::SystemManaged,
        node_group: None,
        cluster_id: "".to_string(),
        warehouse_id: "".to_string(),
        runtime_node_group: None,
        cache_id: id.to_string(),
    }
}

fn self_managed_node(node_id: &str) -> NodeInfo {
    NodeInfo {
        id: String::from(node_id),
        secret: "".to_string(),
        cpu_nums: 0,
        version: 0,
        http_address: "ip3:port".to_string(),
        flight_address: String::from("ip:port"),
        discovery_address: "ip2:port".to_string(),
        binary_version: "binary_version".to_string(),
        node_type: NodeType::SelfManaged,
        node_group: None,
        cluster_id: "test-cluster-id".to_string(),
        warehouse_id: "test-cluster-id".to_string(),
        runtime_node_group: None,
        cache_id: node_id.to_string(),
    }
}

async fn nodes(lift: Duration, size: usize) -> Result<(MetaStore, WarehouseMgr, Vec<String>)> {
    let (kv_api, cluster_manager) = new_cluster_api(lift).await?;

    let mut nodes = Vec::with_capacity(size);
    for _index in 0..size {
        let name = GlobalUniq::unique();
        cluster_manager
            .start_node(system_managed_node(&name))
            .await?;
        nodes.push(name);
    }

    Ok((kv_api, cluster_manager, nodes))
}

async fn new_cluster_api(lift: Duration) -> Result<(MetaStore, WarehouseMgr)> {
    let test_api = MetaStore::L(Arc::new(
        LocalMetaService::new::<DatabendRuntime>("management-test")
            .await
            .unwrap(),
    ));
    let cluster_manager =
        WarehouseMgr::create(test_api.clone(), "test-tenant-id", lift, &BUILD_INFO)?;
    Ok((test_api, cluster_manager))
}

async fn assert_no_key(kv: &MetaStore, key: &str) {
    let reply = kv.get_kv(key).await.unwrap();

    match reply {
        None => {}
        Some(v) => match v.seq {
            0 => {}
            _ => panic!("assert_no_key {}", key),
        },
    }
}

async fn assert_key_value(kv: &MetaStore, key: &str, value: Vec<u8>) {
    let reply = kv.get_kv(key).await.unwrap();

    match reply {
        Some(SeqV { data, .. }) => {
            assert_eq!(data, value);
        }
        catch => panic!("GetKVActionReply{:?}", catch),
    }
}

async fn assert_key_seq(kv: &MetaStore, key: &str, expect: MatchSeq) {
    let reply = kv.get_kv(key).await.unwrap();

    match reply {
        Some(SeqV { seq, .. }) => {
            assert!(expect.match_seq(&seq).is_ok());
        }
        catch => panic!("GetKVActionReply{:?}", catch),
    }
}

async fn assert_key_expire(kv: &MetaStore, key: &str, lift: Duration) {
    let reply = kv.get_kv(key).await.unwrap();

    match reply {
        Some(SeqV {
            meta: Some(meta), ..
        }) => {
            assert!(meta.get_expire_at_ms().unwrap() >= lift.as_millis() as u64);
        }
        catch => panic!("GetKVActionReply{:?}", catch),
    }
}
