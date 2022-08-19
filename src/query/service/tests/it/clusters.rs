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
use std::time::Duration;
use once_cell::sync::OnceCell;
use common_base::base::tokio;
use common_exception::{ErrorCode, Result};
use databend_query::clusters::ClusterDiscovery;
use databend_query::clusters::ClusterHelper;
use pretty_assertions::assert_eq;
use common_base::base::tokio::runtime::Handle;
use common_catalog::cluster_info::Cluster;
use databend_query::GlobalServices;
use crate::tests::{ConfigBuilder, TestGlobalServices};

#[tokio::test(flavor = "multi_thread", worker_threads = 1)]
async fn test_single_cluster_discovery() -> Result<()> {
    TestGlobalServices::setup(crate::tests::ConfigBuilder::create().build()).await?;

    // ClusterDiscovery::init()
    let discover_cluster = ClusterDiscovery::instance().discover().await?;

    let discover_cluster_nodes = discover_cluster.get_nodes();
    assert_eq!(discover_cluster_nodes.len(), 1);
    assert!(discover_cluster.is_empty());
    assert!(discover_cluster.is_local(&discover_cluster_nodes[0]));
    Ok(())
}

// TODO:(Winter) need KVApi for cluster multiple nodes test
// #[tokio::test(flavor = "multi_thread", worker_threads = 1)]
// async fn test_multiple_cluster_discovery() -> Result<()> {
//     let mut config = Config::default();
//     config.query.tenant = String::from("tenant_id");
//     config.query.namespace = String::from("namespace_id");
//
//     let cluster_discovery_1 = ClusterDiscovery::create_global(config.clone()).await?;
//     let cluster_discovery_2 = ClusterDiscovery::create_global(config.clone()).await?;
//
//     cluster_discovery_1.register_to_metastore(&config).await?;
//     cluster_discovery_2.register_to_metastore(&config).await?;
//
//     let discover_cluster_1 = cluster_discovery_1.discover().await?;
//     let discover_cluster_2 = cluster_discovery_2.discover().await?;
//
//     let discover_cluster_nodes_1 = discover_cluster_1.get_nodes();
//     let discover_cluster_nodes_2 = discover_cluster_2.get_nodes();
//
//     assert_eq!(discover_cluster_nodes_1.len(), 2);
//     assert_eq!(discover_cluster_nodes_2.len(), 2);
//     assert_eq!(discover_cluster_1.is_empty(), false);
//     assert_eq!(discover_cluster_2.is_empty(), false);
//     assert_eq!(discover_cluster_1.is_local(&discover_cluster_nodes_1[0]) || discover_cluster_1.is_local(&discover_cluster_nodes_1[1]), true);
//     assert_eq!(discover_cluster_1.is_local(&discover_cluster_nodes_1[0]) && discover_cluster_1.is_local(&discover_cluster_nodes_1[1]), false);
//     assert_eq!(discover_cluster_2.is_local(&discover_cluster_nodes_2[0]) || discover_cluster_1.is_local(&discover_cluster_nodes_2[1]), true);
//     assert_eq!(discover_cluster_2.is_local(&discover_cluster_nodes_2[0]) && discover_cluster_1.is_local(&discover_cluster_nodes_2[1]), false);
//
//     assert_eq!(discover_cluster_nodes_1, discover_cluster_nodes_2);
//     Ok(())
// }
//
// #[async_trait::async_trait]
// trait Singleton {
//     async fn try_create() -> Result<Arc<Self>>;
//
//     async fn init() -> Result<()> {
//         static INSTANTS: OnceCell<Arc<Self>> = OnceCell::new();
//
//         match INSTANTS.set(Self::try_create()?) {
//             Ok(_) => Ok(()),
//             Err(_) => Err(ErrorCode::LogicalError("Cannot init GlobalRuntime twice")),
//         }
//     }
// }
//
//
// struct S {}
//
// #[async_trait::async_trait]
// impl Singleton for S {
//     async fn try_create() -> Result<Arc<Self>> {
//         Ok(Arc::new(S {}))
//     }
// }

// #[tokio::test(flavor = "multi_thread", worker_threads = 10)]
// async fn test_singleton() -> Result<()> {
//     println!("thread name {:?}", std::thread::current().name());
//     GlobalServices::init(ConfigBuilder::create().build()).await?;
//     // tokio::task_local! {
//     //     static UNIQUE_NAME: &str;
//     // }
//
//     // UNIQUE_NAME.sync_scope(1, || {
//     //     println!("task local value: {}", NUMBER.get());
//     // });
//
//     // let handler = tokio::runtime::Handle::current();
//
//     // println!("first address: {}", handler.metrics().num_workers());
//
//     // tokio::time::sleep(Duration::from_secs(2)).await;
//
//     // let handler = tokio::runtime::Handle::current();
//     // println!("second address: {}", handler as *const Handle as usize);
//     Ok(())
// }
