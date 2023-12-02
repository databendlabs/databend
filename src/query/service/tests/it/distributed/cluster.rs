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

use std::sync::Arc;
use std::sync::Barrier;
use std::thread;

use common_base::base::tokio;
use common_config::InnerConfig;
use common_exception::ErrorCode;
use common_exception::Result;
use common_expression::DataBlock;
use databend_query::api::RpcService;
use databend_query::test_kits::create_query_context_with_cluster;
use databend_query::test_kits::table_test_fixture::execute_query;
use databend_query::test_kits::ClusterDescriptor;
use databend_query::test_kits::ConfigBuilder;
use databend_query::test_kits::TestGlobalServices;
use futures_util::TryStreamExt;
use tokio::runtime::Builder as TokioRuntimeBuilder;

#[test]
fn test_init_singleton() -> Result<()> {
    let configs = setup_node_configs(vec![
        "0.0.0.0:6061", // Node 1 address
        "0.0.0.0:6062", /* Node 2 address
                         * Add more addresses as needed */
    ]);
    let task_count = configs.len();
    let barrier = Arc::new(Barrier::new(task_count + 1));
    let mut handles = Vec::with_capacity(task_count);

    let cluster_desc = setup_cluster(&configs);

    for (i, conf) in configs.into_iter().enumerate() {
        let barrier_clone = barrier.clone();
        let thread_name = format!("custom-thread-node-{}", i + 1);
        let is_special_node = i == task_count - 1; // Make the last node the special one

        let conf_clone = conf.clone(); // Clone the configuration as well
        let cluster_desc_clone = cluster_desc.clone();

        let handle = thread::Builder::new()
            .name(thread_name)
            .spawn(move || {
                let rt = TokioRuntimeBuilder::new_current_thread()
                    .enable_all()
                    .build()
                    .expect("Failed to create runtime");

                let inner_async = async move {
                    let _guard = TestGlobalServices::setup(&conf_clone).await?;

                    let mut srv = RpcService::create(conf_clone.clone())?;
                    srv.start(conf_clone.query.flight_api_address.parse()?)
                        .await?;

                    if is_special_node {
                        let ctx = create_query_context_with_cluster(cluster_desc_clone).await?;

                        let res = execute_query(ctx, "select * from system.clusters").await?;
                        let blocks = res.try_collect::<Vec<DataBlock>>().await?;
                        println!("blocks: {:?}", blocks);
                    }

                    barrier_clone.wait();
                    Ok::<(), ErrorCode>(())
                };

                if let Err(e) = rt.block_on(inner_async) {
                    eprintln!("Error in async block: {}", e);
                }
            })
            .map_err(|e| ErrorCode::UnknownException(format!("Failed to spawn thread: {}", e)))?;

        handles.push(handle);
    }

    barrier.wait();
    for handle in handles {
        handle.join().expect("Thread failed to complete");
    }

    Ok(())
}

fn setup_node_configs(addresses: Vec<&str>) -> Vec<InnerConfig> {
    addresses
        .into_iter()
        .enumerate()
        .map(|(i, address)| {
            let mut conf = ConfigBuilder::create().build();
            conf.query.flight_api_address = address.to_string();
            conf.query.cluster_id = format!("node{}", i + 1);
            conf
        })
        .collect()
}

fn setup_cluster(configs: &[InnerConfig]) -> ClusterDescriptor {
    let mut cluster_desc = ClusterDescriptor::new();
    for conf in configs.iter() {
        cluster_desc =
            cluster_desc.with_node(&conf.query.cluster_id, &conf.query.flight_api_address);
    }
    cluster_desc
}
