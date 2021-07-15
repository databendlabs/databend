// Copyright 2020-2021 The Datafuse Authors.
//
// SPDX-License-Identifier: Apache-2.0.

use common_exception::Result;
use common_management::cluster::ClusterClient;
use common_management::cluster::ClusterExecutor;
use common_runtime::tokio;
use rand::Rng;

use crate::api::RpcService;
use crate::configs::Config;
use crate::sessions::SessionMgr;
use crate::sessions::SessionMgrRef;

/// Start services and return the random address.
pub async fn try_start_service(nums: usize) -> Result<Vec<String>> {
    let mut results = vec![];
    let registry = start_cluster_registry().await?;
    for _ in 0..nums {
        let (conf, _) = start_one_service(registry.clone()).await?;
        results.push(conf.flight_api_address.clone());
    }
    tokio::time::sleep(tokio::time::Duration::from_millis(300)).await;

    Ok(results)
}

// Start service and return the session manager for create his own contexts.
pub async fn try_start_service_with_session_mgr() -> Result<(String, SessionMgrRef)> {
    let (conf, mgr) = start_one_service("".to_string()).await?;
    tokio::time::sleep(tokio::time::Duration::from_millis(200)).await;
    Ok((conf.flight_api_address, mgr))
}

// Start a server as registry.
pub async fn start_cluster_registry() -> Result<String> {
    let conf = Config::default();

    let session_manager = SessionMgr::try_create(100)?;
    let srv = RpcService::create(conf.clone(), session_manager.clone());
    tokio::spawn(async move {
        srv.make_server().await?;
        Result::Ok(())
    });
    Ok(conf.cluster_registry_uri)
}

// Register an executor to the namespace.
pub async fn register_one_executor_to_namespace(
    meta_service_uri: String,
    namespace: String,
    executor: &ClusterExecutor,
) -> Result<()> {
    let cluster_client = ClusterClient::create(meta_service_uri);
    cluster_client.register(namespace, executor).await
}

// Start one random service and get the session manager.
async fn start_one_service(meta_service_uri: String) -> Result<(Config, SessionMgrRef)> {
    let mut conf = Config::default();

    let mut rng = rand::thread_rng();
    let port: u32 = rng.gen_range(10000..11000);
    let flight_api_address = format!("127.0.0.1:{}", port);
    conf.flight_api_address = flight_api_address.clone();
    conf.cluster_registry_uri = meta_service_uri.clone();

    let session_manager = SessionMgr::try_create(100)?;
    let srv = RpcService::create(conf.clone(), session_manager.clone());
    tokio::spawn(async move {
        srv.make_server().await?;
        Result::Ok(())
    });

    // Register to the namespace.
    {
        let conf_cloned = conf.clone();
        let executor = conf_cloned.executor_from_config()?;
        register_one_executor_to_namespace(
            conf_cloned.cluster_namespace,
            conf_cloned.cluster_registry_uri,
            &executor,
        )
        .await?;
    }
    Ok((conf, session_manager))
}
