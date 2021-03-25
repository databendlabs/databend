// Copyright 2020-2021 The FuseQuery Authors.
//
// SPDX-License-Identifier: Apache-2.0.

use rand::Rng;

use crate::clusters::{Cluster, Node};
use crate::configs::Config;
use crate::error::{FuseQueryError, FuseQueryResult};
use crate::rpcs::RpcService;
use crate::sessions::{FuseQueryContextRef, Session, SessionRef};

/// Start services and return the random address.
pub async fn try_start_service(nums: usize) -> FuseQueryResult<Vec<String>> {
    let mut results = vec![];

    for _ in 0..nums {
        let (addr, _) = start_one_service().await?;
        results.push(addr.clone());
    }
    tokio::time::sleep(tokio::time::Duration::from_millis(200)).await;

    Ok(results)
}

// Start service and return the session manager for create his own contexts.
pub async fn try_start_service_with_session_mgr() -> FuseQueryResult<(String, SessionRef)> {
    let (addr, mgr) = start_one_service().await?;
    tokio::time::sleep(tokio::time::Duration::from_millis(200)).await;
    Ok((addr, mgr))
}

// Start a cluster and return the context who has the cluster info.
pub async fn try_create_context_with_nodes(nums: usize) -> FuseQueryResult<FuseQueryContextRef> {
    let addrs = try_start_service(nums).await?;
    let ctx = crate::tests::try_create_context()?;
    for (i, addr) in addrs.iter().enumerate() {
        ctx.try_get_cluster()?.add_node(&Node {
            name: format!("node{}", i),
            cpus: 4,
            priority: 255,
            address: addr.clone(),
            local: false,
        })?;
    }
    Ok(ctx)
}

// Start one random service and get the session manager.
async fn start_one_service() -> FuseQueryResult<(String, SessionRef)> {
    let mut rng = rand::thread_rng();
    let port: u32 = rng.gen_range(10000..11000);
    let addr = format!("127.0.0.1:{}", port);

    let mut conf = Config::default();
    conf.rpc_api_address = addr.clone();

    let cluster = Cluster::create(conf.clone());
    let session_manager = Session::create();
    let srv = RpcService::create(conf, cluster, session_manager.clone());
    tokio::spawn(async move {
        srv.make_server().await?;
        Ok::<(), FuseQueryError>(())
    });
    Ok((addr, session_manager))
}
