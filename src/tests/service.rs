// Copyright 2020-2021 The FuseQuery Authors.
//
// SPDX-License-Identifier: Apache-2.0.

use rand::Rng;

use crate::clusters::{Cluster, Node};
use crate::configs::Config;
use crate::error::{FuseQueryError, FuseQueryResult};
use crate::rpcs::RpcService;
use crate::sessions::{FuseQueryContextRef, Session};

/// Start services and return the random address.
pub async fn try_start_service(nums: usize) -> FuseQueryResult<Vec<String>> {
    let mut results = vec![];

    for _ in 0..nums {
        let mut rng = rand::thread_rng();
        let port: u32 = rng.gen_range(10000..11000);
        let addr = format!("127.0.0.1:{}", port);
        results.push(addr.clone());

        let mut conf = Config::default();
        conf.rpc_api_address = addr;

        let cluster = Cluster::create(conf.clone());
        let session_manager = Session::create();
        let srv = RpcService::create(conf.clone(), cluster.clone(), session_manager.clone());
        tokio::spawn(async move {
            srv.make_server().await?;
            Ok::<(), FuseQueryError>(())
        });
    }
    tokio::time::sleep(tokio::time::Duration::from_millis(200)).await;

    Ok(results)
}

pub async fn try_create_context_with_nodes(nums: usize) -> FuseQueryResult<FuseQueryContextRef> {
    let addrs = try_start_service(nums).await?;
    let ctx = crate::tests::try_create_context()?;
    for (i, addr) in addrs.iter().enumerate() {
        ctx.try_get_cluster()?.add_node(&Node {
            name: format!("node{}", i),
            cpus: 4,
            address: addr.clone(),
        })?;
    }
    Ok(ctx)
}
