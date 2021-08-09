// Copyright 2020-2021 The Datafuse Authors.
//
// SPDX-License-Identifier: Apache-2.0.

use std::env;

use common_exception::ErrorCode;
use common_exception::Result;
use common_exception::ToErrorCode;
use common_runtime::tokio::runtime::Runtime;

use crate::clusters::Cluster;
use crate::configs::Config;
use crate::sessions::FuseQueryContextRef;
use crate::sessions::SessionManager;

pub fn try_create_context() -> Result<FuseQueryContextRef> {
    let config = Config::default();
    try_create_context_with_conf(config)
}
pub fn try_create_context_with_conf(mut config: Config) -> Result<FuseQueryContextRef> {
    let cluster = Cluster::empty();

    // Setup log dir to the tests directory.
    config.log_dir = env::current_dir()?
        .join("../../tests/data/logs")
        .display()
        .to_string();

    let sessions = SessionManager::from_conf(config, cluster)?;
    let test_session = sessions.create_session("TestSession")?;
    let test_context = test_session.create_context();
    test_context.get_settings().set_max_threads(8)?;
    Ok(test_context)
}

#[derive(Clone)]
pub struct ClusterNode {
    name: String,
    priority: u8,
    address: String,
}

impl ClusterNode {
    pub fn create(name: impl ToString, priority: u8, address: impl ToString) -> ClusterNode {
        ClusterNode {
            name: name.to_string(),
            priority,
            address: address.to_string(),
        }
    }
}

pub fn try_create_cluster_context(nodes: &[ClusterNode]) -> Result<FuseQueryContextRef> {
    let config = Config::default();
    let cluster = Cluster::empty();

    for node in nodes {
        let node = node.clone();
        let cluster = cluster.clone();
        std::thread::spawn(move || -> Result<()> {
            let runtime = Runtime::new()
                .map_err_to_code(ErrorCode::TokioError, || "Cannot create tokio runtime.")?;

            runtime.block_on(cluster.add_node(&node.name, node.priority, &node.address))
        })
        .join()
        .unwrap()?;
    }

    let sessions = SessionManager::from_conf(config, cluster)?;
    let test_session = sessions.create_session("TestSession")?;
    let test_context = test_session.create_context();
    test_context.get_settings().set_max_threads(8)?;
    Ok(test_context)
}
