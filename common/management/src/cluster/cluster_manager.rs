// Copyright 2020-2021 The Datafuse Authors.
//
// SPDX-License-Identifier: Apache-2.0.
//

use std::sync::Arc;

use common_exception::ErrorCode;
use common_exception::Result;
use common_kvs::MetadataProvider;

use crate::cluster::ClusterConfig;
use crate::cluster::ClusterExecutor;

pub type ClusterManagerRef = Arc<ClusterManager>;

pub struct ClusterManager {
    config: ClusterConfig,
    metadata_provider: MetadataProvider,
}

impl ClusterManager {
    pub fn from_conf(conf: ClusterConfig) -> ClusterManagerRef {
        unimplemented!()
    }

    /// Register an executor to the namespace.
    pub async fn add_node(&self, executor: &ClusterExecutor) -> Result<()> {
        let namespace = self.config.namespace.clone();
        let key = format!("{}/{}", namespace, executor.name);
        self.metadata_provider.put(key, executor).await
    }

    /// Unregister an executor from namespace.
    pub async fn unregister(&self, executor: &ClusterExecutor) -> Result<()> {
        let namespace = self.config.namespace.clone();
        let key = format!("{}/{}", namespace, executor.name);
        self.metadata_provider.remove(key).await
    }

    /// Get all the executors.
    pub async fn get_executors(&self) -> Result<Vec<ClusterExecutor>> {
        let namespace = self.config.namespace.clone();
        let executors: Vec<(String, ClusterExecutor)> =
            self.metadata_provider.get_from_prefix(namespace).await?;
        executors
            .into_iter()
            .map(|(_k, v)| {
                Ok(ClusterExecutor {
                    name: v.name,
                    priority: v.priority,
                    address: v.address,
                    local: v.local,
                    sequence: v.sequence,
                })
            })
            .collect()
    }

    pub async fn get_executor_by_name(&self, executor_name: String) -> Result<ClusterExecutor> {
        // let key = format!("{}/{}", namespace, executor_name);
        // let res: Option<ClusterExecutor> = self.metadata_provider.get(key).await?;
        // Ok(match res {
        //     None => return Err(ErrorCode::UnknownException("Unknow cluster")),
        //     Some(v) => v,
        // })
        unimplemented!()
    }
}
