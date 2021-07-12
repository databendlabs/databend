// Copyright 2020-2021 The Datafuse Authors.
//
// SPDX-License-Identifier: Apache-2.0.
//

use std::sync::Arc;

use common_exception::ErrorCode;
use common_exception::Result;
use common_kvs::BackendClient;

use crate::cluster::ClusterExecutor;

pub type ClusterClientRef = Arc<ClusterClient>;

pub struct ClusterClient {
    backend_client: BackendClient,
}

impl ClusterClient {
    pub fn create(uri: String) -> ClusterClientRef {
        let backend_client = BackendClient::create(uri);
        Arc::new(ClusterClient { backend_client })
    }

    /// Register an executor to the namespace.
    pub async fn register(&self, namespace: String, executor: &ClusterExecutor) -> Result<()> {
        let key = format!("{}/{}", namespace, executor.name);
        self.backend_client.put(key, executor).await
    }

    /// Unregister an executor from namespace.
    pub async fn unregister(&self, namespace: String, executor: &ClusterExecutor) -> Result<()> {
        let key = format!("{}/{}", namespace, executor.name);
        self.backend_client.remove(key).await
    }

    /// Get all the executors by namespace.
    pub async fn get_executors_by_namespace(
        &self,
        namespace: String,
    ) -> Result<Vec<ClusterExecutor>> {
        let executors: Vec<(String, ClusterExecutor)> =
            self.backend_client.get_from_prefix(namespace).await?;
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

    pub async fn get_executor_by_name(
        &self,
        namespace: String,
        executor_name: String,
    ) -> Result<ClusterExecutor> {
        let key = format!("{}/{}", namespace, executor_name);
        let res: Option<ClusterExecutor> = self.backend_client.get(key).await?;
        Ok(match res {
            None => return Err(ErrorCode::UnknownException("Unknow cluster")),
            Some(v) => v,
        })
    }
}
