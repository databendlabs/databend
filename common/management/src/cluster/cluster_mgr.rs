// Copyright 2020-2021 The Datafuse Authors.
//
// SPDX-License-Identifier: Apache-2.0.
//

use std::sync::Arc;

use common_exception::ErrorCode;
use common_exception::Result;
use common_states::BackendClient;

use crate::cluster::ClusterExecutor;

pub type ClusterMgrRef = Arc<ClusterMgr>;

pub struct ClusterMgr {
    backend_client: BackendClient,
}

impl ClusterMgr {
    pub fn create(uri: String) -> ClusterMgrRef {
        let backend_client = BackendClient::create(uri);
        Arc::new(ClusterMgr { backend_client })
    }

    /// Register an executor to the namespace.
    pub async fn register(&self, namespace: String, executor: &ClusterExecutor) -> Result<()> {
        self.backend_client.put(namespace, executor).await
    }

    /// Unregister an executor from namespace.
    pub async fn unregister(&self, namespace: String, executor: &ClusterExecutor) -> Result<()> {
        self.backend_client.remove(namespace, executor).await
    }

    /// Get all the executors by namespace.
    pub async fn get_executors(&self, namespace: String) -> Result<Vec<ClusterExecutor>> {
        self.backend_client.get(namespace).await
    }

    pub async fn get_executor_by_name(
        &self,
        namespace: String,
        executor_name: String,
    ) -> Result<ClusterExecutor> {
        let executors = self.backend_client.get(namespace.clone()).await?;
        executors
            .into_iter()
            .find(|x| x.name == executor_name)
            .ok_or_else(|| {
                ErrorCode::NotFoundClusterNode(format!(
                    "The executor \"{}\" not found in the namespace \"{}\"",
                    executor_name, namespace
                ))
            })
    }
}
