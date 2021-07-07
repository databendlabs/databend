// Copyright 2020-2021 The Datafuse Authors.
//
// SPDX-License-Identifier: Apache-2.0.
//

use common_exception::Result;

use crate::cluster::backends::MemoryBackend;
use crate::cluster::backends::StoreBackend;
use crate::cluster::cluster_backend::ClusterBackend;
use crate::ClusterExecutor;

pub struct ClusterMgr {
    backend: Box<dyn ClusterBackend>,
}

impl ClusterMgr {
    pub fn create(addr: String) -> ClusterMgr {
        let backend: Box<dyn ClusterBackend> = match addr.as_str() {
            // For test only.
            "" => Box::new(MemoryBackend::create()),
            _ => Box::new(StoreBackend::create(addr)),
        };
        ClusterMgr { backend }
    }

    /// Register an executor to the namespace.
    pub async fn register(&mut self, namespace: String, executor: &ClusterExecutor) -> Result<()> {
        self.backend.put(namespace, executor).await
    }

    /// Unregister an executor from namespace.
    pub async fn unregister(
        &mut self,
        namespace: String,
        executor: &ClusterExecutor,
    ) -> Result<()> {
        self.backend.remove(namespace, executor).await
    }

    /// Get all the executors by namespace.
    pub async fn get_executors(&mut self, namespace: String) -> Result<Vec<ClusterExecutor>> {
        self.backend.get(namespace).await
    }
}
