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
    /// For test only.
    pub fn create_with_memory_backend() -> ClusterMgr {
        ClusterMgr {
            backend: Box::new(MemoryBackend::create()),
        }
    }

    /// Store the executor meta to store.
    pub fn create_with_store_backend(addr: String) -> ClusterMgr {
        ClusterMgr {
            backend: Box::new(StoreBackend::create(addr)),
        }
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
