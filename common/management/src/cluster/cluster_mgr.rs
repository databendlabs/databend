// Copyright 2020-2021 The Datafuse Authors.
//
// SPDX-License-Identifier: Apache-2.0.
//

use common_exception::Result;

use crate::cluster::backend_api::BackendApi;
use crate::cluster::backends::MemoryBackend;
use crate::cluster::backends::StoreBackend;
use crate::ClusterMeta;

pub struct ClusterMgr {
    backend: Box<dyn BackendApi>,
}

impl ClusterMgr {
    /// For test only.
    pub fn create_with_memory_backend() -> ClusterMgr {
        ClusterMgr {
            backend: Box::new(MemoryBackend::create()),
        }
    }

    pub fn create_with_store_backend(addr: String) -> ClusterMgr {
        ClusterMgr {
            backend: Box::new(StoreBackend::create(addr)),
        }
    }

    pub async fn register(&mut self, namespace: String, meta: &ClusterMeta) -> Result<()> {
        self.backend.put(namespace, meta).await
    }

    pub async fn metas(&mut self, namespace: String) -> Result<Vec<ClusterMeta>> {
        self.backend.get(namespace).await
    }
}
