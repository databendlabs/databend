// Copyright 2020-2021 The Datafuse Authors.
//
// SPDX-License-Identifier: Apache-2.0.
//

use common_exception::Result;

use crate::cluster::backend_api::BackendApi;
use crate::cluster::backends::MemoryBackend;
use crate::cluster::backends::StoreBackend;
use crate::ClusterMeta;

pub enum BackendType {
    Memory,
    Store(String),
}

pub struct ClusterMgr {
    backend_api: Box<dyn BackendApi>,
}

impl ClusterMgr {
    pub fn new(backend: BackendType) -> ClusterMgr {
        let backend_api: Box<dyn BackendApi> = match backend {
            BackendType::Memory => Box::new(MemoryBackend::create()),
            BackendType::Store(addr) => Box::new(StoreBackend::create(addr)),
        };
        ClusterMgr { backend_api }
    }

    pub async fn upsert_meta(&mut self, namespace: String, meta: &ClusterMeta) -> Result<()> {
        self.backend_api.put(namespace, meta).await
    }

    pub async fn get_metas(&mut self, _namespace: &str) -> Result<Vec<ClusterMeta>> {
        todo!()
    }
}
