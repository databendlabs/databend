// Copyright 2020-2021 The Datafuse Authors.
//
// SPDX-License-Identifier: Apache-2.0.

use async_trait::async_trait;
use common_exception::Result;

use crate::cluster::backend_api::BackendApi;
use crate::ClusterMeta;

#[allow(dead_code)]
pub struct StoreBackend {
    addr: String,
}

impl StoreBackend {
    pub fn create(addr: String) -> Self {
        Self { addr }
    }
}

#[async_trait]
impl BackendApi for StoreBackend {
    async fn put(&self, _key: String, _meta: &ClusterMeta) -> Result<()> {
        todo!()
    }

    async fn get(&self, _key: String) -> Result<Vec<ClusterMeta>> {
        todo!()
    }
}
