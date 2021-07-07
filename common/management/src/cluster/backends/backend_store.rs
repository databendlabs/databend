// Copyright 2020-2021 The Datafuse Authors.
//
// SPDX-License-Identifier: Apache-2.0.

use async_trait::async_trait;
use common_exception::Result;

use crate::cluster::backend_api::BackendApi;

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
    async fn put(&self, _key: String, _value: Vec<u8>) -> Result<()> {
        todo!()
    }
}
