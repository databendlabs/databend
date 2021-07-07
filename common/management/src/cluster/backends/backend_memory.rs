// Copyright 2020-2021 The Datafuse Authors.
//
// SPDX-License-Identifier: Apache-2.0.

use async_trait::async_trait;
use common_exception::Result;

use crate::cluster::backend_api::BackendApi;

pub struct MemoryBackend {}

impl MemoryBackend {
    pub fn create() -> Self {
        Self {}
    }
}

#[async_trait]
impl BackendApi for MemoryBackend {
    async fn put(&self, _key: String, _value: Vec<u8>) -> Result<()> {
        todo!()
    }
}
