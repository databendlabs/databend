// Copyright 2020-2021 The Datafuse Authors.
//
// SPDX-License-Identifier: Apache-2.0.

use async_trait::async_trait;
use common_exception::Result;

use crate::backends::StateBackend;

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
impl StateBackend for StoreBackend {
    async fn put(&self, _key: String, _value: String) -> Result<()> {
        todo!()
    }

    async fn remove(&self, _key: String) -> Result<()> {
        todo!()
    }

    async fn get(&self, _key: String) -> Result<Option<String>> {
        todo!()
    }
}
