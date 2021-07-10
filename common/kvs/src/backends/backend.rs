// Copyright 2020-2021 The Datafuse Authors.
//
// SPDX-License-Identifier: Apache-2.0.

use async_trait::async_trait;
use common_exception::Result;

#[async_trait]
pub trait StateBackend: Send + Sync {
    async fn put(&self, key: String, value: String) -> Result<()>;

    async fn remove(&self, key: String) -> Result<()>;

    async fn get(&self, key: String) -> Result<Option<String>>;
}
