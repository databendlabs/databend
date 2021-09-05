// Copyright 2020-2021 The Datafuse Authors.
//
// SPDX-License-Identifier: Apache-2.0.

use async_trait::async_trait;
use common_exception::Result;
use common_runtime::tokio::sync::OwnedMutexGuard;

#[async_trait]
pub trait Lock: Send + Sync {
    async fn unlock(&mut self);
}

#[async_trait]
impl<T: Send + Sync> Lock for OwnedMutexGuard<T> {
    async fn unlock(&mut self) {}
}

#[async_trait]
pub trait Backend: Send + Sync {
    /// Get value string by key.
    async fn get(&self, key: String) -> Result<Option<String>>;
    /// Get all value strings which prefix with the key.
    async fn get_from_prefix(&self, prefix: String) -> Result<Vec<(String, String)>>;

    async fn put(&self, key: String, value: String) -> Result<()>;
    async fn remove(&self, key: String) -> Result<()>;

    /// Get the key lock.
    async fn lock(&self, key: String) -> Result<Box<dyn Lock>>;
}
