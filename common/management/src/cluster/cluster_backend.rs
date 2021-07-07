// Copyright 2020-2021 The Datafuse Authors.
//
// SPDX-License-Identifier: Apache-2.0.

use async_trait::async_trait;
use common_exception::Result;

use crate::ClusterExecutor;

#[async_trait]
pub trait ClusterBackend {
    /// Put an executor to the namespace.
    /// if the executor is exists in the namespace, replace it, others append.
    async fn put(&self, namespace: String, executor: &ClusterExecutor) -> Result<()>;

    /// Remove an executor from the namespace.
    /// if the executor is not exists, nothing to do.
    async fn remove(&self, namespace: String, executor: &ClusterExecutor) -> Result<()>;

    /// Get all the executors by namespace key.
    async fn get(&self, namespace: String) -> Result<Vec<ClusterExecutor>>;
}
