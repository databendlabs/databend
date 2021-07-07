// Copyright 2020-2021 The Datafuse Authors.
//
// SPDX-License-Identifier: Apache-2.0.

use async_trait::async_trait;
use common_exception::Result;

use crate::ClusterMeta;

#[async_trait]
pub trait BackendApi {
    /// Put a meta to the values.
    /// if the meta is exists in the values, replace it
    /// others, appends to the values.
    async fn put(&self, key: String, meta: &ClusterMeta) -> Result<()>;

    /// Get all the metas by key.
    async fn get(&self, key: String) -> Result<Vec<ClusterMeta>>;
}
