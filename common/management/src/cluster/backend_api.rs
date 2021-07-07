// Copyright 2020-2021 The Datafuse Authors.
//
// SPDX-License-Identifier: Apache-2.0.

use async_trait::async_trait;
use common_exception::Result;

#[async_trait]
pub trait BackendApi {
    async fn put(&self, key: String, value: Vec<u8>) -> Result<()>;
}
