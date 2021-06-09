// Copyright 2020-2021 The Datafuse Authors.
//
// SPDX-License-Identifier: Apache-2.0.

use common_exception::Result;

#[async_trait::async_trait]
pub trait Abortable {
    fn abort(&self);
    async fn wait_server_terminal(&self) -> Result<()>;
}
