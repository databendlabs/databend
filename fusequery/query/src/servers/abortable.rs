// Copyright 2020-2021 The Datafuse Authors.
//
// SPDX-License-Identifier: Apache-2.0.

use common_exception::Result;
use common_exception::ErrorCode;
use std::time::Duration;

pub type Elapsed = Duration;

#[async_trait::async_trait]
pub trait AbortableService<Args, R> {
    fn abort(&self, force: bool) -> Result<()>;

    async fn start(&self, args: Args) -> Result<R>;

    async fn wait_terminal(&self, duration: Option<Duration>) -> Result<Elapsed>;
}
