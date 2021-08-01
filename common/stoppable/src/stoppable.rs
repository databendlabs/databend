// Copyright 2020-2021 The Datafuse Authors.
//
// SPDX-License-Identifier: Apache-2.0.

use common_exception::ErrorCode;
use common_runtime::tokio::sync::broadcast;

/// A task that can be started and stopped.
#[async_trait::async_trait]
pub trait Stoppable {
    /// Start working without blocking the calling thread.
    /// When returned, it should have been successfully started.
    /// Otherwise an Err() should be returned.
    ///
    /// Calling `start()` on a started task should get an error.
    async fn start(&mut self) -> Result<(), ErrorCode>;

    /// Blocking stop. It should not return until everything is cleaned up.
    ///
    /// In case a graceful `stop()` had blocked for too long,
    /// the caller submit a FORCE stop by sending a `()` to `force`.
    /// An impl should either close everything at once, or just ignore the `force` signal if it does not support force stop.
    ///
    /// Calling `stop()` twice should get an error.
    async fn stop(&mut self, mut force: Option<broadcast::Receiver<()>>) -> Result<(), ErrorCode>;
}
