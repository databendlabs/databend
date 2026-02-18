// Copyright 2021 Datafuse Labs
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use crate::errors::ConnectionClosed;
use crate::errors::EarlyRemoved;

/// Errors that can occur during semaphore permit acquisition.
///
/// This enum represents all possible failure modes when attempting to acquire
/// a distributed semaphore permit through the meta-service.
#[derive(thiserror::Error, Debug)]
pub enum AcquireError {
    /// The connection to meta-service was lost during acquisition.
    ///
    /// This can occur due to network issues, meta-service unavailability,
    /// or gRPC connection failures. The operation can typically be retried.
    #[error("AcquireError: {0}")]
    ConnectionClosed(#[from] ConnectionClosed),

    /// The permit entry was removed from meta-service before acquisition completed.
    ///
    /// This typically occurs when the permit's TTL expires while waiting in the queue,
    /// or when external processes manually remove the permit entry.
    #[error(
        "AcquireError: the semaphore permit entry is removed from meta-service before being acquired: {0}"
    )]
    EarlyRemoved(#[from] EarlyRemoved),
}
