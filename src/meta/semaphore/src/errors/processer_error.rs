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

use tonic::Status;

use crate::errors::AcquirerClosed;
use crate::errors::ConnectionClosed;

/// Errors during semaphore permit processing.
#[derive(thiserror::Error, Debug)]
pub enum ProcessorError {
    /// Connection to meta-service was lost.
    #[error("ProcessorError: {0}")]
    ConnectionClosed(#[from] ConnectionClosed),

    /// Acquirer or Permit was dropped and there is no receiving end to receive the event.
    #[error("ProcessorError: the semaphore Acquirer or Permit is dropped {0}")]
    AcquirerClosed(#[from] AcquirerClosed),
}

impl From<Status> for ProcessorError {
    fn from(status: Status) -> Self {
        ProcessorError::ConnectionClosed(status.into())
    }
}
