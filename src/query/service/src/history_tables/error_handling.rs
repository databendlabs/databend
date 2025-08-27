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

use std::cmp::min;

use databend_common_exception::ErrorCode;

/// Error counters for tracking persistent and temporary errors during history table operations
#[derive(Debug, Default)]
pub struct ErrorCounters {
    persistent: u32,
    temporary: u32,
}

impl ErrorCounters {
    /// Create a new ErrorCounters instance with zero counts
    pub fn new() -> Self {
        Self::default()
    }

    /// Reset both error counters to zero
    pub fn reset(&mut self) {
        self.persistent = 0;
        self.temporary = 0;
    }

    /// Increment persistent error counter and return the new count
    pub fn increment_persistent(&mut self) -> u32 {
        self.persistent += 1;
        self.persistent
    }

    /// Increment temporary error counter and return the new count
    pub fn increment_temporary(&mut self) -> u32 {
        self.temporary += 1;
        self.temporary
    }

    /// Check if persistent error count has exceeded the maximum allowed attempts
    pub fn persistent_exceeded_limit(&self) -> bool {
        self.persistent > MAX_PERSISTENT_ERROR_ATTEMPTS
    }

    /// Calculate backoff duration in seconds for temporary errors using exponential backoff
    pub fn calculate_temp_backoff(&self) -> u64 {
        min(
            2u64.saturating_pow(self.temporary),
            MAX_TEMP_ERROR_BACKOFF_SECONDS,
        )
    }

    /// Get current persistent error count
    pub fn persistent_count(&self) -> u32 {
        self.persistent
    }

    /// Get current temporary error count
    pub fn temporary_count(&self) -> u32 {
        self.temporary
    }
}

/// Maximum number of persistent error attempts before giving up
const MAX_PERSISTENT_ERROR_ATTEMPTS: u32 = 3;

/// Maximum backoff time in seconds for temporary errors (10 minutes)
const MAX_TEMP_ERROR_BACKOFF_SECONDS: u64 = 10 * 60;

/// Check if the error is a temporary error that should be retried
/// We will use this to determine if we should retry the operation.
pub fn is_temp_error(e: &ErrorCode) -> bool {
    let code = e.code();

    // Storage and I/O errors are considered temporary errors
    let storage = code == ErrorCode::STORAGE_NOT_FOUND
        || code == ErrorCode::STORAGE_PERMISSION_DENIED
        || code == ErrorCode::STORAGE_UNAVAILABLE
        || code == ErrorCode::STORAGE_UNSUPPORTED
        || code == ErrorCode::STORAGE_INSECURE
        || code == ErrorCode::INVALID_OPERATION
        || code == ErrorCode::STORAGE_OTHER;

    // If acquire semaphore failed, we consider it a temporary error
    let meta = code == ErrorCode::META_SERVICE_ERROR;
    let transaction = code == ErrorCode::UNRESOLVABLE_CONFLICT;

    storage || transaction || meta
}
