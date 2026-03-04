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

use std::fmt;

/// Error indicating that a semaphore acquirer or permit has been closed.
///
/// This occurs when the acquirer instance becomes unavailable due to connection loss,
/// explicit closure, permit expiration, or meta-service unavailability.
#[derive(thiserror::Error, Debug)]
pub struct AcquirerClosed {
    reason: String,
    when: Vec<String>,
}

impl AcquirerClosed {
    /// Creates a new acquirer closed error with the specified reason.
    pub fn new(reason: impl ToString) -> Self {
        AcquirerClosed {
            reason: reason.to_string(),
            when: vec![],
        }
    }

    /// Adds contextual information about when or where the error occurred.
    ///
    /// Context is displayed in the order it was added and enables method chaining.
    pub fn context(mut self, context: impl ToString) -> Self {
        self.when.push(context.to_string());
        self
    }
}

impl fmt::Display for AcquirerClosed {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(
            f,
            "distributed-Semaphore Acquirer or Permit is closed: {}",
            self.reason
        )?;

        if self.when.is_empty() {
            return Ok(());
        }

        write!(f, "; when: (")?;

        for (i, when) in self.when.iter().enumerate() {
            if i > 0 {
                write!(f, "; ")?;
            }
            write!(f, "{}", when)?;
        }

        write!(f, ")")
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_basic_error_creation() {
        let err = AcquirerClosed::new("Connection timeout");
        assert_eq!(
            err.to_string(),
            "distributed-Semaphore Acquirer or Permit is closed: Connection timeout"
        );
    }

    #[test]
    fn test_error_with_context_chain() {
        let err = AcquirerClosed::new("Network unreachable")
            .context("while acquiring read permit")
            .context("during database query");
        assert_eq!(
            err.to_string(),
            "distributed-Semaphore Acquirer or Permit is closed: Network unreachable; when: (while acquiring read permit; during database query)"
        );
    }

    #[test]
    fn test_error_without_context() {
        let err = AcquirerClosed::new("Simple error");
        assert_eq!(
            err.to_string(),
            "distributed-Semaphore Acquirer or Permit is closed: Simple error"
        );
    }
}
