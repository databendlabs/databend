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
use std::io;

use tonic::Status;

use crate::errors::either::Either;

/// The connection to the meta-service has been closed.
#[derive(thiserror::Error, Debug)]
pub struct ConnectionClosed {
    reason: Either<io::Error, String>,
    when: Vec<String>,
}

impl ConnectionClosed {
    /// Create a new connection closed error.
    pub fn new(reason: impl Into<Either<io::Error, String>>) -> Self {
        ConnectionClosed {
            reason: reason.into(),
            when: vec![],
        }
    }

    /// Create a new connection closed error from an io::Error.
    pub fn new_io_error(reason: impl Into<io::Error>) -> Self {
        ConnectionClosed {
            reason: Either::A(reason.into()),
            when: vec![],
        }
    }

    /// Create a new connection closed error from a string.
    pub fn new_str(reason: impl ToString) -> Self {
        ConnectionClosed {
            reason: Either::B(reason.to_string()),
            when: vec![],
        }
    }

    /// Append a context to the error.
    pub fn context(mut self, context: impl ToString) -> Self {
        self.when.push(context.to_string());
        self
    }
}

impl fmt::Display for ConnectionClosed {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(
            f,
            "distributed-Semaphore connection closed: {}",
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

impl From<io::Error> for ConnectionClosed {
    fn from(err: io::Error) -> Self {
        ConnectionClosed::new_io_error(err)
    }
}

impl From<Status> for ConnectionClosed {
    fn from(status: Status) -> Self {
        ConnectionClosed::new_str(&status.to_string())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_context() {
        let err = ConnectionClosed::new_str("test")
            .context("context")
            .context("context2");
        assert_eq!(
            err.to_string(),
            "distributed-Semaphore connection closed: test; when: (context; context2)"
        );
    }
}
