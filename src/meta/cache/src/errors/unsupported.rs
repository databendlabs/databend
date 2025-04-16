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

/// An error indicating that caching is not supported.
#[derive(thiserror::Error, Debug, Clone)]
pub struct Unsupported {
    /// The reason for the unsupported operation.
    ///
    /// This error is raised when a feature or operation is not supported,
    /// typically due to version incompatibility between meta client and meta server.
    reason: String,

    /// A chain of contexts describing when the error occurred.
    /// Each context is added using the `context` method.
    when: Vec<String>,
}

impl fmt::Display for Unsupported {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "Unsupported: {}", self.reason)?;

        if !self.when.is_empty() {
            write!(f, "; when: ({})", self.when.join(", "))?;
        }
        Ok(())
    }
}

impl Unsupported {
    pub fn new(reason: impl fmt::Display) -> Self {
        Self {
            reason: reason.to_string(),
            when: vec![],
        }
    }

    pub fn context(mut self, context: impl fmt::Display) -> Self {
        self.when.push(context.to_string());
        self
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_unsupported() {
        let error = Unsupported::new("test");
        assert_eq!(error.to_string(), "Unsupported: test");

        let error = error.context("test").context("test2");
        assert_eq!(error.to_string(), "Unsupported: test; when: (test, test2)");
    }
}
