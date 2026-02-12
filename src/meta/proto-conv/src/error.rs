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

#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub struct Incompatible {
    pub context: String,
    pub reason: String,
}

impl fmt::Display for Incompatible {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "{}", self.reason)?;

        if !self.context.is_empty() {
            write!(f, "; when:({})", self.context)?;
        }
        Ok(())
    }
}

impl Incompatible {
    pub fn new(reason: impl Into<String>) -> Self {
        Self {
            context: "".into(),
            reason: reason.into(),
        }
    }

    /// Set the context of the error.
    pub fn with_context(mut self, context: impl Into<String>) -> Self {
        self.context = context.into();
        self
    }
}
