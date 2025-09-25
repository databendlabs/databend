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

/// The error that indicates the MetaNode is stopped and cannot handle the request.
#[derive(Clone, Debug, thiserror::Error)]
pub struct MetaNodeStopped {
    context: Vec<String>,
}

impl fmt::Display for MetaNodeStopped {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "MetaNodeStopped")?;
        for context in self.context.iter() {
            write!(f, "; ")?;
            write!(f, "when:({})", context)?;
        }
        Ok(())
    }
}

impl From<MetaNodeStopped> for tonic::Status {
    fn from(e: MetaNodeStopped) -> Self {
        tonic::Status::unavailable(e.to_string())
    }
}

impl Default for MetaNodeStopped {
    fn default() -> Self {
        Self::new()
    }
}

impl MetaNodeStopped {
    pub fn new() -> Self {
        MetaNodeStopped { context: vec![] }
    }

    pub fn with_context(mut self, context: impl ToString) -> Self {
        self.context.push(context.to_string());
        self
    }
}
