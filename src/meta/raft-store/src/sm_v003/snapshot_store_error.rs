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

use std::fmt::Display;
use std::io;

use databend_common_meta_types::raft_types::ErrorSubject;
use databend_common_meta_types::raft_types::StorageError;
use openraft::AnyError;
use openraft::ErrorVerb;

/// Errors that occur when accessing snapshot store
#[derive(Debug, thiserror::Error)]
#[error("SnapshotStoreError({verb:?}: {source}, while: {context}")]
pub struct SnapshotStoreError {
    verb: ErrorVerb,

    #[source]
    source: io::Error,
    context: String,
}

impl SnapshotStoreError {
    pub fn read(error: io::Error) -> Self {
        Self {
            verb: ErrorVerb::Read,
            source: error,
            context: "".to_string(),
        }
    }

    pub fn write(error: io::Error) -> Self {
        Self {
            verb: ErrorVerb::Write,
            source: error,
            context: "".to_string(),
        }
    }

    pub fn add_context(&mut self, context: impl Display) {
        if self.context.is_empty() {
            self.context = context.to_string();
        } else {
            self.context = format!("{}; while {}", self.context, context);
        }
    }

    pub fn with_context(mut self, context: impl Display) -> Self {
        self.add_context(context);
        self
    }

    /// Add meta and context info to the error.
    ///
    /// meta is anything that can be displayed.
    pub fn with_meta(self, context: impl Display, meta: impl Display) -> Self {
        self.with_context(format_args!("{}: {}", context, meta))
    }
}

impl From<SnapshotStoreError> for StorageError {
    fn from(error: SnapshotStoreError) -> Self {
        StorageError::new(
            ErrorSubject::Snapshot(None),
            error.verb,
            AnyError::new(&error),
        )
    }
}


impl From<SnapshotStoreError> for io::Error {
    fn from(e: SnapshotStoreError) -> Self {
        io::Error::other(e)
    }
}
