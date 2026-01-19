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

use anyerror::AnyError;

/// Storage level error that is raised by meta service.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
#[error("{0}")]
pub struct MetaStorageError(pub AnyError);

impl MetaStorageError {
    pub fn damaged<D: fmt::Display, F: FnOnce() -> D>(
        error: &(impl std::error::Error + 'static),
        context: F,
    ) -> Self {
        MetaStorageError(AnyError::new(error).add_context(context))
    }

    pub fn name(&self) -> &'static str {
        "MetaStorageError"
    }
}

impl From<std::string::FromUtf8Error> for MetaStorageError {
    fn from(error: std::string::FromUtf8Error) -> Self {
        MetaStorageError(AnyError::new(&error))
    }
}

impl From<serde_json::Error> for MetaStorageError {
    fn from(error: serde_json::Error) -> MetaStorageError {
        MetaStorageError(AnyError::new(&error))
    }
}

impl From<sled::Error> for MetaStorageError {
    fn from(error: sled::Error) -> MetaStorageError {
        MetaStorageError(AnyError::new(&error))
    }
}

impl From<io::Error> for MetaStorageError {
    fn from(error: io::Error) -> Self {
        MetaStorageError(AnyError::new(&error))
    }
}

impl From<MetaStorageError> for io::Error {
    fn from(e: MetaStorageError) -> Self {
        io::Error::new(io::ErrorKind::InvalidData, e)
    }
}
