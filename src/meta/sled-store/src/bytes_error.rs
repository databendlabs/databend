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

use anyerror::AnyError;
use databend_common_meta_stoerr::MetaBytesError;
use databend_common_meta_stoerr::MetaStorageError;

/// Errors that occur when encode/decode
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize, PartialEq, Eq, thiserror::Error)]
#[error("SledBytesError: {source}")]
pub struct SledBytesError {
    #[source]
    pub source: AnyError,
}

impl SledBytesError {
    pub fn new(error: &(impl std::error::Error + 'static)) -> Self {
        Self {
            source: AnyError::new(error),
        }
    }
}

impl From<serde_json::Error> for SledBytesError {
    fn from(e: serde_json::Error) -> Self {
        Self::new(&e)
    }
}

impl From<std::string::FromUtf8Error> for SledBytesError {
    fn from(e: std::string::FromUtf8Error) -> Self {
        Self::new(&e)
    }
}

// TODO: remove this: after refactoring, sled should not use MetaStorageError directly.
impl From<SledBytesError> for MetaStorageError {
    fn from(e: SledBytesError) -> Self {
        MetaStorageError::BytesError(MetaBytesError::new(&e))
    }
}
