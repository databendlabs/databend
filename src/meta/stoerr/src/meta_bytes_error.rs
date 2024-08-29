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

/// Errors that occur when encode/decode
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
#[error("MetaBytesError: {source}")]
pub struct MetaBytesError {
    #[source]
    pub source: AnyError,
}

impl MetaBytesError {
    pub fn new(error: &(impl std::error::Error + 'static)) -> Self {
        Self {
            source: AnyError::new(error),
        }
    }
}

impl From<serde_json::Error> for MetaBytesError {
    fn from(e: serde_json::Error) -> Self {
        Self::new(&e)
    }
}

impl From<std::string::FromUtf8Error> for MetaBytesError {
    fn from(e: std::string::FromUtf8Error) -> Self {
        Self::new(&e)
    }
}

impl From<prost::EncodeError> for MetaBytesError {
    fn from(e: prost::EncodeError) -> Self {
        Self::new(&e)
    }
}

impl From<prost::DecodeError> for MetaBytesError {
    fn from(e: prost::DecodeError) -> Self {
        Self::new(&e)
    }
}
