// Copyright 2021 Datafuse Labs.
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

use common_exception::ErrorCode;
use serde::Deserialize;
use serde::Serialize;

/// Errors about an invalid meta operation result
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, thiserror::Error)]
pub enum MetaResultError {
    #[error("The result of an add operation is invalid: before: {prev}, after: {result}")]
    InvalidAddResult { prev: String, result: String },

    #[error("Expect result of type: {expect}, got: {got}")]
    InvalidType { expect: String, got: String },
}

impl MetaResultError {
    pub fn invalid_type<T, U>(_: T, _: U) -> Self {
        MetaResultError::InvalidType {
            expect: std::any::type_name::<T>().to_string(),
            got: std::any::type_name::<T>().to_string(),
        }
    }
}

impl From<MetaResultError> for ErrorCode {
    fn from(e: MetaResultError) -> Self {
        ErrorCode::MetaNodeInternalError(e.to_string())
    }
}
