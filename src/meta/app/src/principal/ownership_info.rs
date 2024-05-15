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
use std::fmt::Display;

use anyerror::AnyError;
use databend_common_exception::ErrorCode;

use crate::principal::OwnershipObject;

#[derive(serde::Serialize, serde::Deserialize, Clone, Debug, Eq, PartialEq, Hash)]
pub struct OwnershipInfo {
    pub object: OwnershipObject,
    pub role: String,
}

#[derive(Clone, Debug, Eq, PartialEq, thiserror::Error)]
pub struct OwnershipInfoSerdeError {
    pub message: String,
    pub source: AnyError,
}

impl Display for OwnershipInfoSerdeError {
    fn fmt(&self, f: &mut fmt::Formatter) -> std::fmt::Result {
        write!(f, "{} cause: {}", self.message, self.source)
    }
}

impl TryFrom<Vec<u8>> for OwnershipInfo {
    type Error = OwnershipInfoSerdeError;

    fn try_from(value: Vec<u8>) -> Result<Self, Self::Error> {
        match serde_json::from_slice(&value) {
            Ok(role_info) => Ok(role_info),
            Err(serialize_error) => Err(OwnershipInfoSerdeError {
                message: "Cannot deserialize GrantOwnershipInfo from bytes".to_string(),
                source: AnyError::new(&serialize_error),
            }),
        }
    }
}

impl From<OwnershipInfoSerdeError> for ErrorCode {
    fn from(e: OwnershipInfoSerdeError) -> Self {
        ErrorCode::InvalidReply(e.to_string())
    }
}
