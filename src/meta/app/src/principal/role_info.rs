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
use std::fmt::Formatter;

use anyerror::AnyError;
use chrono::DateTime;
use chrono::Utc;
use databend_common_exception::ErrorCode;
use serde::Deserialize;
use serde::Serialize;

use crate::principal::UserGrantSet;

#[derive(Serialize, Deserialize, Clone, Debug, Eq, PartialEq, Default)]
#[serde(default)]
pub struct RoleInfo {
    pub name: String,

    pub grants: UserGrantSet,
    pub created_on: DateTime<Utc>,
    pub update_on: DateTime<Utc>,
}

/// Error when ser/de RoleInfo
#[derive(Clone, Debug, Eq, PartialEq, thiserror::Error)]
pub struct RoleInfoSerdeError {
    pub message: String,
    pub source: AnyError,
}

impl RoleInfo {
    pub fn new(name: &str) -> Self {
        let now = Utc::now();
        Self {
            name: name.to_string(),
            grants: UserGrantSet::empty(),
            created_on: now,
            update_on: now,
        }
    }

    pub fn identity(&self) -> &str {
        &self.name
    }

    pub fn update_role_time(&mut self) {
        self.update_on = Utc::now()
    }
}

impl TryFrom<Vec<u8>> for RoleInfo {
    type Error = RoleInfoSerdeError;

    fn try_from(value: Vec<u8>) -> Result<Self, Self::Error> {
        match serde_json::from_slice(&value) {
            Ok(role_info) => Ok(role_info),
            Err(serialize_error) => Err(RoleInfoSerdeError {
                message: "Cannot deserialize RoleInfo from bytes".to_string(),
                source: AnyError::new(&serialize_error),
            }),
        }
    }
}

impl Display for RoleInfoSerdeError {
    fn fmt(&self, f: &mut Formatter) -> std::fmt::Result {
        write!(f, "{} cause: {}", self.message, self.source)
    }
}

impl From<RoleInfoSerdeError> for ErrorCode {
    fn from(e: RoleInfoSerdeError) -> Self {
        ErrorCode::InvalidReply(e.to_string())
    }
}
