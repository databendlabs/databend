// Copyright 2022 Datafuse Labs.
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
use common_exception::Result;
use serde::Deserialize;
use serde::Serialize;

use crate::UserGrantSet;

#[derive(Serialize, Deserialize, Clone, Debug, Eq, PartialEq, Default)]
#[serde(default)]
pub struct RoleInfo {
    pub name: String,

    pub grants: UserGrantSet,
}

impl RoleInfo {
    pub fn new(name: String) -> Self {
        Self {
            name,
            grants: UserGrantSet::empty(),
        }
    }
}

impl TryFrom<Vec<u8>> for RoleInfo {
    type Error = ErrorCode;

    fn try_from(value: Vec<u8>) -> Result<Self> {
        match serde_json::from_slice(&value) {
            Ok(role_info) => Ok(role_info),
            Err(serialize_error) => Err(ErrorCode::IllegalRoleInfoFormat(format!(
                "Cannot deserialize role info from bytes. cause {}",
                serialize_error
            ))),
        }
    }
}
