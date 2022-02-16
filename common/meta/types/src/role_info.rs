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

use serde::Deserialize;
use serde::Serialize;

use crate::MetaError;
use crate::MetaResult;
use crate::RoleIdentity;
use crate::UserGrantSet;

#[derive(Serialize, Deserialize, Clone, Debug, Eq, PartialEq, Default)]
#[serde(default)]
pub struct RoleInfo {
    pub name: String,
    pub host: String,

    pub grants: UserGrantSet,
}

impl RoleInfo {
    pub fn new(name: String, host: String) -> Self {
        Self {
            name,
            host,
            grants: UserGrantSet::empty(),
        }
    }

    pub fn identity(&self) -> RoleIdentity {
        RoleIdentity {
            name: self.name.clone(),
            host: self.host.clone(),
        }
    }
}

impl TryFrom<Vec<u8>> for RoleInfo {
    type Error = MetaError;

    fn try_from(value: Vec<u8>) -> MetaResult<Self> {
        match serde_json::from_slice(&value) {
            Ok(role_info) => Ok(role_info),
            Err(serialize_error) => Err(MetaError::IllegalRoleInfoFormat(format!(
                "Cannot deserialize role info from bytes. cause {}",
                serialize_error
            ))),
        }
    }
}
