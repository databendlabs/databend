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

use common_datavalues::DataValue;
use common_exception::ErrorCode;
use common_exception::Result;
use serde::Deserialize;
use serde::Serialize;

#[derive(Serialize, Deserialize, Clone, Debug, PartialEq)]
#[serde(default)]
pub struct UserSetting {
    // The name of the setting.
    pub name: String,

    // The value of the setting.
    pub value: DataValue,
}

impl UserSetting {
    pub fn create(name: &str, value: DataValue) -> UserSetting {
        UserSetting {
            name: name.to_string(),
            value,
        }
    }
}

impl Default for UserSetting {
    fn default() -> Self {
        UserSetting {
            name: "".to_string(),
            value: DataValue::Null,
        }
    }
}

impl TryFrom<Vec<u8>> for UserSetting {
    type Error = ErrorCode;

    fn try_from(value: Vec<u8>) -> Result<Self> {
        match serde_json::from_slice(&value) {
            Ok(info) => Ok(info),
            Err(serialize_error) => Err(ErrorCode::IllegalUserInfoFormat(format!(
                "Cannot deserialize setting from bytes. cause {}",
                serialize_error
            ))),
        }
    }
}
