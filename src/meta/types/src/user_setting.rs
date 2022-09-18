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

#[derive(Serialize, Deserialize, Clone, Debug, PartialEq, Eq)]
pub struct UserSetting {
    // The name of the setting.
    pub name: String,
    // The value of the setting.
    pub value: UserSettingValue,
}

#[derive(Serialize, Deserialize, Clone, Debug, PartialEq, Eq)]
pub enum UserSettingValue {
    UInt64(u64),
    String(Vec<u8>),
}

impl UserSettingValue {
    pub fn as_u64(&self) -> Result<u64> {
        match self {
            UserSettingValue::UInt64(v) => Ok(*v),
            other => Result::Err(ErrorCode::BadDataValueType(format!(
                "Unexpected type:{:?} to get u64 number",
                other
            ))),
        }
    }

    pub fn as_string(&self) -> Result<Vec<u8>> {
        match self {
            UserSettingValue::String(v) => Ok(v.to_owned()),
            other => Result::Err(ErrorCode::BadDataValueType(format!(
                "Unexpected type:{:?} to get u64 number",
                other
            ))),
        }
    }
}

impl UserSetting {
    pub fn create(name: &str, value: UserSettingValue) -> UserSetting {
        UserSetting {
            name: name.to_string(),
            value,
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
