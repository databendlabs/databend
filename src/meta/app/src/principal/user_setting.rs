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

use core::fmt;

use databend_common_exception::ErrorCode;
use databend_common_exception::Result;
use serde::Deserialize;
use serde::Deserializer;
use serde::Serialize;
use serde::Serializer;

#[derive(Serialize, Deserialize, Clone, Debug, PartialEq, Eq)]
pub struct UserSetting {
    // The name of the setting.
    pub name: String,
    // The value of the setting.
    pub value: UserSettingValue,
}

#[derive(Serialize, Deserialize, Clone, PartialEq, Eq)]
pub enum UserSettingValue {
    UInt64(u64),

    // TO BE COMPATIBLE WITH old version: `String<Vec<u8>>`
    #[serde(deserialize_with = "deser_str_from_vu8")]
    #[serde(serialize_with = "str_vu8")]
    String(String),
}

fn deser_str_from_vu8<'de, D>(deserializer: D) -> std::result::Result<String, D::Error>
where D: Deserializer<'de> {
    let s: Vec<u8> = Deserialize::deserialize(deserializer)?;
    Ok(String::from_utf8(s).unwrap())
}

fn str_vu8<S>(data: &String, s: S) -> std::result::Result<S::Ok, S::Error>
where S: Serializer {
    s.serialize_bytes(data.as_bytes())
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

    pub fn as_string(&self) -> String {
        match self {
            UserSettingValue::String(v) => v.to_owned(),
            UserSettingValue::UInt64(v) => format!("{}", v),
        }
    }
}

impl fmt::Display for UserSettingValue {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            UserSettingValue::UInt64(v) => write!(f, "{}", v),
            UserSettingValue::String(v) => write!(f, "{}", v),
        }
    }
}

impl fmt::Debug for UserSettingValue {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            UserSettingValue::UInt64(v) => write!(f, "{}", v),
            UserSettingValue::String(_) => write!(f, "{}", self),
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
