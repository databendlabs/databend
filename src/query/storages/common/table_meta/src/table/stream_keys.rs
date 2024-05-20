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

// Stream table options.
pub const OPT_KEY_TABLE_NAME: &str = "table_name";
pub const OPT_KEY_DATABASE_NAME: &str = "table_database";
pub const OPT_KEY_TABLE_ID: &str = "table_id";
pub const OPT_KEY_TABLE_VER: &str = "table_version";
pub const OPT_KEY_MODE: &str = "mode";

pub const MODE_APPEND_ONLY: &str = "append_only";
pub const MODE_STANDARD: &str = "standard";

#[derive(serde::Serialize, serde::Deserialize, Clone, Debug, PartialEq, Eq)]
pub enum ChangeType {
    // append only.
    Append,
    // standard.
    Insert,
    Delete,
}

#[derive(Debug, Clone, Eq, PartialEq)]
pub enum StreamMode {
    AppendOnly,
    Standard,
}

impl std::str::FromStr for StreamMode {
    type Err = databend_common_exception::ErrorCode;
    fn from_str(s: &str) -> databend_common_exception::Result<Self> {
        match s {
            MODE_APPEND_ONLY => Ok(StreamMode::AppendOnly),
            MODE_STANDARD => Ok(StreamMode::Standard),
            _ => Err(databend_common_exception::ErrorCode::IllegalStream(
                format!("invalid stream mode: {}", s),
            )),
        }
    }
}

impl std::fmt::Display for StreamMode {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        write!(f, "{}", match self {
            StreamMode::AppendOnly => MODE_APPEND_ONLY.to_string(),
            StreamMode::Standard => MODE_STANDARD.to_string(),
        })
    }
}

pub fn get_change_type(table_alias_name: &Option<String>) -> Option<ChangeType> {
    let mut change_type = None;
    if let Some(table_alias) = table_alias_name {
        let alias_param = table_alias.split('$').collect::<Vec<_>>();
        if alias_param.len() == 2 && alias_param[1].len() == 8 {
            if let Ok(suffix) = i64::from_str_radix(alias_param[1], 16) {
                // 2023-01-01 00:00:00.
                let base_timestamp = 1672502400;
                if suffix > base_timestamp {
                    change_type = match alias_param[0] {
                        "_change_append" => Some(ChangeType::Append),
                        "_change_insert" => Some(ChangeType::Insert),
                        "_change_delete" => Some(ChangeType::Delete),
                        _ => None,
                    };
                }
            }
        }
    }
    change_type
}
