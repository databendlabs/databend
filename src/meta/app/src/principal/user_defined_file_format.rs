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

use databend_common_exception::ErrorCode;
use databend_common_exception::Result;
use serde::Deserialize;
use serde::Serialize;

use crate::principal::FileFormatParams;
use crate::principal::UserIdentity;

#[derive(Serialize, Deserialize, Clone, Debug, Eq, PartialEq, Default)]
#[serde(default)]
pub struct UserDefinedFileFormat {
    pub name: String,
    pub file_format_params: FileFormatParams,
    pub creator: UserIdentity,
}

impl UserDefinedFileFormat {
    pub fn new(name: &str, file_format_params: FileFormatParams, creator: UserIdentity) -> Self {
        Self {
            name: name.to_string(),
            file_format_params,
            creator,
        }
    }
}

impl TryFrom<Vec<u8>> for UserDefinedFileFormat {
    type Error = ErrorCode;

    fn try_from(value: Vec<u8>) -> Result<Self> {
        match serde_json::from_slice(&value) {
            Ok(udf) => Ok(udf),
            Err(serialize_error) => Err(ErrorCode::IllegalFileFormat(format!(
                "Cannot deserialize user defined file format from bytes. cause {}",
                serialize_error
            ))),
        }
    }
}
