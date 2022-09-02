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

use std::convert::TryFrom;

use common_datavalues::DataSchemaRef;
use common_exception::ErrorCode;
use common_exception::Result;
use serde::Deserialize;
use serde::Serialize;

#[derive(Serialize, Deserialize, Clone, Debug, Eq, PartialEq, Default)]
#[serde(default)]
pub struct UserTabularFunction {
    pub name: String,
    pub parameters: Vec<String>,

    pub schema: DataSchemaRef,
    pub as_query: String,
}

impl UserTabularFunction {
    pub fn new(name: &str, parameters: Vec<String>, schema: DataSchemaRef, as_query: &str) -> Self {
        Self {
            name: name.to_string(),
            parameters,
            schema,
            as_query: as_query.to_string(),
        }
    }
}

impl TryFrom<Vec<u8>> for UserTabularFunction {
    type Error = ErrorCode;

    fn try_from(value: Vec<u8>) -> Result<Self> {
        match serde_json::from_slice(&value) {
            Ok(tabular_function) => Ok(tabular_function),
            Err(serialize_error) => Err(ErrorCode::IllegalTabularFunctionFormat(format!(
                "Cannot deserialize user tabular function from bytes. cause {}",
                serialize_error
            ))),
        }
    }
}
