// Copyright 2020 Datafuse Labs.
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

use common_exception::Result;

use crate::prelude::DFUtf8Array;
use crate::prelude::DataColumn;
use crate::TypeSerializer;

pub struct Utf8Serializer {}

impl TypeSerializer for Utf8Serializer {
    fn serialize_strings(&self, column: &DataColumn) -> Result<Vec<String>> {
        let array = column.to_array()?;
        let array: &DFUtf8Array = array.static_cast();

        let result: Vec<String> = array
            .into_iter()
            .map(|x| {
                x.map(|v| v.to_string())
                    .unwrap_or_else(|| "NULL".to_owned())
            })
            .collect();
        Ok(result)
    }
}
