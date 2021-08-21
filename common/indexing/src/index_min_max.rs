// Copyright 2021 Datafuse Labs.
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
//

use common_datavalues::DataValue;

use crate::IndexSchemaVersion;

/// Min and Max index.
#[derive(Debug, PartialEq)]
pub struct MinMaxIndex {
    pub col: String,
    pub min: DataValue,
    pub max: DataValue,
    pub version: IndexSchemaVersion,
}

impl MinMaxIndex {
    pub fn create(col: String, min: DataValue, max: DataValue) -> Self {
        MinMaxIndex {
            col,
            min,
            max,
            version: IndexSchemaVersion::V1,
        }
    }

    pub fn typ(&self) -> &str {
        "min_max"
    }
}
