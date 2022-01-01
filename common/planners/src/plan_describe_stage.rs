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

use common_datavalues::DataField;
use common_datavalues::DataSchemaRef;
use common_datavalues::DataSchemaRefExt;
use common_datavalues::DataType;

#[derive(serde::Serialize, serde::Deserialize, Clone, Debug, PartialEq)]
pub struct DescribeStagePlan {
    pub name: String,
}

impl DescribeStagePlan {
    pub fn schema(&self) -> DataSchemaRef {
        DataSchemaRefExt::create(vec![
            DataField::new("parent_properties", DataType::String(false), false),
            DataField::new("properties", DataType::String(false), false),
            DataField::new("property_types", DataType::String(false), false),
            DataField::new("property_values", DataType::String(false), false),
            DataField::new("property_defaults", DataType::String(false), false),
            DataField::new("property_changed", DataType::Boolean(false), false),
        ])
    }
}
