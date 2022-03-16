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

use common_datavalues::prelude::*;
#[derive(serde::Serialize, serde::Deserialize, Clone, Debug, PartialEq)]
pub struct DescribeUserStagePlan {
    pub name: String,
}

impl DescribeUserStagePlan {
    pub fn schema(&self) -> DataSchemaRef {
        DataSchemaRefExt::create(vec![
            DataField::new("name", Vu8::to_data_type()),
            DataField::new("stage_type", Vu8::to_data_type()),
            DataField::new("stage_params", Vu8::to_data_type()),
            DataField::new("copy_options", Vu8::to_data_type()),
            DataField::new("file_format_options", Vu8::to_data_type()),
            DataField::new("comment", Vu8::to_data_type()),
        ])
    }
}
