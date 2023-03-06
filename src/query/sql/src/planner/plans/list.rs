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
use std::sync::Arc;

use common_expression::types::DataType;
use common_expression::types::NumberDataType;
use common_expression::DataField;
use common_expression::DataSchema;
use common_expression::DataSchemaRef;
use common_meta_app::principal::StageInfo;

#[derive(PartialEq, Eq, Clone, Debug)]
pub struct ListPlan {
    pub stage: StageInfo,
    pub path: String,
    pub pattern: String,
}

impl ListPlan {
    pub fn schema(&self) -> DataSchemaRef {
        let name = DataField::new("name", DataType::String);
        let size = DataField::new("size", DataType::Number(NumberDataType::UInt64));
        let md5 = DataField::new("md5", DataType::Nullable(Box::new(DataType::String)));
        let last_modified = DataField::new("last_modified", DataType::String);
        let creator = DataField::new("creator", DataType::Nullable(Box::new(DataType::String)));

        Arc::new(DataSchema::new(vec![
            name,
            size,
            md5,
            last_modified,
            creator,
        ]))
    }
}
