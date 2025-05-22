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

use databend_common_expression::types::DataType;
use databend_common_expression::types::NumberDataType;
use databend_common_expression::DataField;
use databend_common_expression::DataSchemaRef;
use databend_common_expression::DataSchemaRefExt;
use databend_common_meta_app::schema::CreateOption;
use databend_common_meta_app::schema::SequenceIdent;

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct CreateSequencePlan {
    pub create_option: CreateOption,
    pub ident: SequenceIdent,
    pub comment: Option<String>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct DropSequencePlan {
    pub ident: SequenceIdent,
    pub if_exists: bool,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct DescSequencePlan {
    pub ident: SequenceIdent,
}

impl DescSequencePlan {
    pub fn schema(&self) -> DataSchemaRef {
        DataSchemaRefExt::create(vec![
            DataField::new("name", DataType::String),
            DataField::new("start", DataType::Number(NumberDataType::UInt64)),
            DataField::new("interval", DataType::Number(NumberDataType::Int64)),
            DataField::new("current", DataType::Number(NumberDataType::UInt64)),
            DataField::new("created_on", DataType::Timestamp),
            DataField::new("updated_on", DataType::Timestamp),
            DataField::new("comment", DataType::Nullable(Box::new(DataType::String))),
        ])
    }
}
