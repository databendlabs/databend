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

use std::time::Duration;

use databend_common_expression::types::DataType;
use databend_common_expression::DataField;
use databend_common_expression::DataSchemaRef;
use databend_common_expression::DataSchemaRefExt;
use databend_common_meta_app::principal::StageInfo;

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum PresignAction {
    Download,
    Upload,
}

#[derive(Debug, Clone)]
pub struct PresignPlan {
    pub stage: Box<StageInfo>,
    pub path: String,
    pub action: PresignAction,
    pub expire: Duration,
    pub content_type: Option<String>,
}

impl PresignPlan {
    pub fn schema(&self) -> DataSchemaRef {
        DataSchemaRefExt::create(vec![
            DataField::new("method", DataType::String),
            DataField::new("headers", DataType::Variant),
            DataField::new("url", DataType::String),
        ])
    }
}
