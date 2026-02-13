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

use std::sync::Arc;

use databend_common_expression::DataField;
use databend_common_expression::DataSchema;
use databend_common_expression::DataSchemaRef;
use databend_common_expression::types::DataType;
use databend_common_expression::types::NumberDataType;

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum RefreshSelection {
    SegmentLocation(String),
    BlockLocation(String),
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct RefreshVirtualColumnPlan {
    pub catalog: String,
    pub database: String,
    pub table: String,
    pub limit: Option<u64>,
    pub overwrite: bool,
    pub selection: Option<RefreshSelection>,
}

impl RefreshVirtualColumnPlan {
    pub fn schema(&self) -> DataSchemaRef {
        Arc::new(DataSchema::new(vec![DataField::new(
            "refreshed_blocks",
            DataType::Number(NumberDataType::UInt64),
        )]))
    }
}
