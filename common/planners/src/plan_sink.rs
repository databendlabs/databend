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

use std::sync::Arc;

use common_datavalues::DataField;
use common_datavalues::DataSchemaRef;
use common_datavalues::DataSchemaRefExt;
use common_datavalues::DataType;
use common_meta_types::TableInfo;
use lazy_static::lazy_static;

use crate::PlanNode;

lazy_static! {
    pub static ref SINK_SCHEMA: DataSchemaRef = DataSchemaRefExt::create(vec![
        DataField::new("seg_loc", DataType::String, false),
        DataField::new("seg_info", DataType::String, false),
    ]);
}

#[derive(serde::Serialize, serde::Deserialize, Clone, Debug, PartialEq)]
pub struct SinkPlan {
    pub table_info: TableInfo,
    pub input: Arc<PlanNode>,
    pub cast_needed: bool,
}

impl SinkPlan {
    /// Return sink schema
    pub fn schema(&self) -> DataSchemaRef {
        SINK_SCHEMA.clone()
    }
}
