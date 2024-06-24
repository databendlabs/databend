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

use databend_common_catalog::plan::StageTableInfo;
use databend_common_exception::Result;
use databend_common_expression::types::DataType;
use databend_common_expression::types::NumberDataType;
use databend_common_expression::DataField;
use databend_common_expression::DataSchemaRef;
use databend_common_expression::DataSchemaRefExt;

use crate::executor::PhysicalPlan;
use crate::ColumnBinding;

#[derive(Clone, Debug, serde::Serialize, serde::Deserialize)]
pub struct CopyIntoLocation {
    pub plan_id: u32,
    pub input: Box<PhysicalPlan>,
    pub project_columns: Vec<ColumnBinding>,
    pub input_schema: DataSchemaRef,
    pub to_stage_info: StageTableInfo,
}

impl CopyIntoLocation {
    pub fn output_schema(&self) -> Result<DataSchemaRef> {
        Ok(DataSchemaRefExt::create(vec![
            DataField::new("rows_unloaded", DataType::Number(NumberDataType::UInt64)),
            DataField::new("input_bytes", DataType::Number(NumberDataType::UInt64)),
            DataField::new("output_bytes", DataType::Number(NumberDataType::UInt64)),
        ]))
    }
}
