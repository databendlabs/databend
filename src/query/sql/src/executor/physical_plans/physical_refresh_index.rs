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

use std::fmt::Display;
use std::fmt::Formatter;

use databend_common_exception::Result;
use databend_common_expression::types::DataType;
use databend_common_expression::DataField;
use databend_common_expression::DataSchemaRef;
use databend_common_expression::DataSchemaRefExt;
use databend_common_meta_app::schema::TableInfo;

use crate::executor::PhysicalPlan;
use crate::ColumnBinding;

#[derive(Clone, Debug, serde::Serialize, serde::Deserialize)]
pub struct RefreshIndex {
    pub input: Box<PhysicalPlan>,
    pub index_id: u64,
    pub table_info: TableInfo,
    pub select_schema: DataSchemaRef,
    pub select_column_bindings: Vec<ColumnBinding>,
}

impl RefreshIndex {
    pub fn _output_schema(&self) -> Result<DataSchemaRef> {
        Ok(DataSchemaRefExt::create(vec![DataField::new(
            "index_loc",
            DataType::String,
        )]))
    }
}

impl Display for RefreshIndex {
    fn fmt(&self, f: &mut Formatter) -> std::fmt::Result {
        write!(f, "RefreshIndex")
    }
}
