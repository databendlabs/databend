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

use databend_common_catalog::plan::DataSourcePlan;
use databend_common_catalog::plan::Projection;
use databend_common_exception::Result;
use databend_common_expression::DataField;
use databend_common_expression::DataSchemaRef;
use databend_common_expression::DataSchemaRefExt;

use crate::executor::explain::PlanStatsInfo;
use crate::executor::PhysicalPlan;

#[derive(Clone, Debug, serde::Serialize, serde::Deserialize)]
pub struct RowFetch {
    // A unique id of operator in a `PhysicalPlan` tree, only used for display.
    pub plan_id: u32,
    pub input: Box<PhysicalPlan>,
    // cloned from `input`.
    pub source: Box<DataSourcePlan>,
    // projection on the source table schema.
    pub cols_to_fetch: Projection,
    pub row_id_col_offset: usize,
    pub fetched_fields: Vec<DataField>,
    pub need_wrap_nullable: bool,

    /// Only used for explain
    pub stat_info: Option<PlanStatsInfo>,
}

impl RowFetch {
    pub fn output_schema(&self) -> Result<DataSchemaRef> {
        let mut fields = self.input.output_schema()?.fields().clone();
        fields.extend_from_slice(&self.fetched_fields);
        Ok(DataSchemaRefExt::create(fields))
    }
}
