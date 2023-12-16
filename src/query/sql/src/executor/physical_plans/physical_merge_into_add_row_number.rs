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

use std::collections::BTreeMap;
use std::sync::Arc;

use databend_common_exception::ErrorCode;
use databend_common_exception::Result;
use databend_common_expression::types::DataType;
use databend_common_expression::types::NumberDataType;
use databend_common_expression::DataField;
use databend_common_expression::DataSchema;
use databend_common_expression::DataSchemaRef;
use databend_common_expression::ROW_NUMBER_COL_NAME;

use crate::executor::PhysicalPlan;
use crate::executor::PhysicalPlanBuilder;
use crate::optimizer::ColumnSet;
use crate::optimizer::SExpr;

// add row_number for distributed merge into
#[derive(Clone, Debug, serde::Serialize, serde::Deserialize)]
pub struct MergeIntoAddRowNumber {
    pub cluster_index: BTreeMap<String, usize>,
    pub input: Box<PhysicalPlan>,
    pub output_schema: DataSchemaRef,
}

impl MergeIntoAddRowNumber {
    pub fn output_schema(&self) -> Result<DataSchemaRef> {
        Ok(self.output_schema.clone())
    }
}

impl PhysicalPlanBuilder {
    pub(crate) async fn build_add_row_number(
        &mut self,
        s_expr: &SExpr,
        required: ColumnSet,
    ) -> Result<PhysicalPlan> {
        let input_plan = self.build(s_expr.child(0)?, required).await?;
        if self.ctx.get_cluster().is_empty() {
            return Err(ErrorCode::CannotConnectNode(
                "there is only one node when build distributed merge into",
            ));
        }
        let mut cluster_index = BTreeMap::new();
        for (id, node) in self.ctx.get_cluster().nodes.iter().enumerate() {
            cluster_index.insert(node.id.clone(), id);
        }
        let input_schema = input_plan.output_schema()?;
        let mut fields = input_schema.fields.clone();
        fields.push(DataField::new(
            ROW_NUMBER_COL_NAME,
            DataType::Number(NumberDataType::UInt64),
        ));
        let meta = input_schema.meta().clone();

        Ok(PhysicalPlan::MergeIntoAddRowNumber(Box::new(
            MergeIntoAddRowNumber {
                cluster_index,
                input: Box::new(input_plan),
                output_schema: Arc::new(DataSchema::new_from(fields, meta)),
            },
        )))
    }
}
