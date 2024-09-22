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

use databend_common_exception::ErrorCode;
use databend_common_exception::Result;
use databend_common_expression::types::DataType;
use databend_common_expression::DataField;
use databend_common_expression::DataSchemaRef;
use databend_common_expression::DataSchemaRefExt;

use crate::executor::explain::PlanStatsInfo;
use crate::executor::PhysicalPlan;
use crate::executor::PhysicalPlanBuilder;
use crate::optimizer::SExpr;
use crate::plans::AsyncFunctionArgument;
use crate::ColumnSet;
use crate::IndexType;
use crate::ScalarExpr;

#[derive(Clone, Debug, serde::Serialize, serde::Deserialize)]
pub struct AsyncFunction {
    // A unique id of operator in a `PhysicalPlan` tree, only used for display.
    pub plan_id: u32,
    pub input: Box<PhysicalPlan>,
    pub async_func_descs: Vec<AsyncFunctionDesc>,
    // Only used for explain
    pub stat_info: Option<PlanStatsInfo>,
}

impl AsyncFunction {
    pub fn output_schema(&self) -> Result<DataSchemaRef> {
        let input_schema = self.input.output_schema()?;
        let mut fields = input_schema.fields().clone();
        for async_func_desc in self.async_func_descs.iter() {
            let name = async_func_desc.output_column.to_string();
            let data_type = async_func_desc.data_type.clone();
            fields.push(DataField::new(&name, *data_type));
        }
        Ok(DataSchemaRefExt::create(fields))
    }
}

#[derive(Clone, Debug, Eq, PartialEq, serde::Serialize, serde::Deserialize)]
pub struct AsyncFunctionDesc {
    pub func_name: String,
    pub display_name: String,
    pub output_column: IndexType,
    pub arg_indices: Vec<IndexType>,
    pub data_type: Box<DataType>,

    pub func_arg: AsyncFunctionArgument,
}

impl PhysicalPlanBuilder {
    pub(crate) async fn build_async_func(
        &mut self,
        s_expr: &SExpr,
        async_func_plan: &crate::plans::AsyncFunction,
        mut required: ColumnSet,
        stat_info: PlanStatsInfo,
    ) -> Result<PhysicalPlan> {
        // 1. Prune unused Columns.
        let mut used = vec![];
        for item in async_func_plan.items.iter() {
            if required.contains(&item.index) {
                required.extend(item.scalar.used_columns());
                used.push(item.clone());
            }
        }

        // 2. Build physical plan.
        if used.is_empty() {
            return self.build(s_expr.child(0)?, required).await;
        }
        let input = self.build(s_expr.child(0)?, required).await?;
        let input_schema = input.output_schema()?;

        let async_func_descs = used
            .iter()
            .map(|item| {
                if let ScalarExpr::AsyncFunctionCall(async_func) = &item.scalar {
                    let arg_indices = async_func
                        .arguments
                        .iter()
                        .map(|arg| match arg {
                            ScalarExpr::BoundColumnRef(col) => {
                                let index =
                                    match input_schema.index_of(&col.column.index.to_string()) {
                                        Ok(index) => index,
                                        Err(_) => {
                                            return Err(ErrorCode::Internal(format!(
                                                "Unable to get async function's argument \"{}\".",
                                                col.column.column_name
                                            )));
                                        }
                                    };
                                Ok(index)
                            }
                            _ => Err(ErrorCode::Internal(
                                "Async function's argument must be a BoundColumnRef".to_string(),
                            )),
                        })
                        .collect::<Result<Vec<_>>>()?;

                    let async_func_desc = AsyncFunctionDesc {
                        func_name: async_func.func_name.clone(),
                        display_name: async_func.display_name.clone(),
                        output_column: item.index,
                        arg_indices,
                        data_type: async_func.return_type.clone(),
                        func_arg: async_func.func_arg.clone(),
                    };
                    Ok(async_func_desc)
                } else {
                    Err(ErrorCode::Internal("Expected async function".to_string()))
                }
            })
            .collect::<Result<Vec<_>>>()?;

        Ok(PhysicalPlan::AsyncFunction(AsyncFunction {
            plan_id: 0,
            input: Box::new(input),
            async_func_descs,
            stat_info: Some(stat_info),
        }))
    }
}
