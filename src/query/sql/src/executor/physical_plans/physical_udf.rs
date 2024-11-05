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
use databend_common_functions::BUILTIN_FUNCTIONS;

use crate::executor::explain::PlanStatsInfo;
use crate::executor::PhysicalPlan;
use crate::executor::PhysicalPlanBuilder;
use crate::optimizer::SExpr;
use crate::plans::UDFType;
use crate::ColumnSet;
use crate::IndexType;
use crate::ScalarExpr;

#[derive(Clone, Debug, serde::Serialize, serde::Deserialize)]
pub struct Udf {
    // A unique id of operator in a `PhysicalPlan` tree, only used for display.
    pub plan_id: u32,
    pub input: Box<PhysicalPlan>,
    pub udf_funcs: Vec<UdfFunctionDesc>,
    pub script_udf: bool,
    // Only used for explain
    pub stat_info: Option<PlanStatsInfo>,
}

impl Udf {
    pub fn output_schema(&self) -> Result<DataSchemaRef> {
        let input_schema = self.input.output_schema()?;
        let mut fields = input_schema.fields().clone();
        for udf_func in self.udf_funcs.iter() {
            let name = udf_func.output_column.to_string();
            let data_type = udf_func.data_type.clone();
            fields.push(DataField::new(&name, *data_type));
        }
        Ok(DataSchemaRefExt::create(fields))
    }
}

#[derive(Clone, Debug, Eq, PartialEq, serde::Serialize, serde::Deserialize)]
pub struct UdfFunctionDesc {
    pub name: String,
    pub func_name: String,
    pub output_column: IndexType,
    pub arg_indices: Vec<IndexType>,
    pub arg_exprs: Vec<String>,
    pub data_type: Box<DataType>,

    pub udf_type: UDFType,
}

impl PhysicalPlanBuilder {
    pub(crate) async fn build_udf(
        &mut self,
        s_expr: &SExpr,
        udf_plan: &crate::plans::Udf,
        mut required: ColumnSet,
        stat_info: PlanStatsInfo,
    ) -> Result<PhysicalPlan> {
        // 1. Prune unused Columns.
        let mut used = vec![];
        for item in udf_plan.items.iter() {
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

        let udf_funcs = used
            .iter()
            .map(|item| {
                if let ScalarExpr::UDFCall(func) = &item.scalar {
                    let arg_indices = func
                        .arguments
                        .iter()
                        .map(|arg| match arg {
                            ScalarExpr::BoundColumnRef(col) => {
                                let index =
                                    match input_schema.index_of(&col.column.index.to_string()) {
                                        Ok(index) => index,
                                        Err(_) => {
                                            return Err(ErrorCode::Internal(format!(
                                                "Unable to get udf function's argument \"{}\".",
                                                col.column.column_name
                                            )));
                                        }
                                    };
                                Ok(index)
                            }
                            _ => Err(ErrorCode::Internal(
                                "Udf function's argument must be a BoundColumnRef".to_string(),
                            )),
                        })
                        .collect::<Result<Vec<_>>>()?;

                    let arg_exprs = func
                        .arguments
                        .iter()
                        .map(|arg| {
                            let expr = arg.as_expr()?;
                            let remote_expr = expr.as_remote_expr();
                            Ok(remote_expr.as_expr(&BUILTIN_FUNCTIONS).sql_display())
                        })
                        .collect::<Result<Vec<_>>>()?;

                    let udf_func = UdfFunctionDesc {
                        name: func.name.clone(),
                        func_name: func.func_name.clone(),
                        output_column: item.index,
                        arg_indices,
                        arg_exprs,
                        data_type: func.return_type.clone(),
                        udf_type: func.udf_type.clone(),
                    };
                    Ok(udf_func)
                } else {
                    Err(ErrorCode::Internal("Expected udf function".to_string()))
                }
            })
            .collect::<Result<Vec<_>>>()?;

        Ok(PhysicalPlan::Udf(Udf {
            plan_id: 0,
            input: Box::new(input),
            udf_funcs,
            script_udf: udf_plan.script_udf,
            stat_info: Some(stat_info),
        }))
    }
}
