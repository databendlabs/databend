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

use std::collections::HashMap;

use common_exception::ErrorCode;
use common_exception::Result;
use common_expression::types::DataType;
use common_expression::ConstantFolder;
use common_expression::DataField;
use common_expression::DataSchema;
use common_expression::DataSchemaRef;
use common_expression::DataSchemaRefExt;
use common_expression::RemoteExpr;
use common_functions::BUILTIN_FUNCTIONS;

use crate::executor::explain::PlanStatsInfo;
use crate::executor::PhysicalPlan;
use crate::executor::PhysicalPlanBuilder;
use crate::optimizer::SExpr;
use crate::ColumnSet;
use crate::IndexType;
use crate::ScalarExpr;
use crate::TypeCheck;

#[derive(Clone, Debug, serde::Serialize, serde::Deserialize)]
pub struct Lambda {
    // A unique id of operator in a `PhysicalPlan` tree, only used for display.
    pub plan_id: u32,
    pub input: Box<PhysicalPlan>,
    pub lambda_funcs: Vec<LambdaFunctionDesc>,

    // Only used for explain
    pub stat_info: Option<PlanStatsInfo>,
}

impl Lambda {
    pub fn output_schema(&self) -> Result<DataSchemaRef> {
        let input_schema = self.input.output_schema()?;
        let mut fields = input_schema.fields().clone();
        for lambda_func in self.lambda_funcs.iter() {
            let name = lambda_func.output_column.to_string();
            let data_type = lambda_func.data_type.clone();
            fields.push(DataField::new(&name, *data_type));
        }
        Ok(DataSchemaRefExt::create(fields))
    }
}

#[derive(Clone, Debug, Eq, PartialEq, serde::Serialize, serde::Deserialize)]
pub struct LambdaFunctionDesc {
    pub func_name: String,
    pub output_column: IndexType,
    pub arg_indices: Vec<IndexType>,
    pub arg_exprs: Vec<String>,
    pub params: Vec<String>,
    pub lambda_expr: RemoteExpr,
    pub data_type: Box<DataType>,
}

impl PhysicalPlanBuilder {
    pub(crate) async fn build_lambda(
        &mut self,
        s_expr: &SExpr,
        lambda: &crate::plans::Lambda,
        mut required: ColumnSet,
        stat_info: PlanStatsInfo,
    ) -> Result<PhysicalPlan> {
        // 1. Prune unused Columns.
        let mut used = vec![];
        // Keep all columns, as some lambda functions may be arguments to other lambda functions.
        for s in lambda.items.iter() {
            used.push(s.clone());
            s.scalar.used_columns().iter().for_each(|c| {
                required.insert(*c);
            })
        }

        // 2. Build physical plan.
        if used.is_empty() {
            return self.build(s_expr.child(0)?, required).await;
        }
        let lambda = crate::plans::Lambda { items: used };
        let input = self.build(s_expr.child(0)?, required).await?;
        let input_schema = input.output_schema()?;
        let mut index = input_schema.num_fields();
        let mut lambda_index_map = HashMap::new();
        let lambda_funcs = lambda
            .items
            .iter()
            .map(|item| {
                if let ScalarExpr::LambdaFunction(func) = &item.scalar {
                    let arg_indices = func
                        .args
                        .iter()
                        .map(|arg| {
                            match arg {
                                ScalarExpr::BoundColumnRef(col) => {
                                    let index = match input_schema.index_of(&col.column.index.to_string()) {
                                        Ok(index) => index,
                                        Err(_) => {
                                            // the argument of lambda function may be another lambda function
                                            match lambda_index_map.get(&col.column.column_name) {
                                                Some(index) => *index,
                                                None => {
                                                    return Err(ErrorCode::Internal(format!(
                                                        "Unable to get lambda function's argument \"{}\".",
                                                        col.column.column_name
                                                    )))
                                                }
                                            }
                                        }
                                    };
                                    Ok(index)
                                }
                                _ => {
                                    Err(ErrorCode::Internal(
                                        "Lambda function's argument must be a BoundColumnRef"
                                            .to_string(),
                                    ))
                                }
                            }
                        })
                        .collect::<Result<Vec<_>>>()?;

                    lambda_index_map.insert(
                        func.display_name.clone(),
                        index,
                    );
                    index += 1;

                    let arg_exprs = func
                        .args
                        .iter()
                        .map(|arg| {
                            let expr = arg.as_expr()?;
                            let remote_expr = expr.as_remote_expr();
                            Ok(remote_expr.as_expr(&BUILTIN_FUNCTIONS).sql_display())
                        })
                        .collect::<Result<Vec<_>>>()?;

                    let params = func
                        .params
                        .iter()
                        .map(|(param, _)| param.clone())
                        .collect::<Vec<_>>();

                    // build schema for lambda expr.
                    let mut field_index = 0;
                    let lambda_fields = func
                        .params
                        .iter()
                        .map(|(_, ty)| {
                            let field = DataField::new(&field_index.to_string(), ty.clone());
                            field_index += 1;
                            field
                        })
                        .collect::<Vec<_>>();
                    let lambda_schema = DataSchema::new(lambda_fields);

                    let expr = func
                        .lambda_expr
                        .type_check(&lambda_schema)?
                        .project_column_ref(|index| {
                            lambda_schema.index_of(&index.to_string()).unwrap()
                        });
                    let (expr, _) =
                        ConstantFolder::fold(&expr, &self.func_ctx, &BUILTIN_FUNCTIONS);
                    let lambda_expr = expr.as_remote_expr();

                    let lambda_func = LambdaFunctionDesc {
                        func_name: func.func_name.clone(),
                        output_column: item.index,
                        arg_indices,
                        arg_exprs,
                        params,
                        lambda_expr,
                        data_type: func.return_type.clone(),
                    };
                    Ok(lambda_func)
                } else {
                    Err(ErrorCode::Internal("Expected lambda function".to_string()))
                }
            })
            .collect::<Result<Vec<_>>>()?;

        Ok(PhysicalPlan::Lambda(Lambda {
            plan_id: self.next_plan_id(),
            input: Box::new(input),
            lambda_funcs,
            stat_info: Some(stat_info),
        }))
    }
}
