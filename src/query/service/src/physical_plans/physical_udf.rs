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

use std::any::Any;
use std::collections::BTreeMap;

use databend_common_catalog::plan::DataSourcePlan;
use databend_common_exception::ErrorCode;
use databend_common_exception::Result;
use databend_common_expression::types::DataType;
use databend_common_expression::DataField;
use databend_common_expression::DataSchemaRef;
use databend_common_expression::DataSchemaRefExt;
use databend_common_functions::BUILTIN_FUNCTIONS;
use databend_common_pipeline_transforms::TransformPipelineHelper;
use databend_common_sql::optimizer::ir::SExpr;
use databend_common_sql::plans::UDFType;
use databend_common_sql::ColumnSet;
use databend_common_sql::IndexType;
use databend_common_sql::ScalarExpr;
use itertools::Itertools;

use crate::physical_plans::explain::PlanStatsInfo;
use crate::physical_plans::format::PhysicalFormat;
use crate::physical_plans::format::UdfFormatter;
use crate::physical_plans::physical_plan::IPhysicalPlan;
use crate::physical_plans::physical_plan::PhysicalPlan;
use crate::physical_plans::physical_plan::PhysicalPlanMeta;
use crate::physical_plans::PhysicalPlanBuilder;
use crate::pipelines::processors::transforms::TransformUdfScript;
use crate::pipelines::processors::transforms::TransformUdfServer;
use crate::pipelines::PipelineBuilder;

#[derive(Clone, Debug, serde::Serialize, serde::Deserialize)]
pub struct Udf {
    pub meta: PhysicalPlanMeta,
    pub input: PhysicalPlan,
    pub udf_funcs: Vec<UdfFunctionDesc>,
    pub script_udf: bool,
    // Only used for explain
    pub stat_info: Option<PlanStatsInfo>,
}

#[typetag::serde]
impl IPhysicalPlan for Udf {
    fn as_any(&self) -> &dyn Any {
        self
    }
    fn get_meta(&self) -> &PhysicalPlanMeta {
        &self.meta
    }

    fn get_meta_mut(&mut self) -> &mut PhysicalPlanMeta {
        &mut self.meta
    }

    #[recursive::recursive]
    fn output_schema(&self) -> Result<DataSchemaRef> {
        let input_schema = self.input.output_schema()?;
        let mut fields = input_schema.fields().clone();
        for udf_func in self.udf_funcs.iter() {
            let name = udf_func.output_column.to_string();
            let data_type = udf_func.data_type.clone();
            fields.push(DataField::new(&name, *data_type));
        }
        Ok(DataSchemaRefExt::create(fields))
    }

    fn children<'a>(&'a self) -> Box<dyn Iterator<Item = &'a PhysicalPlan> + 'a> {
        Box::new(std::iter::once(&self.input))
    }

    fn children_mut<'a>(&'a mut self) -> Box<dyn Iterator<Item = &'a mut PhysicalPlan> + 'a> {
        Box::new(std::iter::once(&mut self.input))
    }

    fn formatter(&self) -> Result<Box<dyn PhysicalFormat + '_>> {
        Ok(UdfFormatter::create(self))
    }

    #[recursive::recursive]
    fn try_find_single_data_source(&self) -> Option<&DataSourcePlan> {
        self.input.try_find_single_data_source()
    }

    fn get_desc(&self) -> Result<String> {
        Ok(self
            .udf_funcs
            .iter()
            .map(|x| format!("{}({})", x.func_name, x.arg_exprs.join(", ")))
            .join(", "))
    }

    fn derive(&self, mut children: Vec<PhysicalPlan>) -> PhysicalPlan {
        assert_eq!(children.len(), 1);
        let input = children.pop().unwrap();
        PhysicalPlan::new(Udf {
            meta: self.meta.clone(),
            input,
            udf_funcs: self.udf_funcs.clone(),
            script_udf: self.script_udf,
            stat_info: self.stat_info.clone(),
        })
    }

    fn build_pipeline2(&self, builder: &mut PipelineBuilder) -> Result<()> {
        self.input.build_pipeline(builder)?;

        if self.script_udf {
            let runtimes = TransformUdfScript::init_runtime(&self.udf_funcs)?;
            builder.main_pipeline.try_add_transformer(|| {
                Ok(TransformUdfScript::new(
                    builder.func_ctx.clone(),
                    self.udf_funcs.clone(),
                    runtimes.clone(),
                ))
            })
        } else {
            let semaphore = TransformUdfServer::init_semaphore(builder.ctx.clone())?;
            let endpoints =
                TransformUdfServer::init_endpoints(builder.ctx.clone(), &self.udf_funcs)?;
            builder.main_pipeline.try_add_async_transformer(|| {
                TransformUdfServer::new(
                    builder.ctx.clone(),
                    self.udf_funcs.clone(),
                    semaphore.clone(),
                    endpoints.clone(),
                )
            })
        }
    }
}

#[derive(Clone, Debug, Eq, Hash, PartialEq, serde::Serialize, serde::Deserialize)]
pub struct UdfFunctionDesc {
    pub name: String,
    pub func_name: String,
    pub output_column: IndexType,
    pub arg_indices: Vec<IndexType>,
    pub arg_exprs: Vec<String>,
    pub data_type: Box<DataType>,
    pub headers: BTreeMap<String, String>,

    pub udf_type: UDFType,
}

impl PhysicalPlanBuilder {
    pub async fn build_udf(
        &mut self,
        s_expr: &SExpr,
        udf_plan: &databend_common_sql::plans::Udf,
        required: ColumnSet,
        stat_info: PlanStatsInfo,
    ) -> Result<PhysicalPlan> {
        // 1. Prune unused Columns.
        let mut used = vec![];
        for item in udf_plan.items.iter() {
            if required.contains(&item.index) {
                used.push(item.clone());
            }
        }

        let mut child_required = self.derive_child_required_columns(s_expr, &required)?;
        debug_assert_eq!(child_required.len(), s_expr.arity());
        let child_required = child_required.remove(0);

        // 2. Build physical plan.
        if used.is_empty() {
            return self.build(s_expr.child(0)?, child_required).await;
        }
        let input = self.build(s_expr.child(0)?, child_required).await?;
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
                        func_name: func.handler.clone(),
                        output_column: item.index,
                        arg_indices,
                        arg_exprs,
                        data_type: func.return_type.clone(),
                        headers: func.headers.clone(),
                        udf_type: func.udf_type.clone(),
                    };
                    Ok(udf_func)
                } else {
                    Err(ErrorCode::Internal("Expected udf function".to_string()))
                }
            })
            .collect::<Result<Vec<_>>>()?;

        Ok(PhysicalPlan::new(Udf {
            input,
            udf_funcs,
            script_udf: udf_plan.script_udf,
            stat_info: Some(stat_info),
            meta: PhysicalPlanMeta::new("Udf"),
        }))
    }
}
