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

use databend_common_catalog::plan::DataSourcePlan;
use databend_common_exception::ErrorCode;
use databend_common_exception::Result;
use databend_common_expression::DataField;
use databend_common_expression::DataSchemaRef;
use databend_common_expression::DataSchemaRefExt;
use databend_common_pipeline_transforms::TransformPipelineHelper;
use databend_common_sql::ColumnSet;
use databend_common_sql::ScalarExpr;
use databend_common_sql::binder::AsyncFunctionDesc;
use databend_common_sql::optimizer::ir::SExpr;
use itertools::Itertools;

use crate::physical_plans::explain::PlanStatsInfo;
use crate::physical_plans::format::AsyncFunctionFormatter;
use crate::physical_plans::format::PhysicalFormat;
use crate::physical_plans::physical_plan::IPhysicalPlan;
use crate::physical_plans::physical_plan::PhysicalPlan;
use crate::physical_plans::physical_plan::PhysicalPlanMeta;
use crate::physical_plans::physical_plan_builder::PhysicalPlanBuilder;
use crate::pipelines::PipelineBuilder;
use crate::pipelines::processors::transforms::TransformAsyncFunction;

#[derive(Clone, Debug, serde::Serialize, serde::Deserialize)]
pub struct AsyncFunction {
    pub meta: PhysicalPlanMeta,
    pub input: PhysicalPlan,
    pub async_func_descs: Vec<AsyncFunctionDesc>,
    // Only used for explain
    pub stat_info: Option<PlanStatsInfo>,
}

#[typetag::serde]
impl IPhysicalPlan for AsyncFunction {
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
        for async_func_desc in self.async_func_descs.iter() {
            let name = async_func_desc.output_column.to_string();
            let data_type = async_func_desc.data_type.clone();
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
        Ok(AsyncFunctionFormatter::create(self))
    }

    #[recursive::recursive]
    fn try_find_single_data_source(&self) -> Option<&DataSourcePlan> {
        self.input.try_find_single_data_source()
    }

    fn get_desc(&self) -> Result<String> {
        Ok(self
            .async_func_descs
            .iter()
            .map(|x| x.display_name.clone())
            .join(", "))
    }

    fn derive(&self, mut children: Vec<PhysicalPlan>) -> PhysicalPlan {
        assert_eq!(children.len(), 1);

        PhysicalPlan::new(AsyncFunction {
            meta: self.meta.clone(),
            input: children.remove(0),
            async_func_descs: self.async_func_descs.clone(),
            stat_info: self.stat_info.clone(),
        })
    }

    fn build_pipeline2(&self, builder: &mut PipelineBuilder) -> Result<()> {
        self.input.build_pipeline(builder)?;

        let operators = TransformAsyncFunction::init_operators(&self.async_func_descs)?;
        let sequence_counters =
            TransformAsyncFunction::create_sequence_counters(self.async_func_descs.len());

        builder.main_pipeline.try_add_async_transformer(|| {
            TransformAsyncFunction::new(
                builder.ctx.clone(),
                self.async_func_descs.clone(),
                operators.clone(),
                sequence_counters.clone(),
            )
        })?;

        Ok(())
    }
}

impl PhysicalPlanBuilder {
    pub async fn build_async_func(
        &mut self,
        s_expr: &SExpr,
        async_func_plan: &databend_common_sql::plans::AsyncFunction,
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

        Ok(PhysicalPlan::new(AsyncFunction {
            input,
            meta: PhysicalPlanMeta::new("AsyncFunction"),
            async_func_descs,
            stat_info: Some(stat_info),
        }))
    }
}
