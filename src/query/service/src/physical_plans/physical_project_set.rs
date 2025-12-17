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
use databend_common_exception::Result;
use databend_common_expression::ConstantFolder;
use databend_common_expression::DataField;
use databend_common_expression::DataSchemaRef;
use databend_common_expression::DataSchemaRefExt;
use databend_common_expression::RemoteExpr;
use databend_common_functions::BUILTIN_FUNCTIONS;
use databend_common_pipeline::core::ProcessorPtr;
use databend_common_sql::ColumnSet;
use databend_common_sql::IndexType;
use databend_common_sql::TypeCheck;
use databend_common_sql::optimizer::ir::SExpr;
use itertools::Itertools;

use crate::physical_plans::PhysicalPlanBuilder;
use crate::physical_plans::explain::PlanStatsInfo;
use crate::physical_plans::format::PhysicalFormat;
use crate::physical_plans::format::ProjectSetFormatter;
use crate::physical_plans::physical_plan::IPhysicalPlan;
use crate::physical_plans::physical_plan::PhysicalPlan;
use crate::physical_plans::physical_plan::PhysicalPlanMeta;
use crate::pipelines::PipelineBuilder;
use crate::pipelines::processors::transforms::TransformSRF;

#[derive(Clone, Debug, serde::Serialize, serde::Deserialize)]
pub struct ProjectSet {
    meta: PhysicalPlanMeta,
    pub projections: ColumnSet,
    pub input: PhysicalPlan,
    pub srf_exprs: Vec<(RemoteExpr, IndexType)>,

    // Only used for explain
    pub stat_info: Option<PlanStatsInfo>,
}

impl IPhysicalPlan for ProjectSet {
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
        let mut fields = Vec::with_capacity(input_schema.num_fields() + self.srf_exprs.len());
        for (i, field) in input_schema.fields().iter().enumerate() {
            if self.projections.contains(&i) {
                fields.push(field.clone());
            }
        }
        fields.extend(self.srf_exprs.iter().map(|(srf, index)| {
            DataField::new(
                &index.to_string(),
                srf.as_expr(&BUILTIN_FUNCTIONS).data_type().clone(),
            )
        }));
        Ok(DataSchemaRefExt::create(fields))
    }

    fn children<'a>(&'a self) -> Box<dyn Iterator<Item = &'a PhysicalPlan> + 'a> {
        Box::new(std::iter::once(&self.input))
    }

    fn children_mut<'a>(&'a mut self) -> Box<dyn Iterator<Item = &'a mut PhysicalPlan> + 'a> {
        Box::new(std::iter::once(&mut self.input))
    }

    fn formatter(&self) -> Result<Box<dyn PhysicalFormat + '_>> {
        Ok(ProjectSetFormatter::create(self))
    }

    #[recursive::recursive]
    fn try_find_single_data_source(&self) -> Option<&DataSourcePlan> {
        self.input.try_find_single_data_source()
    }

    fn get_desc(&self) -> Result<String> {
        Ok(self
            .srf_exprs
            .iter()
            .map(|(x, _)| x.as_expr(&BUILTIN_FUNCTIONS).sql_display())
            .join(", "))
    }

    fn derive(&self, mut children: Vec<PhysicalPlan>) -> PhysicalPlan {
        assert_eq!(children.len(), 1);
        let input = children.pop().unwrap();
        PhysicalPlan::new(ProjectSet {
            meta: self.meta.clone(),
            projections: self.projections.clone(),
            input,
            srf_exprs: self.srf_exprs.clone(),
            stat_info: self.stat_info.clone(),
        })
    }

    fn build_pipeline2(&self, builder: &mut PipelineBuilder) -> Result<()> {
        self.input.build_pipeline(builder)?;

        let srf_exprs = self
            .srf_exprs
            .iter()
            .map(|(expr, _)| expr.as_expr(&BUILTIN_FUNCTIONS))
            .collect::<Vec<_>>();
        let max_block_size = builder.settings.get_max_block_size()? as usize;
        let max_block_bytes = builder.settings.get_max_block_bytes()? as usize;

        builder.main_pipeline.add_transform(|input, output| {
            Ok(ProcessorPtr::create(TransformSRF::try_create(
                input,
                output,
                builder.func_ctx.clone(),
                self.projections.clone(),
                srf_exprs.clone(),
                max_block_size,
                max_block_bytes,
            )))
        })
    }
}

impl PhysicalPlanBuilder {
    pub async fn build_project_set(
        &mut self,
        s_expr: &SExpr,
        project_set: &databend_common_sql::plans::ProjectSet,
        mut required: ColumnSet,
        stat_info: PlanStatsInfo,
    ) -> Result<PhysicalPlan> {
        // 1. Prune unused Columns.
        let column_projections = required.clone().into_iter().collect::<Vec<_>>();
        for s in project_set.srfs.iter() {
            required.extend(s.scalar.used_columns().iter().copied());
        }

        // 2. Build physical plan.
        let input = self.build(s_expr.child(0)?, required).await?;
        let input_schema = input.output_schema()?;
        let srf_exprs = project_set
            .srfs
            .iter()
            .map(|item| {
                let expr = item
                    .scalar
                    .type_check(input_schema.as_ref())?
                    .project_column_ref(|index| input_schema.index_of(&index.to_string()))?;
                let (expr, _) = ConstantFolder::fold(&expr, &self.func_ctx, &BUILTIN_FUNCTIONS);
                Ok((expr.as_remote_expr(), item.index))
            })
            .collect::<Result<Vec<_>>>()?;

        let mut projections = ColumnSet::new();
        for column in column_projections.iter() {
            if let Some((index, _)) = input_schema.column_with_name(&column.to_string()) {
                projections.insert(index);
            }
        }

        Ok(PhysicalPlan::new(ProjectSet {
            input,
            meta: PhysicalPlanMeta::new("Unnest"),
            srf_exprs,
            projections,
            stat_info: Some(stat_info),
        }))
    }
}

crate::register_physical_plan!(ProjectSet => crate::physical_plans::physical_project_set::ProjectSet);
