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
use std::collections::HashMap;

use databend_common_catalog::plan::DataSourcePlan;
use databend_common_exception::Result;
use databend_common_expression::ConstantFolder;
use databend_common_expression::DataSchemaRef;
use databend_common_expression::DataSchemaRefExt;
use databend_common_expression::RemoteExpr;
use databend_common_functions::BUILTIN_FUNCTIONS;
use databend_common_sql::ColumnSet;
use databend_common_sql::TypeCheck;
use databend_common_sql::executor::cast_expr_to_non_null_boolean;
use databend_common_sql::optimizer::ir::SExpr;

use crate::physical_plans::PhysicalPlanBuilder;
use crate::physical_plans::explain::PlanStatsInfo;
use crate::physical_plans::format::PhysicalFormat;
use crate::physical_plans::format::SecureFilterFormatter;
use crate::physical_plans::physical_plan::IPhysicalPlan;
use crate::physical_plans::physical_plan::PhysicalPlan;
use crate::physical_plans::physical_plan::PhysicalPlanMeta;
use crate::pipelines::PipelineBuilder;

#[derive(Clone, Debug, serde::Serialize, serde::Deserialize)]
pub struct SecureFilter {
    meta: PhysicalPlanMeta,
    pub projections: ColumnSet,
    pub input: PhysicalPlan,
    // Assumption: expression's data type must be `DataType::Boolean`.
    pub predicates: Vec<RemoteExpr>,

    // Only used for explain
    pub stat_info: Option<PlanStatsInfo>,
}

#[typetag::serde]
impl IPhysicalPlan for SecureFilter {
    fn as_any(&self) -> &dyn Any {
        self
    }
    fn get_meta(&self) -> &PhysicalPlanMeta {
        &self.meta
    }

    fn get_meta_mut(&mut self) -> &mut PhysicalPlanMeta {
        &mut self.meta
    }

    #[stacksafe::stacksafe]
    fn output_schema(&self) -> Result<DataSchemaRef> {
        let input_schema = self.input.output_schema()?;
        let mut fields = Vec::with_capacity(self.projections.len());
        for (i, field) in input_schema.fields().iter().enumerate() {
            if self.projections.contains(&i) {
                fields.push(field.clone());
            }
        }
        Ok(DataSchemaRefExt::create(fields))
    }

    fn children<'a>(&'a self) -> Box<dyn Iterator<Item = &'a PhysicalPlan> + 'a> {
        Box::new(std::iter::once(&self.input))
    }

    fn children_mut<'a>(&'a mut self) -> Box<dyn Iterator<Item = &'a mut PhysicalPlan> + 'a> {
        Box::new(std::iter::once(&mut self.input))
    }

    #[stacksafe::stacksafe]
    fn formatter(&self) -> Result<Box<dyn PhysicalFormat + '_>> {
        Ok(SecureFilterFormatter::create(self))
    }

    #[stacksafe::stacksafe]
    fn try_find_single_data_source(&self) -> Option<&DataSourcePlan> {
        self.input.try_find_single_data_source()
    }

    fn get_desc(&self) -> Result<String> {
        Ok(match self.predicates.is_empty() {
            true => String::new(),
            false => format!(
                "SECURE: {}",
                self.predicates[0].as_expr(&BUILTIN_FUNCTIONS).sql_display()
            ),
        })
    }

    fn get_labels(&self) -> Result<HashMap<String, Vec<String>>> {
        Ok(HashMap::from([(
            String::from("Secure Filter condition"),
            self.predicates
                .iter()
                .map(|x| format!("SECURE: {}", x.as_expr(&BUILTIN_FUNCTIONS).sql_display()))
                .collect(),
        )]))
    }

    fn derive(&self, mut children: Vec<PhysicalPlan>) -> PhysicalPlan {
        assert_eq!(children.len(), 1);
        let input = children.pop().unwrap();
        PhysicalPlan::new(SecureFilter {
            meta: self.meta.clone(),
            projections: self.projections.clone(),
            input,
            predicates: self.predicates.clone(),
            stat_info: self.stat_info.clone(),
        })
    }

    fn build_pipeline2(&self, builder: &mut PipelineBuilder) -> Result<()> {
        self.input.build_pipeline(builder)?;

        // SecureFilter uses the same transform as regular Filter for execution
        // The security aspect is handled at plan level (stats suppression) and binding level
        builder.main_pipeline.add_transform(
            builder.filter_transform_builder(&self.predicates, self.projections.clone())?,
        )
    }
}

impl PhysicalPlanBuilder {
    pub async fn build_secure_filter(
        &mut self,
        s_expr: &SExpr,
        secure_filter: &databend_common_sql::plans::SecureFilter,
        mut required: ColumnSet,
        stat_info: PlanStatsInfo,
    ) -> Result<PhysicalPlan> {
        // 1. Prune unused Columns.
        let used = secure_filter
            .predicates
            .iter()
            .fold(required.clone(), |acc, v| {
                acc.union(&v.used_columns()).cloned().collect()
            });

        // 2. Build physical plan.
        let input = self.build(s_expr.child(0)?, used).await?;
        required = required
            .union(self.metadata.read().get_retained_column())
            .cloned()
            .collect();
        let column_projections = required.clone().into_iter().collect::<Vec<_>>();
        let input_schema = input.output_schema()?;
        let mut projections = ColumnSet::new();
        for column in column_projections.iter() {
            if let Some((index, _)) = input_schema.column_with_name(&column.to_string()) {
                projections.insert(index);
            }
        }

        Ok(PhysicalPlan::new(SecureFilter {
            meta: PhysicalPlanMeta::new("SecureFilter"),
            projections,
            input,
            predicates: secure_filter
                .predicates
                .iter()
                .map(|scalar| {
                    let expr = scalar
                        .type_check(input_schema.as_ref())?
                        .project_column_ref(|index| input_schema.index_of(&index.to_string()))?;
                    let expr = cast_expr_to_non_null_boolean(expr)?;
                    let (expr, _) = ConstantFolder::fold(&expr, &self.func_ctx, &BUILTIN_FUNCTIONS);
                    Ok(expr.as_remote_expr())
                })
                .collect::<Result<_>>()?,

            stat_info: Some(stat_info),
        }))
    }
}
