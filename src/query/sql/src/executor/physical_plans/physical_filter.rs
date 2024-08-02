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

use databend_common_exception::Result;
use databend_common_expression::ConstantFolder;
use databend_common_expression::DataSchemaRef;
use databend_common_expression::DataSchemaRefExt;
use databend_common_expression::RemoteExpr;
use databend_common_functions::BUILTIN_FUNCTIONS;

use crate::executor::cast_expr_to_non_null_boolean;
use crate::executor::explain::PlanStatsInfo;
use crate::executor::PhysicalPlan;
use crate::executor::PhysicalPlanBuilder;
use crate::optimizer::ColumnSet;
use crate::optimizer::SExpr;
use crate::TypeCheck;

#[derive(Clone, Debug, serde::Serialize, serde::Deserialize)]
pub struct Filter {
    // A unique id of operator in a `PhysicalPlan` tree, only used for display.
    pub plan_id: u32,
    pub projections: ColumnSet,
    pub input: Box<PhysicalPlan>,
    // Assumption: expression's data type must be `DataType::Boolean`.
    pub predicates: Vec<RemoteExpr>,

    // Only used for explain
    pub stat_info: Option<PlanStatsInfo>,
}

impl Filter {
    pub fn output_schema(&self) -> Result<DataSchemaRef> {
        let input_schema = self.input.output_schema()?;
        let mut fields = Vec::with_capacity(self.projections.len());
        for (i, field) in input_schema.fields().iter().enumerate() {
            if self.projections.contains(&i) {
                fields.push(field.clone());
            }
        }
        Ok(DataSchemaRefExt::create(fields))
    }
}

impl PhysicalPlanBuilder {
    pub(crate) async fn build_filter(
        &mut self,
        s_expr: &SExpr,
        filter: &crate::plans::Filter,
        mut required: ColumnSet,
        stat_info: PlanStatsInfo,
    ) -> Result<PhysicalPlan> {
        // 1. Prune unused Columns.
        let used = filter.predicates.iter().fold(required.clone(), |acc, v| {
            acc.union(&v.used_columns()).cloned().collect()
        });

        // 2. Build physical plan.
        let input = Box::new(self.build(s_expr.child(0)?, used).await?);
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

        Ok(PhysicalPlan::Filter(Filter {
            plan_id: 0,
            projections,
            input,
            predicates: filter
                .predicates
                .iter()
                .map(|scalar| {
                    let expr = scalar
                        .type_check(input_schema.as_ref())?
                        .project_column_ref(|index| {
                            input_schema.index_of(&index.to_string()).unwrap()
                        });
                    let expr = cast_expr_to_non_null_boolean(expr)?;
                    let (expr, _) = ConstantFolder::fold(&expr, &self.func_ctx, &BUILTIN_FUNCTIONS);
                    Ok(expr.as_remote_expr())
                })
                .collect::<Result<_>>()?,

            stat_info: Some(stat_info),
        }))
    }
}
