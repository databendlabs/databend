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

use common_exception::Result;
use common_expression::ConstantFolder;
use common_expression::DataField;
use common_expression::DataSchemaRef;
use common_expression::DataSchemaRefExt;
use common_expression::RemoteExpr;
use common_functions::BUILTIN_FUNCTIONS;

use crate::executor::explain::PlanStatsInfo;
use crate::executor::physical_plan::PhysicalPlan;
use crate::executor::physical_plan_builder::PhysicalPlanBuilder;
use crate::optimizer::ColumnSet;
use crate::optimizer::SExpr;
use crate::IndexType;
use crate::TypeCheck;

#[derive(Clone, Debug, serde::Serialize, serde::Deserialize)]
pub struct EvalScalar {
    // A unique id of operator in a `PhysicalPlan` tree, only used for display.
    pub plan_id: u32,
    pub projections: ColumnSet,
    pub input: Box<PhysicalPlan>,
    pub exprs: Vec<(RemoteExpr, IndexType)>,

    /// Only used for explain
    pub stat_info: Option<PlanStatsInfo>,
}

impl EvalScalar {
    pub fn output_schema(&self) -> Result<DataSchemaRef> {
        if self.exprs.is_empty() {
            return self.input.output_schema();
        }
        let input_schema = self.input.output_schema()?;
        let mut fields = Vec::with_capacity(self.projections.len());
        for (i, field) in input_schema.fields().iter().enumerate() {
            if self.projections.contains(&i) {
                fields.push(field.clone());
            }
        }
        let input_column_nums = input_schema.num_fields();
        for (i, (expr, index)) in self.exprs.iter().enumerate() {
            let i = i + input_column_nums;
            if !self.projections.contains(&i) {
                continue;
            }
            let name = index.to_string();
            let data_type = expr.as_expr(&BUILTIN_FUNCTIONS).data_type().clone();
            fields.push(DataField::new(&name, data_type));
        }
        Ok(DataSchemaRefExt::create(fields))
    }
}

impl PhysicalPlanBuilder {
    pub(crate) async fn build_eval_scalar(
        &mut self,
        s_expr: &SExpr,
        eval_scalar: &crate::plans::EvalScalar,
        mut required: ColumnSet,
        stat_info: PlanStatsInfo,
    ) -> Result<PhysicalPlan> {
        // 1. Prune unused Columns.
        let column_projections = required.clone().into_iter().collect::<Vec<_>>();
        let mut used = vec![];
        // Only keep columns needed by parent plan.
        for s in eval_scalar.items.iter() {
            if !required.contains(&s.index) {
                continue;
            }
            used.push(s.clone());
            s.scalar.used_columns().iter().for_each(|c| {
                required.insert(*c);
            })
        }
        // 2. Build physical plan.
        if used.is_empty() {
            self.build(s_expr.child(0)?, required).await
        } else {
            let input = self.build(s_expr.child(0)?, required).await?;
            let eval_scalar = crate::plans::EvalScalar { items: used };
            self.create_eval_scalar(&eval_scalar, column_projections, input, stat_info)
        }
    }

    pub(crate) fn create_eval_scalar(
        &mut self,
        eval_scalar: &crate::plans::EvalScalar,
        column_projections: Vec<IndexType>,
        input: PhysicalPlan,
        stat_info: PlanStatsInfo,
    ) -> Result<PhysicalPlan> {
        let input_schema = input.output_schema()?;

        let exprs = eval_scalar
            .items
            .iter()
            .map(|item| {
                let expr = item
                    .scalar
                    .type_check(input_schema.as_ref())?
                    .project_column_ref(|index| input_schema.index_of(&index.to_string()).unwrap());
                let (expr, _) = ConstantFolder::fold(&expr, &self.func_ctx, &BUILTIN_FUNCTIONS);
                Ok((expr.as_remote_expr(), item.index))
            })
            .collect::<Result<Vec<_>>>()?;

        let exprs = exprs
            .into_iter()
            .filter(|(scalar, idx)| {
                if let RemoteExpr::ColumnRef { id, .. } = scalar {
                    return idx.to_string() != input_schema.field(*id).name().as_str();
                }
                true
            })
            .collect::<Vec<_>>();

        let mut projections = ColumnSet::new();
        for column in column_projections.iter() {
            if let Ok(index) = input_schema.index_of(&column.to_string()) {
                projections.insert(index);
            }
        }
        let input_column_nums = input_schema.num_fields();
        for (index, (_, idx)) in exprs.iter().enumerate() {
            if column_projections.contains(idx) {
                projections.insert(index + input_column_nums);
            }
        }
        Ok(PhysicalPlan::EvalScalar(EvalScalar {
            plan_id: self.next_plan_id(),
            projections,
            input: Box::new(input),
            exprs,
            stat_info: Some(stat_info),
        }))
    }
}
