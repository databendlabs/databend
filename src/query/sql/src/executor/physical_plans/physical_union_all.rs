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
use databend_common_expression::DataField;
use databend_common_expression::DataSchema;
use databend_common_expression::DataSchemaRef;
use databend_common_expression::DataSchemaRefExt;
use databend_common_expression::RemoteExpr;

use crate::executor::explain::PlanStatsInfo;
use crate::executor::PhysicalPlan;
use crate::executor::PhysicalPlanBuilder;
use crate::optimizer::ir::SExpr;
use crate::ColumnSet;
use crate::IndexType;
use crate::ScalarExpr;
use crate::TypeCheck;

#[derive(Clone, Debug, serde::Serialize, serde::Deserialize)]
pub struct UnionAll {
    // A unique id of operator in a `PhysicalPlan` tree, only used for display.
    pub plan_id: u32,
    pub left: Box<PhysicalPlan>,
    pub right: Box<PhysicalPlan>,
    pub left_outputs: Vec<(IndexType, Option<RemoteExpr>)>,
    pub right_outputs: Vec<(IndexType, Option<RemoteExpr>)>,
    pub schema: DataSchemaRef,
    pub cte_scan_names: Vec<String>,

    // Only used for explain
    pub stat_info: Option<PlanStatsInfo>,
}

impl UnionAll {
    pub fn output_schema(&self) -> Result<DataSchemaRef> {
        Ok(self.schema.clone())
    }
}

impl PhysicalPlanBuilder {
    pub(crate) async fn build_union_all(
        &mut self,
        s_expr: &SExpr,
        union_all: &crate::plans::UnionAll,
        mut required: ColumnSet,
        stat_info: PlanStatsInfo,
    ) -> Result<PhysicalPlan> {
        // 1. Prune unused Columns.
        let metadata = self.metadata.read().clone();
        let lazy_columns = metadata.lazy_columns();
        required.extend(lazy_columns);

        // Use left's output columns as the offset indices
        // if the union has a CTE, the output columns are not filtered
        // otherwise, if the output columns of the union do not contain the columns used by the plan in the union, the expression will fail to obtain data.
        let (offset_indices, left_required, right_required) =
            if !union_all.cte_scan_names.is_empty() {
                let left: ColumnSet = union_all
                    .left_outputs
                    .iter()
                    .map(|(index, _)| *index)
                    .collect();
                let right: ColumnSet = union_all
                    .right_outputs
                    .iter()
                    .map(|(index, _)| *index)
                    .collect();

                let offset_indices: Vec<usize> = (0..union_all.left_outputs.len()).collect();
                (offset_indices, left, right)
            } else {
                let offset_indices: Vec<usize> = (0..union_all.left_outputs.len())
                    .filter(|index| required.contains(&union_all.output_indexes[*index]))
                    .collect();

                if offset_indices.is_empty() {
                    (
                        vec![0],
                        ColumnSet::from([union_all.left_outputs[0].0]),
                        ColumnSet::from([union_all.right_outputs[0].0]),
                    )
                } else {
                    offset_indices.iter().fold(
                        (vec![], ColumnSet::default(), ColumnSet::default()),
                        |(mut offset_indices, mut left, mut right), &index| {
                            left.insert(union_all.left_outputs[index].0);
                            right.insert(union_all.right_outputs[index].0);
                            offset_indices.push(index);
                            (offset_indices, left, right)
                        },
                    )
                }
            };

        // 2. Build physical plan.
        let left_plan = self.build(s_expr.child(0)?, left_required.clone()).await?;
        let right_plan = self.build(s_expr.child(1)?, right_required.clone()).await?;

        let left_schema = left_plan.output_schema()?;
        let right_schema = right_plan.output_schema()?;

        let left_outputs = process_outputs(&union_all.left_outputs, &offset_indices, &left_schema)?;
        let right_outputs =
            process_outputs(&union_all.right_outputs, &offset_indices, &right_schema)?;

        let mut fields = Vec::with_capacity(offset_indices.len());
        for offset in offset_indices {
            let index = union_all.output_indexes[offset];
            let data_type = if let Some(scalar_expr) = &union_all.left_outputs[offset].1 {
                let expr = scalar_expr
                    .type_check(left_schema.as_ref())?
                    .project_column_ref(|idx| left_schema.index_of(&idx.to_string()).unwrap());
                expr.data_type().clone()
            } else {
                let col_index = union_all.left_outputs[offset].0;
                left_schema
                    .field_with_name(&col_index.to_string())?
                    .data_type()
                    .clone()
            };

            fields.push(DataField::new(&index.to_string(), data_type));
        }

        Ok(PhysicalPlan::UnionAll(UnionAll {
            plan_id: 0,
            left: Box::new(left_plan),
            right: Box::new(right_plan),
            left_outputs,
            right_outputs,
            schema: DataSchemaRefExt::create(fields),

            cte_scan_names: union_all.cte_scan_names.clone(),
            stat_info: Some(stat_info),
        }))
    }
}

fn process_outputs(
    outputs: &[(IndexType, Option<ScalarExpr>)],
    offset_indices: &[usize],
    schema: &DataSchema,
) -> Result<Vec<(IndexType, Option<RemoteExpr>)>> {
    let mut results = Vec::with_capacity(offset_indices.len());
    for index in offset_indices {
        let output = &outputs[*index];
        if let Some(scalar_expr) = &output.1 {
            let expr = scalar_expr
                .type_check(schema)?
                .project_column_ref(|idx| schema.index_of(&idx.to_string()).unwrap());
            results.push((output.0, Some(expr.as_remote_expr())));
        } else {
            results.push((output.0, None));
        }
    }
    Ok(results)
}
