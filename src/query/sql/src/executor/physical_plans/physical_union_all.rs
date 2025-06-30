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

use itertools::Itertools;
use databend_common_exception::Result;
use databend_common_expression::DataField;
use databend_common_expression::DataSchema;
use databend_common_expression::DataSchemaRef;
use databend_common_expression::DataSchemaRefExt;
use databend_common_expression::RemoteExpr;

use crate::executor::explain::PlanStatsInfo;
use crate::executor::{IPhysicalPlan, PhysicalPlan, PhysicalPlanMeta};
use crate::executor::PhysicalPlanBuilder;
use crate::optimizer::ir::SExpr;
use crate::ColumnSet;
use crate::IndexType;
use crate::ScalarExpr;
use crate::TypeCheck;

#[derive(Clone, Debug, serde::Serialize, serde::Deserialize)]
pub struct UnionAll {
    meta: PhysicalPlanMeta,
    pub left: Box<dyn IPhysicalPlan>,
    pub right: Box<dyn IPhysicalPlan>,
    pub left_outputs: Vec<(IndexType, Option<RemoteExpr>)>,
    pub right_outputs: Vec<(IndexType, Option<RemoteExpr>)>,
    pub schema: DataSchemaRef,
    pub cte_scan_names: Vec<String>,

    // Only used for explain
    pub stat_info: Option<PlanStatsInfo>,
}

#[typetag::serde]
impl IPhysicalPlan for UnionAll {
    fn get_meta(&self) -> &PhysicalPlanMeta {
        &self.meta
    }

    fn get_meta_mut(&mut self) -> &mut PhysicalPlanMeta {
        &mut self.meta
    }

    fn output_schema(&self) -> Result<DataSchemaRef> {
        Ok(self.schema.clone())
    }

    fn children<'a>(&'a self) -> Box<dyn Iterator<Item=&'a Box<dyn IPhysicalPlan>> + 'a> {
        Box::new(std::iter::once(&self.left).chain(std::iter::once(&self.right)))
    }

    fn children_mut<'a>(&'a mut self) -> Box<dyn Iterator<Item=&'a mut Box<dyn IPhysicalPlan>> + 'a> {
        Box::new(std::iter::once(&mut self.left).chain(std::iter::once(&mut self.right)))
    }

    fn get_desc(&self) -> Result<String> {
        Ok(self
            .left_outputs
            .iter()
            .zip(self.right_outputs.iter())
            .map(|(l, r)| format!("#{} <- #{}", l.0, r.0))
            .join(", "))
    }
}

impl PhysicalPlanBuilder {
    pub(crate) async fn build_union_all(
        &mut self,
        s_expr: &SExpr,
        union_all: &crate::plans::UnionAll,
        mut required: ColumnSet,
        stat_info: PlanStatsInfo,
    ) -> Result<Box<dyn IPhysicalPlan>> {
        // 1. Prune unused Columns.
        let metadata = self.metadata.read().clone();
        let lazy_columns = metadata.lazy_columns();
        required.extend(lazy_columns);

        // if the union has a CTE, the output columns are not filtered
        // otherwise, if the output columns of the union do not contain the columns used by the plan in the union, the expression will fail to obtain data.
        let (left_required, right_required) = if !union_all.cte_scan_names.is_empty() {
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

            (left, right)
        } else {
            let indices: Vec<usize> = (0..union_all.left_outputs.len())
                .filter(|index| required.contains(&union_all.output_indexes[*index]))
                .collect();
            if indices.is_empty() {
                (
                    ColumnSet::from([union_all.left_outputs[0].0]),
                    ColumnSet::from([union_all.right_outputs[0].0]),
                )
            } else {
                indices.iter().fold(
                    (ColumnSet::default(), ColumnSet::default()),
                    |(mut left, mut right), &index| {
                        left.insert(union_all.left_outputs[index].0);
                        right.insert(union_all.right_outputs[index].0);
                        (left, right)
                    },
                )
            }
        };

        // 2. Build physical plan.
        let left_plan = self.build(s_expr.child(0)?, left_required.clone()).await?;
        let right_plan = self.build(s_expr.child(1)?, right_required.clone()).await?;

        let left_schema = left_plan.output_schema()?;
        let right_schema = right_plan.output_schema()?;

        let fields = union_all
            .left_outputs
            .iter()
            .enumerate()
            .filter(|(_, (index, _))| left_required.contains(index))
            .map(|(i, (index, expr))| {
                let data_type = if let Some(expr) = expr {
                    expr.data_type()?
                } else {
                    left_schema
                        .field_with_name(&index.to_string())?
                        .data_type()
                        .clone()
                };
                let output_index = union_all.output_indexes[i];
                Ok(DataField::new(&output_index.to_string(), data_type))
            })
            .collect::<Result<Vec<_>>>()?;

        let left_outputs = process_outputs(&union_all.left_outputs, &left_required, &left_schema)?;
        let right_outputs =
            process_outputs(&union_all.right_outputs, &right_required, &right_schema)?;

        Ok(Box::new(UnionAll {
            left: left_plan,
            right: right_plan,
            left_outputs,
            right_outputs,
            schema: DataSchemaRefExt::create(fields),
            meta: PhysicalPlanMeta::new("UnionAll"),
            cte_scan_names: union_all.cte_scan_names.clone(),
            stat_info: Some(stat_info),
        }))
    }
}

fn process_outputs(
    outputs: &[(IndexType, Option<ScalarExpr>)],
    required: &ColumnSet,
    schema: &DataSchema,
) -> Result<Vec<(IndexType, Option<RemoteExpr>)>> {
    outputs
        .iter()
        .filter(|(index, _)| required.contains(index))
        .map(|(index, scalar_expr)| {
            if let Some(scalar_expr) = scalar_expr {
                let expr = scalar_expr
                    .type_check(schema)?
                    .project_column_ref(|idx| schema.index_of(&idx.to_string()).unwrap());
                Ok((*index, Some(expr.as_remote_expr())))
            } else {
                Ok((*index, None))
            }
        })
        .collect()
}
