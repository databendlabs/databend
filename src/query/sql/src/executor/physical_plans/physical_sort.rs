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

use databend_common_ast::ast::FormatTreeNode;
use databend_common_catalog::plan::DataSourcePlan;
use databend_common_exception::Result;
use databend_common_expression::types::DataType;
use databend_common_expression::DataField;
use databend_common_expression::DataSchema;
use databend_common_expression::DataSchemaRef;
use databend_common_expression::DataSchemaRefExt;
use databend_common_pipeline_transforms::processors::sort::utils::ORDER_COL_NAME;
use itertools::Itertools;

use crate::executor::explain::PlanStatsInfo;
use crate::executor::format::format_output_columns;
use crate::executor::format::plan_stats_info_to_format_tree;
use crate::executor::format::FormatContext;
use crate::executor::physical_plan::DeriveHandle;
use crate::executor::physical_plans::common::SortDesc;
use crate::executor::physical_plans::WindowPartition;
use crate::executor::physical_plans::WindowPartitionTopN;
use crate::executor::physical_plans::WindowPartitionTopNFunc;
use crate::executor::IPhysicalPlan;
use crate::executor::PhysicalPlan;
use crate::executor::PhysicalPlanBuilder;
use crate::executor::PhysicalPlanMeta;
use crate::optimizer::ir::SExpr;
use crate::plans::WindowFuncType;
use crate::ColumnSet;
use crate::IndexType;

#[derive(Clone, Debug, serde::Serialize, serde::Deserialize)]
pub struct Sort {
    pub meta: PhysicalPlanMeta,
    pub input: Box<dyn IPhysicalPlan>,
    pub order_by: Vec<SortDesc>,
    /// limit = Limit.limit + Limit.offset
    pub limit: Option<usize>,
    /// If the sort plan is after the exchange plan.
    /// It's [None] if the sorting plan is in single node mode.
    pub after_exchange: Option<bool>,
    pub pre_projection: Option<Vec<IndexType>>,

    // Only used for explain
    pub stat_info: Option<PlanStatsInfo>,
}

#[typetag::serde]
impl IPhysicalPlan for Sort {
    fn get_meta(&self) -> &PhysicalPlanMeta {
        &self.meta
    }

    fn get_meta_mut(&mut self) -> &mut PhysicalPlanMeta {
        &mut self.meta
    }

    fn output_schema(&self) -> Result<DataSchemaRef> {
        let input_schema = self.input.output_schema()?;
        let mut fields = input_schema.fields().clone();
        if matches!(self.after_exchange, Some(true)) {
            // If the plan is after exchange plan in cluster mode,
            // the order column is at the last of the input schema.
            debug_assert_eq!(fields.last().unwrap().name(), ORDER_COL_NAME);
            debug_assert_eq!(
                fields.last().unwrap().data_type(),
                &self.order_col_type(&input_schema)?
            );
            fields.pop();
        } else {
            if let Some(proj) = &self.pre_projection {
                let fileted_fields = proj
                    .iter()
                    .filter_map(|index| input_schema.field_with_name(&index.to_string()).ok())
                    .cloned()
                    .collect::<Vec<_>>();
                if fileted_fields.len() < fields.len() {
                    // Only if the projection is not a full projection, we need to add a projection transform.
                    fields = fileted_fields
                }
            }

            if matches!(self.after_exchange, Some(false)) {
                // If the plan is before exchange plan in cluster mode,
                // the order column should be added to the output schema.
                fields.push(DataField::new(
                    ORDER_COL_NAME,
                    self.order_col_type(&input_schema)?,
                ));
            }
        }

        Ok(DataSchemaRefExt::create(fields))
    }

    fn children<'a>(&'a self) -> Box<dyn Iterator<Item = &'a Box<dyn IPhysicalPlan>> + 'a> {
        Box::new(std::iter::once(&self.input))
    }

    fn children_mut<'a>(
        &'a mut self,
    ) -> Box<dyn Iterator<Item = &'a mut Box<dyn IPhysicalPlan>> + 'a> {
        Box::new(std::iter::once(&mut self.input))
    }

    fn to_format_node(
        &self,
        ctx: &mut FormatContext<'_>,
        children: Vec<FormatTreeNode<String>>,
    ) -> Result<FormatTreeNode<String>> {
        let sort_keys = self
            .order_by
            .iter()
            .map(|sort_key| {
                Ok(format!(
                    "{} {} {}",
                    sort_key.display_name,
                    if sort_key.asc { "ASC" } else { "DESC" },
                    if sort_key.nulls_first {
                        "NULLS FIRST"
                    } else {
                        "NULLS LAST"
                    }
                ))
            })
            .collect::<Result<Vec<_>>>()?
            .join(", ");

        let mut node_children = vec![
            FormatTreeNode::new(format!(
                "output columns: [{}]",
                format_output_columns(self.output_schema()?, &ctx.metadata, true)
            )),
            FormatTreeNode::new(format!("sort keys: [{sort_keys}]")),
        ];

        if let Some(info) = &self.stat_info {
            node_children.extend(plan_stats_info_to_format_tree(info));
        }

        node_children.extend(children);
        Ok(FormatTreeNode::with_children(
            "Sort".to_string(),
            node_children,
        ))
    }

    #[recursive::recursive]
    fn try_find_single_data_source(&self) -> Option<&DataSourcePlan> {
        self.input.try_find_single_data_source()
    }

    fn get_desc(&self) -> Result<String> {
        Ok(self
            .order_by
            .iter()
            .map(|x| {
                format!(
                    "{}{}{}",
                    x.display_name,
                    if x.asc { "" } else { " DESC" },
                    if x.nulls_first { " NULLS FIRST" } else { "" },
                )
            })
            .join(", "))
    }

    fn derive(&self, mut children: Vec<Box<dyn IPhysicalPlan>>) -> Box<dyn IPhysicalPlan> {
        let mut new_physical_plan = self.clone();
        assert_eq!(children.len(), 1);
        new_physical_plan.input = children.pop().unwrap();
        Box::new(new_physical_plan)
    }
}

impl Sort {
    fn order_col_type(&self, schema: &DataSchema) -> Result<DataType> {
        if self.order_by.len() == 1 {
            let order_by_field = schema.field_with_name(&self.order_by[0].order_by.to_string())?;
            if matches!(
                order_by_field.data_type(),
                DataType::Number(_) | DataType::Date | DataType::Timestamp | DataType::String
            ) {
                return Ok(order_by_field.data_type().clone());
            }
        }
        Ok(DataType::Binary)
    }
}

impl PhysicalPlanBuilder {
    pub(crate) async fn build_sort(
        &mut self,
        s_expr: &SExpr,
        sort: &crate::plans::Sort,
        mut required: ColumnSet,
        stat_info: PlanStatsInfo,
    ) -> Result<Box<dyn IPhysicalPlan>> {
        // 1. Prune unused Columns.
        sort.items.iter().for_each(|s| {
            required.insert(s.index);
        });

        // If the query will be optimized by lazy reading, we don't need to do pre-projection.
        let pre_projection: Option<Vec<usize>> = if self.metadata.read().lazy_columns().is_empty() {
            sort.pre_projection.clone()
        } else {
            None
        };
        let input_plan = self.build(s_expr.child(0)?, required).await?;

        let order_by = sort
            .items
            .iter()
            .map(|v| SortDesc {
                asc: v.asc,
                nulls_first: v.nulls_first,
                order_by: v.index,
                display_name: self.metadata.read().column(v.index).name(),
            })
            .collect::<Vec<_>>();

        // Add WindowPartition for parallel sort in window.
        if let Some(window) = &sort.window_partition {
            let window_partition = window
                .partition_by
                .iter()
                .map(|v| v.index)
                .collect::<Vec<_>>();

            return Ok(Box::new(WindowPartition {
                meta: PhysicalPlanMeta::new("WindowPartition"),
                input: input_plan.clone(),
                partition_by: window_partition.clone(),
                order_by: order_by.clone(),
                after_exchange: sort.after_exchange,
                top_n: window.top.map(|top| WindowPartitionTopN {
                    func: match window.func {
                        WindowFuncType::RowNumber => WindowPartitionTopNFunc::RowNumber,
                        WindowFuncType::Rank => WindowPartitionTopNFunc::Rank,
                        WindowFuncType::DenseRank => WindowPartitionTopNFunc::DenseRank,
                        _ => unreachable!(),
                    },
                    top,
                }),
                stat_info: Some(stat_info.clone()),
            }));
        };

        // 2. Build physical plan.
        Ok(Box::new(Sort {
            order_by,
            pre_projection,
            input: input_plan,
            limit: sort.limit,
            after_exchange: sort.after_exchange,
            stat_info: Some(stat_info),
            meta: PhysicalPlanMeta::new("Sort"),
        }))
    }
}
