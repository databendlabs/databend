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

use std::fmt::Display;

use databend_common_exception::Result;
use databend_common_expression::types::DataType;
use databend_common_expression::DataField;
use databend_common_expression::DataSchema;
use databend_common_expression::DataSchemaRef;
use databend_common_expression::DataSchemaRefExt;
use databend_common_pipeline_transforms::processors::sort::utils::ORDER_COL_NAME;

use crate::executor::explain::PlanStatsInfo;
use crate::executor::physical_plans::common::SortDesc;
use crate::executor::physical_plans::Exchange;
use crate::executor::physical_plans::FragmentKind;
use crate::executor::physical_plans::WindowPartition;
use crate::executor::physical_plans::WindowPartitionTopN;
use crate::executor::physical_plans::WindowPartitionTopNFunc;
use crate::executor::PhysicalPlan;
use crate::executor::PhysicalPlanBuilder;
use crate::optimizer::ir::SExpr;
use crate::plans::WindowFuncType;
use crate::ColumnSet;
use crate::IndexType;

#[derive(Clone, Debug, serde::Serialize, serde::Deserialize)]
pub struct Sort {
    /// A unique id of operator in a `PhysicalPlan` tree, only used for display.
    pub plan_id: u32,
    pub input: Box<PhysicalPlan>,
    pub order_by: Vec<SortDesc>,
    /// limit = Limit.limit + Limit.offset
    pub limit: Option<usize>,
    pub step: SortStep,
    pub pre_projection: Option<Vec<IndexType>>,
    pub broadcast_id: Option<u32>,

    // Only used for explain
    pub stat_info: Option<PlanStatsInfo>,
}

#[derive(Debug, Hash, Clone, Copy, serde::Serialize, serde::Deserialize, PartialEq, Eq)]
pub enum SortStep {
    // single node mode
    Single,

    // cluster mode
    Partial, // before the exchange plan
    Final,   // after the exchange plan

    // range shuffle mode
    Sample,
    Shuffled,
    Route,
}

impl Display for SortStep {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            SortStep::Single => write!(f, "Single"),
            SortStep::Partial => write!(f, "Partial"),
            SortStep::Final => write!(f, "Final"),
            SortStep::Sample => write!(f, "Sample"),
            SortStep::Shuffled => write!(f, "Shuffled"),
            SortStep::Route => write!(f, "Route"),
        }
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

    pub fn output_schema(&self) -> Result<DataSchemaRef> {
        let input_schema = self.input.output_schema()?;
        match self.step {
            SortStep::Final | SortStep::Shuffled => {
                let mut fields = input_schema.fields().clone();
                // If the plan is after exchange plan in cluster mode,
                // the order column is at the last of the input schema.
                debug_assert_eq!(fields.last().unwrap().name(), ORDER_COL_NAME);
                debug_assert_eq!(
                    fields.last().unwrap().data_type(),
                    &self.order_col_type(&input_schema)?
                );
                fields.pop();
                Ok(DataSchemaRefExt::create(fields))
            }
            SortStep::Single | SortStep::Partial | SortStep::Sample => {
                let mut fields = self
                    .pre_projection
                    .as_ref()
                    .and_then(|proj| {
                        let fileted_fields = proj
                            .iter()
                            .map(|index| {
                                input_schema
                                    .field_with_name(&index.to_string())
                                    .unwrap()
                                    .clone()
                            })
                            .collect::<Vec<_>>();

                        if fileted_fields.len() < input_schema.fields().len() {
                            // Only if the projection is not a full projection, we need to add a projection transform.
                            Some(fileted_fields)
                        } else {
                            None
                        }
                    })
                    .unwrap_or_else(|| input_schema.fields().clone());
                if self.step != SortStep::Single {
                    // If the plan is before exchange plan in cluster mode,
                    // the order column should be added to the output schema.
                    fields.push(DataField::new(
                        ORDER_COL_NAME,
                        self.order_col_type(&input_schema)?,
                    ));
                }
                Ok(DataSchemaRefExt::create(fields))
            }
            SortStep::Route => Ok(input_schema),
        }
    }
}

impl PhysicalPlanBuilder {
    pub(crate) async fn build_sort(
        &mut self,
        s_expr: &SExpr,
        sort: &crate::plans::Sort,
        mut required: ColumnSet,
        stat_info: PlanStatsInfo,
    ) -> Result<PhysicalPlan> {
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

            let sort_step = match sort.after_exchange {
                Some(false) => SortStep::Partial,
                Some(true) => SortStep::Final,
                None => SortStep::Single,
            };

            let input_plan = self.build(s_expr.unary_child(), required).await?;

            return Ok(PhysicalPlan::WindowPartition(WindowPartition {
                plan_id: 0,
                input: Box::new(input_plan),
                partition_by: window_partition.clone(),
                order_by: order_by.clone(),
                sort_step,
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
        let Some(after_exchange) = sort.after_exchange else {
            let input_plan = self.build(s_expr.unary_child(), required).await?;
            return Ok(PhysicalPlan::Sort(Sort {
                plan_id: 0,
                input: Box::new(input_plan),
                order_by,
                limit: sort.limit,
                step: SortStep::Single,
                pre_projection,
                broadcast_id: None,
                stat_info: Some(stat_info),
            }));
        };

        let settings = self.ctx.get_settings();
        if !settings.get_enable_shuffle_sort()? || settings.get_max_threads()? == 1 {
            let input_plan = self.build(s_expr.unary_child(), required).await?;
            return if !after_exchange {
                Ok(PhysicalPlan::Sort(Sort {
                    plan_id: 0,
                    input: Box::new(input_plan),
                    order_by,
                    limit: sort.limit,
                    step: SortStep::Partial,
                    pre_projection,
                    broadcast_id: None,
                    stat_info: Some(stat_info),
                }))
            } else {
                Ok(PhysicalPlan::Sort(Sort {
                    plan_id: 0,
                    input: Box::new(input_plan),
                    order_by,
                    limit: sort.limit,
                    step: SortStep::Final,
                    pre_projection: None,
                    broadcast_id: None,
                    stat_info: Some(stat_info),
                }))
            };
        }

        if after_exchange {
            let input_plan = self.build(s_expr.unary_child(), required).await?;
            return Ok(PhysicalPlan::Sort(Sort {
                plan_id: 0,
                input: Box::new(input_plan),
                order_by,
                limit: sort.limit,
                step: SortStep::Route,
                pre_projection: None,
                broadcast_id: None,
                stat_info: Some(stat_info),
            }));
        }

        let input_plan = self.build(s_expr.unary_child(), required).await?;
        let sample = PhysicalPlan::Sort(Sort {
            plan_id: 0,
            input: Box::new(input_plan),
            order_by: order_by.clone(),
            limit: sort.limit,
            step: SortStep::Sample,
            pre_projection,
            broadcast_id: Some(self.ctx.get_next_broadcast_id()),
            stat_info: Some(stat_info.clone()),
        });
        let exchange = PhysicalPlan::Exchange(Exchange {
            plan_id: 0,
            input: Box::new(sample),
            kind: FragmentKind::Normal,
            keys: vec![],
            ignore_exchange: false,
            allow_adjust_parallelism: false,
        });
        Ok(PhysicalPlan::Sort(Sort {
            plan_id: 0,
            input: Box::new(exchange),
            order_by,
            limit: sort.limit,
            step: SortStep::Shuffled,
            pre_projection: None,
            broadcast_id: None,
            stat_info: Some(stat_info),
        }))
    }
}
