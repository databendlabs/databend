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
use std::assert_matches::debug_assert_matches;
use std::fmt::Display;

use databend_common_catalog::plan::DataSourcePlan;
use databend_common_exception::Result;
use databend_common_expression::DataField;
use databend_common_expression::DataSchema;
use databend_common_expression::DataSchemaRef;
use databend_common_expression::DataSchemaRefExt;
use databend_common_expression::SortColumnDescription;
use databend_common_pipeline_transforms::TransformPipelineHelper;
use databend_common_pipeline_transforms::blocks::CompoundBlockOperator;
use databend_common_pipeline_transforms::sorts::core::order_field_type;
use databend_common_pipeline_transforms::sorts::utils::ORDER_COL_NAME;
use databend_common_sql::ColumnSet;
use databend_common_sql::IndexType;
use databend_common_sql::evaluator::BlockOperator;
use databend_common_sql::executor::physical_plans::FragmentKind;
use databend_common_sql::executor::physical_plans::SortDesc;
use databend_common_sql::optimizer::ir::SExpr;
use databend_common_sql::plans::WindowFuncType;
use itertools::Itertools;

use crate::physical_plans::Exchange;
use crate::physical_plans::PhysicalPlanBuilder;
use crate::physical_plans::PhysicalPlanCast;
use crate::physical_plans::WindowPartition;
use crate::physical_plans::WindowPartitionTopN;
use crate::physical_plans::WindowPartitionTopNFunc;
use crate::physical_plans::explain::PlanStatsInfo;
use crate::physical_plans::format::PhysicalFormat;
use crate::physical_plans::format::SortFormatter;
use crate::physical_plans::physical_plan::IPhysicalPlan;
use crate::physical_plans::physical_plan::PhysicalPlan;
use crate::physical_plans::physical_plan::PhysicalPlanMeta;
use crate::pipelines::PipelineBuilder;
use crate::pipelines::builders::SortPipelineBuilder;
use crate::spillers::SortSpiller;

type TransformSortBuilder =
    crate::pipelines::processors::transforms::TransformSortBuilder<SortSpiller>;

#[derive(Clone, Debug, serde::Serialize, serde::Deserialize)]
pub struct Sort {
    pub meta: PhysicalPlanMeta,
    pub input: PhysicalPlan,
    pub order_by: Vec<SortDesc>,
    /// limit = Limit.limit + Limit.offset
    pub limit: Option<usize>,
    pub step: SortStep,
    pub pre_projection: Option<Vec<IndexType>>,
    pub broadcast_id: Option<u32>,
    pub enable_fixed_rows: bool,

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

#[typetag::serde]
impl IPhysicalPlan for Sort {
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
        match self.step {
            SortStep::Final | SortStep::Shuffled => {
                let mut fields = input_schema.fields().clone();
                // If the plan is after exchange plan in cluster mode,
                // the order column is at the last of the input schema.
                debug_assert_eq!(fields.last().unwrap().name(), ORDER_COL_NAME);
                debug_assert_eq!(
                    fields.last().unwrap().data_type(),
                    &order_field_type(
                        &input_schema,
                        &self.sort_desc(&input_schema)?,
                        self.enable_fixed_rows
                    ),
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
                        order_field_type(
                            &input_schema,
                            &self.sort_desc(&input_schema)?,
                            self.enable_fixed_rows,
                        ),
                    ));
                }
                Ok(DataSchemaRefExt::create(fields))
            }
            SortStep::Route => Ok(input_schema),
        }
    }

    fn children<'a>(&'a self) -> Box<dyn Iterator<Item = &'a PhysicalPlan> + 'a> {
        Box::new(std::iter::once(&self.input))
    }

    fn children_mut<'a>(&'a mut self) -> Box<dyn Iterator<Item = &'a mut PhysicalPlan> + 'a> {
        Box::new(std::iter::once(&mut self.input))
    }

    fn formatter(&self) -> Result<Box<dyn PhysicalFormat + '_>> {
        Ok(SortFormatter::create(self))
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

    fn derive(&self, mut children: Vec<PhysicalPlan>) -> PhysicalPlan {
        assert_eq!(children.len(), 1);
        let input = children.pop().unwrap();
        PhysicalPlan::new(Sort {
            meta: self.meta.clone(),
            input,
            order_by: self.order_by.clone(),
            limit: self.limit,
            step: self.step,
            pre_projection: self.pre_projection.clone(),
            broadcast_id: self.broadcast_id,
            enable_fixed_rows: self.enable_fixed_rows,
            stat_info: self.stat_info.clone(),
        })
    }

    fn build_pipeline2(&self, builder: &mut PipelineBuilder) -> Result<()> {
        let output_schema = self.output_schema()?;
        let sort_desc = self
            .order_by
            .iter()
            .map(|desc| {
                let offset = output_schema.index_of(&desc.order_by.to_string())?;
                Ok(SortColumnDescription {
                    offset,
                    asc: desc.asc,
                    nulls_first: desc.nulls_first,
                })
            })
            .collect::<Result<Vec<_>>>()?;
        let sort_desc = sort_desc.into();

        if self.step != SortStep::Shuffled {
            self.input.build_pipeline(builder)?;
        }

        if let Some(proj) = &self.pre_projection {
            debug_assert_matches!(
                self.step,
                SortStep::Single | SortStep::Partial | SortStep::Sample
            );

            let input_schema = self.input.output_schema()?;
            // Do projection to reduce useless data copying during sorting.
            let projection = proj
                .iter()
                .map(|i| input_schema.index_of(&i.to_string()).unwrap())
                .collect::<Vec<_>>();

            builder.main_pipeline.add_transformer(|| {
                CompoundBlockOperator::new(
                    vec![BlockOperator::Project {
                        projection: projection.clone(),
                    }],
                    builder.func_ctx.clone(),
                    input_schema.num_fields(),
                )
            });
        }

        let sort_builder = SortPipelineBuilder::create(
            builder.ctx.clone(),
            output_schema,
            sort_desc,
            self.broadcast_id,
            self.enable_fixed_rows,
        )?
        .with_limit(self.limit);

        let max_threads = builder.settings.get_max_threads()? as usize;
        match self.step {
            SortStep::Single => {
                // Build for single node mode.
                // We build the full sort pipeline for it.
                if builder.main_pipeline.output_len() == 1 || max_threads == 1 {
                    builder.main_pipeline.try_resize(max_threads)?;
                }
                sort_builder
                    .remove_order_col_at_last()
                    .build_full_sort_pipeline(&mut builder.main_pipeline)
            }

            SortStep::Partial => {
                // Build for each cluster node.
                // We build the full sort pipeline for it.
                // Don't remove the order column at last.
                if builder.main_pipeline.output_len() == 1 || max_threads == 1 {
                    builder.main_pipeline.try_resize(max_threads)?;
                }
                sort_builder.build_full_sort_pipeline(&mut builder.main_pipeline)
            }
            SortStep::Final => {
                // TODO(Winter): the query will hang in MultiSortMergeProcessor when max_threads == 1 and output_len != 1
                if max_threads == 1 && builder.main_pipeline.output_len() > 1 {
                    builder.main_pipeline.try_resize(1)?;
                    return sort_builder
                        .remove_order_col_at_last()
                        .build_merge_sort_pipeline(&mut builder.main_pipeline, true);
                }

                // Build for the coordinator node.
                // We only build a `MultiSortMergeTransform`,
                // as the data is already sorted in each cluster node.
                // The input number of the transform is equal to the number of cluster nodes.
                sort_builder
                    .remove_order_col_at_last()
                    .build_multi_merge(&mut builder.main_pipeline)
            }

            SortStep::Sample => {
                if builder.main_pipeline.output_len() == 1 || max_threads == 1 {
                    builder.main_pipeline.try_resize(max_threads)?;
                }
                sort_builder.build_sample(&mut builder.main_pipeline)?;
                builder.exchange_injector = TransformSortBuilder::exchange_injector();
                Ok(())
            }
            SortStep::Shuffled => {
                if Exchange::check_physical_plan(&self.input) {
                    let exchange = TransformSortBuilder::exchange_injector();
                    let old_inject = std::mem::replace(&mut builder.exchange_injector, exchange);
                    self.input.build_pipeline(builder)?;
                    builder.exchange_injector = old_inject;
                } else {
                    self.input.build_pipeline(builder)?;
                }

                if builder.main_pipeline.output_len() == 1 {
                    return Ok(());
                }

                if max_threads == 1 {
                    // TODO(Winter): the query will hang in MultiSortMergeProcessor when max_threads == 1 and output_len != 1
                    unimplemented!();
                }
                sort_builder
                    .remove_order_col_at_last()
                    .build_bounded_merge_sort(&mut builder.main_pipeline)
            }
            SortStep::Route => {
                if builder.main_pipeline.output_len() == 1 {
                    builder
                        .main_pipeline
                        .add_transformer(TransformSortBuilder::build_dummy_route);
                    Ok(())
                } else {
                    TransformSortBuilder::add_route(&mut builder.main_pipeline)
                }
            }
        }
    }
}

impl Sort {
    fn sort_desc(&self, schema: &DataSchema) -> Result<Vec<SortColumnDescription>> {
        self.order_by
            .iter()
            .map(|desc| {
                Ok(SortColumnDescription {
                    offset: schema.index_of(&desc.order_by.to_string())?,
                    asc: desc.asc,
                    nulls_first: desc.nulls_first,
                })
            })
            .collect()
    }
}

impl PhysicalPlanBuilder {
    pub async fn build_sort(
        &mut self,
        s_expr: &SExpr,
        sort: &databend_common_sql::plans::Sort,
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

            return Ok(PhysicalPlan::new(WindowPartition {
                meta: PhysicalPlanMeta::new("WindowPartition"),
                input: input_plan,
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
        let settings = self.ctx.get_settings();
        let enable_fixed_rows = settings.get_enable_fixed_rows_sort()?;

        let Some(after_exchange) = sort.after_exchange else {
            let input_plan = self.build(s_expr.unary_child(), required).await?;
            return Ok(PhysicalPlan::new(Sort {
                input: input_plan,
                order_by,
                limit: sort.limit,
                step: SortStep::Single,
                pre_projection,
                broadcast_id: None,
                enable_fixed_rows,
                stat_info: Some(stat_info),
                meta: PhysicalPlanMeta::new("Sort"),
            }));
        };

        if !settings.get_enable_shuffle_sort()? || settings.get_max_threads()? == 1 {
            let input_plan = self.build(s_expr.unary_child(), required).await?;
            return if !after_exchange {
                Ok(PhysicalPlan::new(Sort {
                    input: input_plan,
                    order_by,
                    limit: sort.limit,
                    step: SortStep::Partial,
                    pre_projection,
                    broadcast_id: None,
                    enable_fixed_rows,
                    stat_info: Some(stat_info),
                    meta: PhysicalPlanMeta::new("Sort"),
                }))
            } else {
                Ok(PhysicalPlan::new(Sort {
                    input: input_plan,
                    order_by,
                    limit: sort.limit,
                    step: SortStep::Final,
                    pre_projection: None,
                    broadcast_id: None,
                    enable_fixed_rows,
                    stat_info: Some(stat_info),
                    meta: PhysicalPlanMeta::new("Sort"),
                }))
            };
        }

        if after_exchange {
            let input_plan = self.build(s_expr.unary_child(), required).await?;
            return Ok(PhysicalPlan::new(Sort {
                input: input_plan,
                order_by,
                limit: sort.limit,
                step: SortStep::Route,
                pre_projection: None,
                broadcast_id: None,
                enable_fixed_rows,
                stat_info: Some(stat_info),
                meta: PhysicalPlanMeta::new("Sort"),
            }));
        }

        let input_plan = self.build(s_expr.unary_child(), required).await?;
        let sample = PhysicalPlan::new(Sort {
            input: input_plan,
            order_by: order_by.clone(),
            limit: sort.limit,
            step: SortStep::Sample,
            pre_projection,
            broadcast_id: Some(self.ctx.get_next_broadcast_id()),
            enable_fixed_rows,
            stat_info: Some(stat_info.clone()),
            meta: PhysicalPlanMeta::new("Sort"),
        });
        let exchange = PhysicalPlan::new(Exchange {
            input: sample,
            kind: FragmentKind::Normal,
            keys: vec![],
            ignore_exchange: false,
            allow_adjust_parallelism: false,
            meta: PhysicalPlanMeta::new("Exchange"),
        });

        Ok(PhysicalPlan::new(Sort {
            input: exchange,
            order_by,
            limit: sort.limit,
            step: SortStep::Shuffled,
            pre_projection: None,
            broadcast_id: None,
            enable_fixed_rows,
            stat_info: Some(stat_info),
            meta: PhysicalPlanMeta::new("Sort"),
        }))
    }
}
