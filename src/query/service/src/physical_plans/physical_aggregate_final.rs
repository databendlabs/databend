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
use std::sync::Arc;

use databend_common_catalog::table_context::TableContext;
use databend_common_exception::ErrorCode;
use databend_common_exception::Result;
use databend_common_expression::DataField;
use databend_common_expression::DataSchemaRef;
use databend_common_expression::DataSchemaRefExt;
use databend_common_expression::RemoteExpr;
use databend_common_pipeline::core::ProcessorPtr;
use databend_common_sql::ColumnSet;
use databend_common_sql::IndexType;
use databend_common_sql::ScalarExpr;
use databend_common_sql::executor::physical_plans::AggregateFunctionDesc;
use databend_common_sql::executor::physical_plans::AggregateFunctionSignature;
use databend_common_sql::executor::physical_plans::SortDesc;
use databend_common_sql::optimizer::ir::SExpr;
use databend_common_sql::plans::Aggregate;
use databend_common_sql::plans::AggregateMode;
use databend_common_sql::plans::ConstantTableScan;
use databend_common_sql::plans::ScalarItem;
use itertools::Itertools;

use super::AggregateExpand;
use super::AggregatePartial;
use super::Exchange;
use super::ExchangeSource;
use super::PhysicalPlanCast;
use crate::physical_plans::explain::PlanStatsInfo;
use crate::physical_plans::format::AggregateFinalFormatter;
use crate::physical_plans::format::PhysicalFormat;
use crate::physical_plans::physical_plan::IPhysicalPlan;
use crate::physical_plans::physical_plan::PhysicalPlan;
use crate::physical_plans::physical_plan::PhysicalPlanMeta;
use crate::physical_plans::physical_plan_builder::PhysicalPlanBuilder;
use crate::pipelines::PipelineBuilder;
use crate::pipelines::processors::transforms::aggregator::AggregateInjector;
use crate::pipelines::processors::transforms::aggregator::FinalSingleStateAggregator;
use crate::pipelines::processors::transforms::aggregator::build_partition_bucket;

#[derive(serde::Serialize, serde::Deserialize, Clone, Debug)]
pub struct AggregateFinal {
    meta: PhysicalPlanMeta,
    pub input: PhysicalPlan,
    pub group_by: Vec<IndexType>,
    pub agg_funcs: Vec<AggregateFunctionDesc>,
    pub before_group_by_schema: DataSchemaRef,
    pub group_by_display: Vec<String>,

    // Only used for explain
    pub stat_info: Option<PlanStatsInfo>,
    pub shuffle_mode: AggregateShuffleMode,
}

#[typetag::serde]
impl IPhysicalPlan for AggregateFinal {
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
        let mut fields = Vec::with_capacity(self.agg_funcs.len() + self.group_by.len());
        for agg in self.agg_funcs.iter() {
            let data_type = agg.sig.return_type.clone();
            fields.push(DataField::new(&agg.output_column.to_string(), data_type));
        }
        for id in self.group_by.iter() {
            let data_type = self
                .before_group_by_schema
                .field_with_name(&id.to_string())?
                .data_type()
                .clone();
            fields.push(DataField::new(&id.to_string(), data_type));
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
        Ok(AggregateFinalFormatter::create(self))
    }

    fn get_desc(&self) -> Result<String> {
        Ok(self.agg_funcs.iter().map(|x| x.display.clone()).join(", "))
    }

    fn get_labels(&self) -> Result<HashMap<String, Vec<String>>> {
        let mut labels = HashMap::with_capacity(2);
        if !self.group_by_display.is_empty() {
            labels.insert(String::from("Grouping keys"), self.group_by_display.clone());
        }

        if !self.agg_funcs.is_empty() {
            labels.insert(
                String::from("Aggregate Functions"),
                self.agg_funcs.iter().map(|x| x.display.clone()).collect(),
            );
        }

        Ok(labels)
    }

    fn derive(&self, mut children: Vec<PhysicalPlan>) -> PhysicalPlan {
        assert_eq!(children.len(), 1);

        PhysicalPlan::new(AggregateFinal {
            input: children.remove(0),
            meta: self.meta.clone(),
            group_by: self.group_by.clone(),
            agg_funcs: self.agg_funcs.clone(),
            before_group_by_schema: self.before_group_by_schema.clone(),
            group_by_display: self.group_by_display.clone(),
            stat_info: self.stat_info.clone(),
            shuffle_mode: self.shuffle_mode.clone(),
        })
    }

    fn build_pipeline2(&self, builder: &mut PipelineBuilder) -> Result<()> {
        let max_block_rows = builder.settings.get_max_block_size()? as usize;
        let max_block_bytes = builder.settings.get_max_block_bytes()? as usize;

        let max_spill_io_requests = builder.settings.get_max_spill_io_requests()?;
        let max_restore_worker = builder.settings.get_max_aggregate_restore_worker()?;
        let enable_experiment_aggregate = builder.settings.get_enable_experiment_aggregate()?;
        let enable_experiment_hash_index = builder.settings.get_enable_experiment_hash_index()?;

        let mut is_cluster_aggregate = false;
        if ExchangeSource::check_physical_plan(&self.input) {
            is_cluster_aggregate = true;
        }

        let params = PipelineBuilder::build_aggregator_params(
            self.before_group_by_schema.clone(),
            &self.group_by,
            &self.agg_funcs,
            is_cluster_aggregate,
            max_spill_io_requests as usize,
            enable_experiment_aggregate,
            enable_experiment_hash_index,
            max_block_rows,
            max_block_bytes,
        )?;

        if params.group_columns.is_empty() {
            self.input.build_pipeline(builder)?;

            builder.main_pipeline.try_resize(1)?;
            builder.main_pipeline.add_transform(|input, output| {
                Ok(ProcessorPtr::create(
                    FinalSingleStateAggregator::try_create(input, output, &params)?,
                ))
            })?;

            return Ok(());
        }

        let old_inject = builder.exchange_injector.clone();

        if ExchangeSource::check_physical_plan(&self.input) {
            builder.exchange_injector = if params.enable_experiment_aggregate {
                AggregateInjector::<true>::create(
                    builder.ctx.clone(),
                    params.clone(),
                    self.shuffle_mode.clone(),
                )
            } else {
                AggregateInjector::<false>::create(
                    builder.ctx.clone(),
                    params.clone(),
                    self.shuffle_mode.clone(),
                )
            };
        }

        self.input.build_pipeline(builder)?;

        // For distributed plans, since we are unaware of the data size processed by other nodes,
        // we estimate the parallelism based on the worst-case scenario.
        let after_group_parallel = match self.input.is_distributed_plan() {
            true => builder.settings.get_max_threads()? as usize,
            false => builder.main_pipeline.output_len(),
        };

        builder.exchange_injector = old_inject;
        build_partition_bucket(
            &mut builder.main_pipeline,
            params.clone(),
            max_restore_worker,
            after_group_parallel,
            builder.ctx.clone(),
            self.shuffle_mode.clone(),
        )
    }
}

impl PhysicalPlanBuilder {
    pub async fn build_aggregate(
        &mut self,
        s_expr: &SExpr,
        agg: &Aggregate,
        mut required: ColumnSet,
        stat_info: PlanStatsInfo,
    ) -> Result<PhysicalPlan> {
        // 1. Prune unused Columns.
        let mut used = vec![];
        for item in &agg.aggregate_functions {
            if required.contains(&item.index) {
                required.extend(item.scalar.used_columns());
                used.push(item.clone());
            }
        }

        agg.group_items.iter().for_each(|i| {
            // If the group item comes from a complex expression, we only include the final
            // column index here. The used columns will be included in its EvalScalar child.
            required.insert(i.index);
        });

        // single key without aggregation
        if agg.group_items.is_empty() && used.is_empty() {
            let mut s =
                ConstantTableScan::new_empty_scan(DataSchemaRef::default(), ColumnSet::new());
            s.num_rows = 1;
            let expr = SExpr::create_leaf(s);
            return self.build(&expr, required).await;
        }

        let agg = Aggregate {
            group_items: agg.group_items.clone(),
            aggregate_functions: used,
            from_distinct: agg.from_distinct,
            mode: agg.mode,
            rank_limit: agg.rank_limit.clone(),
            grouping_sets: agg.grouping_sets.clone(),
        };

        // 2. Build physical plan.
        let input = self.build(s_expr.child(0)?, required).await?;
        let input_schema = input.output_schema()?;
        let group_items = agg.group_items.iter().map(|v| v.index).collect::<Vec<_>>();

        let result: PhysicalPlan = match &agg.mode {
            AggregateMode::Partial => {
                let group_by_display = agg
                    .group_items
                    .iter()
                    .map(|item| Ok(item.scalar.as_expr()?.sql_display()))
                    .collect::<Result<Vec<_>>>()?;

                let mut agg_funcs =
                    build_aggregate_function(&agg.aggregate_functions, &input_schema)?;

                let settings = self.ctx.get_settings();
                let mut group_by_shuffle_mode = settings.get_group_by_shuffle_mode()?;
                if agg.grouping_sets.is_some() {
                    group_by_shuffle_mode = "before_merge".to_string();
                }

                let is_cluster_aggregate = Exchange::check_physical_plan(&input);
                let shuffle_mode = determine_shuffle_mode(self.ctx.clone(), is_cluster_aggregate)?;

                if let Some(grouping_sets) = agg.grouping_sets.as_ref() {
                    // ignore `_grouping_id`.
                    // If the aggregation function argument if a group item,
                    // we cannot use the group item directly.
                    // It's because the group item will be wrapped with nullable and fill dummy NULLs (in `AggregateExpand` plan),
                    // which will cause panic while executing aggregation function.
                    // To avoid the panic, we will duplicate (`Arc::clone`) original group item columns in `AggregateExpand`,
                    // we should use these columns instead.
                    for func in agg_funcs.iter_mut() {
                        for arg in func.arg_indices.iter_mut() {
                            if let Some(pos) = group_items.iter().position(|g| g == arg) {
                                *arg = grouping_sets.dup_group_items[pos].0;
                            }
                        }
                    }
                }

                let rank_limit = agg.rank_limit.map(|(item, limit)| {
                    let desc = item
                        .iter()
                        .map(|v| SortDesc {
                            asc: v.asc,
                            nulls_first: v.nulls_first,
                            order_by: v.index,
                            display_name: self.metadata.read().column(v.index).name(),
                        })
                        .collect::<Vec<_>>();
                    (desc, limit)
                });

                if group_by_shuffle_mode == "before_merge"
                    && let Some(exchange) = Exchange::from_physical_plan(&input)
                {
                    let kind = exchange.kind.clone();
                    let aggregate_partial = if let Some(grouping_sets) = agg.grouping_sets {
                        let expand = PhysicalPlan::new(AggregateExpand {
                            grouping_sets,
                            input: exchange.input.clone(),
                            group_bys: group_items.clone(),
                            stat_info: Some(stat_info.clone()),
                            meta: PhysicalPlanMeta::new("AggregateExpand"),
                        });

                        AggregatePartial {
                            input: expand,
                            agg_funcs,
                            group_by_display,
                            group_by: group_items,
                            stat_info: Some(stat_info),
                            rank_limit: None,
                            meta: PhysicalPlanMeta::new("AggregatePartial"),
                            shuffle_mode: shuffle_mode.clone(),
                        }
                    } else {
                        AggregatePartial {
                            input: exchange.input.clone(),
                            agg_funcs,
                            rank_limit,
                            group_by_display,
                            group_by: group_items,
                            stat_info: Some(stat_info),
                            meta: PhysicalPlanMeta::new("AggregatePartial"),
                            shuffle_mode: shuffle_mode.clone(),
                        }
                    };

                    let keys = {
                        let schema = aggregate_partial.output_schema()?;
                        let end = schema.num_fields();
                        let start = end - aggregate_partial.group_by.len();
                        (start..end)
                            .map(|id| RemoteExpr::ColumnRef {
                                span: None,
                                id,
                                data_type: schema.field(id).data_type().clone(),
                                display_name: (id - start).to_string(),
                            })
                            .collect()
                    };

                    PhysicalPlan::new(Exchange {
                        keys,
                        kind,
                        ignore_exchange: false,
                        allow_adjust_parallelism: true,
                        meta: PhysicalPlanMeta::new("Exchange"),
                        input: PhysicalPlan::new(aggregate_partial),
                    })
                } else if let Some(grouping_sets) = agg.grouping_sets {
                    let expand = AggregateExpand {
                        input,
                        grouping_sets,
                        group_bys: group_items.clone(),
                        stat_info: Some(stat_info.clone()),
                        meta: PhysicalPlanMeta::new("AggregateExpand"),
                    };

                    PhysicalPlan::new(AggregatePartial {
                        agg_funcs,
                        group_by_display,
                        rank_limit: None,
                        group_by: group_items,
                        input: PhysicalPlan::new(expand),
                        stat_info: Some(stat_info),
                        meta: PhysicalPlanMeta::new("AggregatePartial"),
                        shuffle_mode: shuffle_mode.clone(),
                    })
                } else {
                    PhysicalPlan::new(AggregatePartial {
                        input,
                        agg_funcs,
                        group_by_display,
                        group_by: group_items,
                        stat_info: Some(stat_info),
                        rank_limit,
                        meta: PhysicalPlanMeta::new("AggregatePartial"),
                        shuffle_mode: shuffle_mode.clone(),
                    })
                }
            }

            // Hack to get before group by schema, we should refactor this
            AggregateMode::Final => {
                let input_schema = {
                    let mut plan = &input;

                    if let Some(exchange) = Exchange::from_physical_plan(plan) {
                        plan = &exchange.input;
                    }

                    let Some(aggregate) = AggregatePartial::from_physical_plan(plan) else {
                        return Err(ErrorCode::Internal(format!(
                            "invalid input physical plan: {}",
                            input.get_name(),
                        )));
                    };

                    aggregate.input.output_schema()?
                };

                let mut agg_funcs =
                    build_aggregate_function(&agg.aggregate_functions, &input_schema)?;

                if let Some(grouping_sets) = agg.grouping_sets.as_ref() {
                    // The argument types are wrapped nullable due to `AggregateExpand` plan. We should recover them to original types.
                    for func in agg_funcs.iter_mut() {
                        for (arg, ty) in func.arg_indices.iter_mut().zip(func.sig.args.iter_mut()) {
                            if let Some(pos) = group_items.iter().position(|g| g == arg) {
                                *arg = grouping_sets.dup_group_items[pos].0;
                                *ty = grouping_sets.dup_group_items[pos].1.clone();
                            }
                        }
                    }
                }

                if let Some(partial) = AggregatePartial::from_physical_plan(&input) {
                    let group_by_display = partial.group_by_display.clone();
                    let before_group_by_schema = partial.input.output_schema()?;
                    let shuffle_mode = partial.shuffle_mode.clone();

                    PhysicalPlan::new(AggregateFinal {
                        input,
                        agg_funcs,
                        group_by_display,
                        before_group_by_schema,
                        group_by: group_items,
                        stat_info: Some(stat_info),
                        meta: PhysicalPlanMeta::new("AggregateFinal"),
                        shuffle_mode,
                    })
                } else {
                    let Some(exchange) = Exchange::from_physical_plan(&input) else {
                        return Err(ErrorCode::Internal(format!(
                            "invalid input physical plan: {}",
                            input.get_name(),
                        )));
                    };

                    let Some(partial) = AggregatePartial::from_physical_plan(&exchange.input)
                    else {
                        return Err(ErrorCode::Internal(format!(
                            "invalid input physical plan: {}",
                            input.get_name(),
                        )));
                    };

                    let group_by_display = partial.group_by_display.clone();
                    let before_group_by_schema = partial.input.output_schema()?;
                    let shuffle_mode = partial.shuffle_mode.clone();

                    PhysicalPlan::new(AggregateFinal {
                        input,
                        agg_funcs,
                        group_by_display,
                        before_group_by_schema,
                        group_by: group_items,
                        stat_info: Some(stat_info),
                        meta: PhysicalPlanMeta::new("AggregateFinal"),
                        shuffle_mode,
                    })
                }
            }
            AggregateMode::Initial => {
                return Err(ErrorCode::Internal("Invalid aggregate mode: Initial"));
            }
        };

        Ok(result)
    }
}

#[derive(serde::Serialize, serde::Deserialize, Clone, Debug)]
pub enum AggregateShuffleMode {
    // calculate shuffle destination based on hash of rows
    Row,
    // calculate shuffle destination based on id of bucket
    // cpu_nums in cluster stored in it
    Bucket(u64),
}

impl AggregateShuffleMode {
    pub fn determine_radix_bits(&self) -> u64 {
        match &self {
            AggregateShuffleMode::Row => 0,
            AggregateShuffleMode::Bucket(hint) => hint.trailing_zeros() as u64,
        }
    }
}

// TODO: ROW_SHUFFLE_ENABLE_THRESHOLD need to find
const ROW_SHUFFLE_ENABLE_THRESHOLD: u64 = 256;

fn determine_shuffle_mode(
    ctx: Arc<dyn TableContext>,
    is_cluster_aggregate: bool,
) -> Result<AggregateShuffleMode> {
    let settings = ctx.get_settings();
    let force_shuffle_mode = settings.get_force_aggregate_shuffle_mode()?;
    let thread_nums = settings.get_max_threads()?;

    let parallelism = if is_cluster_aggregate {
        (ctx.get_cluster().nodes.len() as u64 * thread_nums).next_power_of_two()
    } else {
        thread_nums.next_power_of_two()
    };

    let use_bucket = match force_shuffle_mode.as_str() {
        "row" => false,
        "bucket" => true,
        "auto" => parallelism <= ROW_SHUFFLE_ENABLE_THRESHOLD,
        _ => false,
    };

    let shuffle_mode = if use_bucket {
        AggregateShuffleMode::Bucket(parallelism)
    } else {
        AggregateShuffleMode::Row
    };

    Ok(shuffle_mode)
}

fn build_aggregate_function(
    agg_functions: &[ScalarItem],
    input_schema: &DataSchemaRef,
) -> Result<Vec<AggregateFunctionDesc>> {
    agg_functions
        .iter()
        .map(|v| match &v.scalar {
            ScalarExpr::AggregateFunction(agg) => {
                let arg_indices = agg
                    .args
                    .iter()
                    .map(|arg| {
                        if let ScalarExpr::BoundColumnRef(col) = arg {
                            Ok(col.column.index)
                        } else {
                            Err(ErrorCode::Internal(
                                "Aggregate function argument must be a BoundColumnRef".to_string(),
                            ))
                        }
                    })
                    .collect::<Result<Vec<_>>>()?;
                let sort_desc_indices = agg
                    .sort_descs
                    .iter()
                    .map(|desc| {
                        if let ScalarExpr::BoundColumnRef(col) = &desc.expr {
                            Ok(col.column.index)
                        } else {
                            Err(ErrorCode::Internal(
                                "Aggregate function sort description must be a BoundColumnRef"
                                    .to_string(),
                            ))
                        }
                    })
                    .collect::<Result<_>>()?;
                let args = arg_indices
                    .iter()
                    .map(|i| {
                        Ok(input_schema
                            .field_with_name(&i.to_string())?
                            .data_type()
                            .clone())
                    })
                    .collect::<Result<_>>()?;
                let sort_descs = agg
                    .sort_descs
                    .iter()
                    .map(|desc| desc.try_into())
                    .collect::<Result<_>>()?;
                Ok(AggregateFunctionDesc {
                    sig: AggregateFunctionSignature {
                        name: agg.func_name.clone(),
                        udaf: None,
                        return_type: *agg.return_type.clone(),
                        args,
                        params: agg.params.clone(),
                        sort_descs,
                    },
                    output_column: v.index,
                    arg_indices,
                    sort_desc_indices,
                    display: v.scalar.as_expr()?.sql_display(),
                })
            }
            ScalarExpr::UDAFCall(udaf) => {
                let arg_indices = udaf
                    .arguments
                    .iter()
                    .map(|arg| {
                        if let ScalarExpr::BoundColumnRef(col) = arg {
                            Ok(col.column.index)
                        } else {
                            Err(ErrorCode::Internal(
                                "Aggregate function argument must be a BoundColumnRef".to_string(),
                            ))
                        }
                    })
                    .collect::<Result<Vec<_>>>()?;
                let args = arg_indices
                    .iter()
                    .map(|i| {
                        Ok(input_schema
                            .field_with_name(&i.to_string())?
                            .data_type()
                            .clone())
                    })
                    .collect::<Result<_>>()?;

                Ok(AggregateFunctionDesc {
                    sig: AggregateFunctionSignature {
                        name: udaf.name.clone(),
                        udaf: Some((udaf.udf_type.clone(), udaf.state_fields.clone())),
                        return_type: *udaf.return_type.clone(),
                        args,
                        params: vec![],
                        sort_descs: vec![],
                    },
                    output_column: v.index,
                    arg_indices,
                    sort_desc_indices: vec![],
                    display: v.scalar.as_expr()?.sql_display(),
                })
            }
            _ => Err(ErrorCode::Internal(
                "Expected aggregate function".to_string(),
            )),
        })
        .collect::<Result<_>>()
}
