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

use std::sync::Arc;

use databend_common_exception::ErrorCode;
use databend_common_exception::Result;
use databend_common_expression::DataField;
use databend_common_expression::DataSchemaRef;
use databend_common_expression::DataSchemaRefExt;
use databend_common_expression::RemoteExpr;

use super::SortDesc;
use crate::executor::explain::PlanStatsInfo;
use crate::executor::physical_plans::AggregateExpand;
use crate::executor::physical_plans::AggregateFunctionDesc;
use crate::executor::physical_plans::AggregateFunctionSignature;
use crate::executor::physical_plans::AggregatePartial;
use crate::executor::physical_plans::Exchange;
use crate::executor::PhysicalPlan;
use crate::executor::PhysicalPlanBuilder;
use crate::optimizer::SExpr;
use crate::plans::AggregateMode;
use crate::plans::DummyTableScan;
use crate::ColumnSet;
use crate::IndexType;
use crate::ScalarExpr;

#[derive(serde::Serialize, serde::Deserialize, Clone, Debug)]
pub struct AggregateFinal {
    // A unique id of operator in a `PhysicalPlan` tree, only used for display.
    pub plan_id: u32,
    pub input: Box<PhysicalPlan>,
    pub group_by: Vec<IndexType>,
    pub agg_funcs: Vec<AggregateFunctionDesc>,
    pub before_group_by_schema: DataSchemaRef,
    pub group_by_display: Vec<String>,

    // Only used for explain
    pub stat_info: Option<PlanStatsInfo>,
}

impl AggregateFinal {
    pub fn output_schema(&self) -> Result<DataSchemaRef> {
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
}

impl PhysicalPlanBuilder {
    pub(crate) async fn build_aggregate(
        &mut self,
        s_expr: &SExpr,
        agg: &crate::plans::Aggregate,
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

        if agg.group_items.is_empty() && used.is_empty() {
            let expr = SExpr::create_leaf(Arc::new(DummyTableScan.into()));
            return self.build(&expr, required).await;
        }

        let agg = crate::plans::Aggregate {
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

        let result = match &agg.mode {
            AggregateMode::Partial => {
                let group_by_display = agg
                    .group_items
                    .iter()
                    .map(|item| Ok(item.scalar.as_expr()?.sql_display()))
                    .collect::<Result<Vec<_>>>()?;

                let mut agg_funcs: Vec<AggregateFunctionDesc> = agg
                    .aggregate_functions
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
                                            "Aggregate function argument must be a BoundColumnRef"
                                                .to_string(),
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
                            let sort_desc_indices = agg.sort_descs
                                .iter()
                                .map(|desc| {
                                    if let ScalarExpr::BoundColumnRef(col) = &desc.expr {
                                        Ok(col.column.index)
                                    } else {
                                        Err(ErrorCode::Internal(
                                            "Aggregate function description must be a BoundColumnRef"
                                                .to_string(),
                                        ))
                                    }
                                })
                                .collect::<Result<_>>()?;
                            let sort_descs = agg.sort_descs
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
                                            "Aggregate function argument must be a BoundColumnRef"
                                                .to_string(),
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
                    .collect::<Result<_>>()?;

                let settings = self.ctx.get_settings();
                let group_by_shuffle_mode = settings.get_group_by_shuffle_mode()?;
                let enable_experimental_aggregate_hashtable =
                    settings.get_enable_experimental_aggregate_hashtable()?;

                if let Some(grouping_sets) = agg.grouping_sets.as_ref() {
                    assert_eq!(grouping_sets.dup_group_items.len(), group_items.len() - 1); // ignore `_grouping_id`.
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

                match input {
                    PhysicalPlan::Exchange(Exchange { input, kind, .. })
                        if group_by_shuffle_mode == "before_merge" =>
                    {
                        let aggregate_partial = if let Some(grouping_sets) = agg.grouping_sets {
                            let expand = AggregateExpand {
                                plan_id: 0,
                                input,
                                group_bys: group_items.clone(),
                                grouping_sets,
                                stat_info: Some(stat_info.clone()),
                            };
                            AggregatePartial {
                                plan_id: 0,
                                input: Box::new(PhysicalPlan::AggregateExpand(expand)),
                                agg_funcs,
                                enable_experimental_aggregate_hashtable,
                                group_by_display,
                                group_by: group_items,
                                stat_info: Some(stat_info),
                                rank_limit: None,
                            }
                        } else {
                            AggregatePartial {
                                plan_id: 0,
                                input,
                                agg_funcs,
                                enable_experimental_aggregate_hashtable,
                                group_by_display,
                                group_by: group_items,
                                stat_info: Some(stat_info),
                                rank_limit,
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

                        PhysicalPlan::Exchange(Exchange {
                            plan_id: 0,
                            kind,
                            allow_adjust_parallelism: true,
                            ignore_exchange: false,
                            input: Box::new(PhysicalPlan::AggregatePartial(aggregate_partial)),
                            keys,
                        })
                    }
                    _ => {
                        if let Some(grouping_sets) = agg.grouping_sets {
                            let expand = AggregateExpand {
                                plan_id: 0,
                                input: Box::new(input),
                                group_bys: group_items.clone(),
                                grouping_sets,
                                stat_info: Some(stat_info.clone()),
                            };
                            PhysicalPlan::AggregatePartial(AggregatePartial {
                                plan_id: 0,
                                agg_funcs,
                                enable_experimental_aggregate_hashtable,
                                group_by_display,
                                group_by: group_items,
                                input: Box::new(PhysicalPlan::AggregateExpand(expand)),
                                stat_info: Some(stat_info),
                                rank_limit: None,
                            })
                        } else {
                            PhysicalPlan::AggregatePartial(AggregatePartial {
                                plan_id: 0,
                                agg_funcs,
                                enable_experimental_aggregate_hashtable,
                                group_by_display,
                                group_by: group_items,
                                input: Box::new(input),
                                stat_info: Some(stat_info),
                                rank_limit,
                            })
                        }
                    }
                }
            }

            // Hack to get before group by schema, we should refactor this
            AggregateMode::Final => {
                let input_schema = match input {
                    PhysicalPlan::AggregatePartial(ref agg) => agg.input.output_schema()?,

                    PhysicalPlan::Exchange(Exchange {
                        input: box PhysicalPlan::AggregatePartial(ref agg),
                        ..
                    }) => agg.input.output_schema()?,

                    _ => {
                        return Err(ErrorCode::Internal(format!(
                            "invalid input physical plan: {}",
                            input.name(),
                        )));
                    }
                };

                let mut agg_funcs: Vec<AggregateFunctionDesc> = agg
                    .aggregate_functions
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
                                            "Aggregate function argument must be a BoundColumnRef"
                                                .to_string(),
                                        ))
                                    }
                                })
                                .collect::<Result<Vec<_>>>()?;
                            let sort_desc_indices = agg.sort_descs
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
                            let sort_descs = agg.sort_descs
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
                                            "Aggregate function argument must be a BoundColumnRef"
                                                .to_string(),
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
                    .collect::<Result<_>>()?;

                if let Some(grouping_sets) = agg.grouping_sets.as_ref() {
                    // The argument types are wrapped nullable due to `AggregateExpand` plan. We should recover them to original types.
                    assert_eq!(grouping_sets.dup_group_items.len(), group_items.len() - 1); // ignore `_grouping_id`.
                    for func in agg_funcs.iter_mut() {
                        for (arg, ty) in func.arg_indices.iter_mut().zip(func.sig.args.iter_mut()) {
                            if let Some(pos) = group_items.iter().position(|g| g == arg) {
                                *arg = grouping_sets.dup_group_items[pos].0;
                                *ty = grouping_sets.dup_group_items[pos].1.clone();
                            }
                        }
                    }
                }

                match input {
                    PhysicalPlan::AggregatePartial(ref partial) => {
                        let before_group_by_schema = partial.input.output_schema()?;

                        PhysicalPlan::AggregateFinal(AggregateFinal {
                            plan_id: 0,
                            group_by_display: partial.group_by_display.clone(),
                            input: Box::new(input),
                            group_by: group_items,
                            agg_funcs,
                            before_group_by_schema,

                            stat_info: Some(stat_info),
                        })
                    }

                    PhysicalPlan::Exchange(Exchange {
                        input: box PhysicalPlan::AggregatePartial(ref partial),
                        ..
                    }) => {
                        let before_group_by_schema = partial.input.output_schema()?;

                        PhysicalPlan::AggregateFinal(AggregateFinal {
                            plan_id: 0,
                            group_by_display: partial.group_by_display.clone(),
                            input: Box::new(input),
                            group_by: group_items,
                            agg_funcs,
                            before_group_by_schema,

                            stat_info: Some(stat_info),
                        })
                    }

                    _ => {
                        return Err(ErrorCode::Internal(format!(
                            "invalid input physical plan: {}",
                            input.name(),
                        )));
                    }
                }
            }
            AggregateMode::Initial => {
                return Err(ErrorCode::Internal("Invalid aggregate mode: Initial"));
            }
        };

        Ok(result)
    }
}
