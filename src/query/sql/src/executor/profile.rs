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
use common_functions::BUILTIN_FUNCTIONS;
use common_profile::AggregateAttribute;
use common_profile::AggregateExpandAttribute;
use common_profile::CteScanAttribute;
use common_profile::EvalScalarAttribute;
use common_profile::ExchangeAttribute;
use common_profile::FilterAttribute;
use common_profile::JoinAttribute;
use common_profile::LambdaAttribute;
use common_profile::LimitAttribute;
use common_profile::OperatorAttribute;
use common_profile::OperatorProfile;
use common_profile::OperatorType;
use common_profile::ProcessorProfiles;
use common_profile::ProjectSetAttribute;
use common_profile::QueryProfile;
use common_profile::SortAttribute;
use common_profile::TableScanAttribute;
use common_profile::WindowAttribute;
use itertools::Itertools;

use crate::executor::format::pretty_display_agg_desc;
use crate::executor::physical_plans::common::FragmentKind;
use crate::executor::physical_plans::physical_window::WindowFunction;
use crate::executor::PhysicalPlan;
use crate::planner::Metadata;
use crate::MetadataRef;

pub struct ProfileHelper;

impl ProfileHelper {
    pub fn build_query_profile(
        query_id: &str,
        metadata: &MetadataRef,
        plan: &PhysicalPlan,
        profs: &ProcessorProfiles,
    ) -> Result<QueryProfile> {
        let mut plan_node_profs = vec![];
        let metadata = metadata.read().clone();
        flatten_plan_node_profile(&metadata, plan, profs, &mut plan_node_profs)?;

        Ok(QueryProfile::new(query_id.to_string(), plan_node_profs))
    }
}

fn flatten_plan_node_profile(
    metadata: &Metadata,
    plan: &PhysicalPlan,
    profs: &ProcessorProfiles,
    plan_node_profs: &mut Vec<OperatorProfile>,
) -> Result<()> {
    match plan {
        PhysicalPlan::TableScan(scan) => {
            let table = metadata.table(scan.table_index).clone();
            let qualified_name = format!("{}.{}", table.database(), table.name());
            let proc_prof = profs.get(&scan.plan_id).copied().unwrap_or_default();
            let prof = OperatorProfile {
                id: scan.plan_id,
                operator_type: OperatorType::TableScan,
                children: vec![],
                execution_info: proc_prof.into(),
                attribute: OperatorAttribute::TableScan(TableScanAttribute { qualified_name }),
            };
            plan_node_profs.push(prof);
        }
        PhysicalPlan::CteScan(scan) => {
            let prof = OperatorProfile {
                id: scan.plan_id,
                operator_type: OperatorType::CteScan,
                children: vec![],
                execution_info: Default::default(),
                attribute: OperatorAttribute::CteScan(CteScanAttribute {
                    cte_idx: scan.cte_idx.0,
                }),
            };
            plan_node_profs.push(prof)
        }
        PhysicalPlan::ConstantTableScan(scan) => {
            let proc_prof = profs.get(&scan.plan_id).copied().unwrap_or_default();
            let prof = OperatorProfile {
                id: scan.plan_id,
                operator_type: OperatorType::ConstantTableScan,
                children: vec![],
                execution_info: proc_prof.into(),
                attribute: OperatorAttribute::Empty,
            };
            plan_node_profs.push(prof);
        }
        PhysicalPlan::Filter(filter) => {
            flatten_plan_node_profile(metadata, &filter.input, profs, plan_node_profs)?;
            let proc_prof = profs.get(&filter.plan_id).copied().unwrap_or_default();
            let prof = OperatorProfile {
                id: filter.plan_id,
                operator_type: OperatorType::Filter,
                children: vec![filter.input.get_id()],
                execution_info: proc_prof.into(),
                attribute: OperatorAttribute::Filter(FilterAttribute {
                    predicate: filter
                        .predicates
                        .iter()
                        .map(|pred| pred.as_expr(&BUILTIN_FUNCTIONS).sql_display())
                        .join(" AND "),
                }),
            };
            plan_node_profs.push(prof);
        }
        PhysicalPlan::Project(project) => {
            flatten_plan_node_profile(metadata, &project.input, profs, plan_node_profs)?;
            let proc_prof = profs.get(&project.plan_id).copied().unwrap_or_default();
            let prof = OperatorProfile {
                id: project.plan_id,
                operator_type: OperatorType::Project,
                children: vec![project.input.get_id()],
                execution_info: proc_prof.into(),
                attribute: OperatorAttribute::Empty,
            };
            plan_node_profs.push(prof);
        }
        PhysicalPlan::EvalScalar(eval) => {
            flatten_plan_node_profile(metadata, &eval.input, profs, plan_node_profs)?;
            let proc_prof = profs.get(&eval.plan_id).copied().unwrap_or_default();
            let prof = OperatorProfile {
                id: eval.plan_id,
                operator_type: OperatorType::EvalScalar,
                execution_info: proc_prof.into(),
                children: vec![eval.input.get_id()],
                attribute: OperatorAttribute::EvalScalar(EvalScalarAttribute {
                    scalars: eval
                        .exprs
                        .iter()
                        .map(|(expr, _)| expr.as_expr(&BUILTIN_FUNCTIONS).sql_display())
                        .join(", "),
                }),
            };
            plan_node_profs.push(prof);
        }
        PhysicalPlan::ProjectSet(project_set) => {
            flatten_plan_node_profile(metadata, &project_set.input, profs, plan_node_profs)?;
            let proc_prof = profs.get(&project_set.plan_id).copied().unwrap_or_default();
            let prof = OperatorProfile {
                id: project_set.plan_id,
                operator_type: OperatorType::ProjectSet,
                execution_info: proc_prof.into(),
                children: vec![project_set.input.get_id()],
                attribute: OperatorAttribute::ProjectSet(ProjectSetAttribute {
                    functions: project_set
                        .srf_exprs
                        .iter()
                        .map(|(expr, _)| expr.as_expr(&BUILTIN_FUNCTIONS).sql_display())
                        .join(", "),
                }),
            };
            plan_node_profs.push(prof);
        }
        PhysicalPlan::Lambda(lambda) => {
            flatten_plan_node_profile(metadata, &lambda.input, profs, plan_node_profs)?;
            let proc_prof = profs.get(&lambda.plan_id).copied().unwrap_or_default();
            let prof = OperatorProfile {
                id: lambda.plan_id,
                operator_type: OperatorType::Lambda,
                execution_info: proc_prof.into(),
                children: vec![lambda.input.get_id()],
                attribute: OperatorAttribute::Lambda(LambdaAttribute {
                    scalars: lambda
                        .lambda_funcs
                        .iter()
                        .map(|func| {
                            let arg_exprs = func.arg_exprs.join(", ");
                            let params = func.params.join(", ");
                            let lambda_expr =
                                func.lambda_expr.as_expr(&BUILTIN_FUNCTIONS).sql_display();
                            format!(
                                "{}({}, {} -> {})",
                                func.func_name, arg_exprs, params, lambda_expr
                            )
                        })
                        .join(", "),
                }),
            };
            plan_node_profs.push(prof);
        }
        PhysicalPlan::AggregateExpand(expand) => {
            flatten_plan_node_profile(metadata, &expand.input, profs, plan_node_profs)?;
            let proc_prof = profs.get(&expand.plan_id).copied().unwrap_or_default();
            let prof = OperatorProfile {
                id: expand.plan_id,
                operator_type: OperatorType::AggregateExpand,
                execution_info: proc_prof.into(),
                children: vec![expand.input.get_id()],
                attribute: OperatorAttribute::AggregateExpand(AggregateExpandAttribute {
                    group_keys: expand
                        .grouping_sets
                        .sets
                        .iter()
                        .map(|columns| {
                            format!(
                                "[{}]",
                                columns
                                    .iter()
                                    .map(|column| metadata.column(*column).name())
                                    .join(", ")
                            )
                        })
                        .join(", "),
                    aggr_exprs: "".to_string(),
                }),
            };
            plan_node_profs.push(prof);
        }
        PhysicalPlan::AggregatePartial(agg_partial) => {
            flatten_plan_node_profile(metadata, &agg_partial.input, profs, plan_node_profs)?;
            let proc_prof = profs.get(&agg_partial.plan_id).copied().unwrap_or_default();
            let prof = OperatorProfile {
                id: agg_partial.plan_id,
                operator_type: OperatorType::Aggregate,
                execution_info: proc_prof.into(),
                children: vec![agg_partial.input.get_id()],
                attribute: OperatorAttribute::Aggregate(AggregateAttribute {
                    group_keys: agg_partial
                        .group_by
                        .iter()
                        .map(|column| metadata.column(*column).name())
                        .join(", "),
                    functions: agg_partial
                        .agg_funcs
                        .iter()
                        .map(|desc| pretty_display_agg_desc(desc, metadata))
                        .join(", "),
                }),
            };
            plan_node_profs.push(prof);
        }
        PhysicalPlan::AggregateFinal(agg_final) => {
            flatten_plan_node_profile(metadata, &agg_final.input, profs, plan_node_profs)?;
            let proc_prof = profs.get(&agg_final.plan_id).copied().unwrap_or_default();
            let prof = OperatorProfile {
                id: agg_final.plan_id,
                operator_type: OperatorType::Aggregate,
                execution_info: proc_prof.into(),
                children: vec![agg_final.input.get_id()],
                attribute: OperatorAttribute::Aggregate(AggregateAttribute {
                    group_keys: agg_final
                        .group_by
                        .iter()
                        .map(|column| metadata.column(*column).name())
                        .join(", "),
                    functions: agg_final
                        .agg_funcs
                        .iter()
                        .map(|desc| pretty_display_agg_desc(desc, metadata))
                        .join(", "),
                }),
            };
            plan_node_profs.push(prof);
        }
        PhysicalPlan::Window(window) => {
            flatten_plan_node_profile(metadata, &window.input, profs, plan_node_profs)?;
            let proc_prof = profs.get(&window.plan_id).copied().unwrap_or_default();
            let partition_by = window
                .partition_by
                .iter()
                .map(|&index| {
                    let name = metadata.column(index).name();
                    Ok(name)
                })
                .collect::<Result<Vec<_>>>()?
                .join(", ");

            let order_by = window
                .order_by
                .iter()
                .map(|v| {
                    let name = metadata.column(v.order_by).name();
                    Ok(name)
                })
                .collect::<Result<Vec<_>>>()?
                .join(", ");

            let frame = window.window_frame.to_string();

            let func = match &window.func {
                WindowFunction::Aggregate(agg) => pretty_display_agg_desc(agg, metadata),
                func => format!("{}", func),
            };
            let prof = OperatorProfile {
                id: window.plan_id,
                operator_type: OperatorType::Window,
                children: vec![window.input.get_id()],
                execution_info: proc_prof.into(),
                attribute: OperatorAttribute::Window(WindowAttribute {
                    functions: format!(
                        "{} OVER (PARTITION BY {} ORDER BY {} {})",
                        func, partition_by, order_by, frame
                    ),
                }),
            };
            plan_node_profs.push(prof);
        }
        PhysicalPlan::Sort(sort) => {
            flatten_plan_node_profile(metadata, &sort.input, profs, plan_node_profs)?;
            let proc_prof = profs.get(&sort.plan_id).copied().unwrap_or_default();
            let prof = OperatorProfile {
                id: sort.plan_id,
                operator_type: OperatorType::Sort,
                execution_info: proc_prof.into(),
                children: vec![sort.input.get_id()],
                attribute: OperatorAttribute::Sort(SortAttribute {
                    sort_keys: sort
                        .order_by
                        .iter()
                        .map(|desc| {
                            format!(
                                "{} {}",
                                metadata.column(desc.order_by).name(),
                                if desc.asc { "ASC" } else { "DESC" }
                            )
                        })
                        .join(", "),
                }),
            };
            plan_node_profs.push(prof);
        }
        PhysicalPlan::Limit(limit) => {
            flatten_plan_node_profile(metadata, &limit.input, profs, plan_node_profs)?;
            let proc_prof = profs.get(&limit.plan_id).copied().unwrap_or_default();
            let prof = OperatorProfile {
                id: limit.plan_id,
                operator_type: OperatorType::Limit,
                execution_info: proc_prof.into(),
                children: vec![limit.input.get_id()],
                attribute: OperatorAttribute::Limit(LimitAttribute {
                    limit: limit.limit.unwrap_or_default(),
                    offset: limit.offset,
                }),
            };
            plan_node_profs.push(prof);
        }
        PhysicalPlan::RowFetch(fetch) => {
            flatten_plan_node_profile(metadata, &fetch.input, profs, plan_node_profs)?;
            let proc_prof = profs.get(&fetch.plan_id).copied().unwrap_or_default();
            let prof = OperatorProfile {
                id: fetch.plan_id,
                operator_type: OperatorType::RowFetch,
                execution_info: proc_prof.into(),
                children: vec![fetch.input.get_id()],
                attribute: OperatorAttribute::Empty,
            };
            plan_node_profs.push(prof);
        }
        PhysicalPlan::HashJoin(hash_join) => {
            flatten_plan_node_profile(metadata, &hash_join.probe, profs, plan_node_profs)?;
            flatten_plan_node_profile(metadata, &hash_join.build, profs, plan_node_profs)?;
            let proc_prof = profs.get(&hash_join.plan_id).copied().unwrap_or_default();
            let prof = OperatorProfile {
                id: hash_join.plan_id,
                operator_type: OperatorType::Join,
                execution_info: proc_prof.into(),
                children: vec![hash_join.probe.get_id(), hash_join.build.get_id()],
                attribute: OperatorAttribute::Join(JoinAttribute {
                    join_type: hash_join.join_type.to_string(),
                    equi_conditions: hash_join
                        .probe_keys
                        .iter()
                        .zip(hash_join.build_keys.iter())
                        .map(|(l, r)| {
                            format!(
                                "{} = {}",
                                l.as_expr(&BUILTIN_FUNCTIONS).sql_display(),
                                r.as_expr(&BUILTIN_FUNCTIONS).sql_display(),
                            )
                        })
                        .join(" AND "),
                    non_equi_conditions: hash_join
                        .non_equi_conditions
                        .iter()
                        .map(|expr| expr.as_expr(&BUILTIN_FUNCTIONS).sql_display())
                        .join(" AND "),
                }),
            };
            plan_node_profs.push(prof);
        }
        PhysicalPlan::RangeJoin(range_join) => {
            flatten_plan_node_profile(metadata, &range_join.left, profs, plan_node_profs)?;
            flatten_plan_node_profile(metadata, &range_join.right, profs, plan_node_profs)?;
            let proc_prof = profs.get(&range_join.plan_id).copied().unwrap_or_default();
            let prof = OperatorProfile {
                id: range_join.plan_id,
                operator_type: OperatorType::Join,
                children: vec![range_join.left.get_id(), range_join.right.get_id()],
                execution_info: proc_prof.into(),
                attribute: OperatorAttribute::Join(JoinAttribute {
                    join_type: range_join.join_type.to_string(),
                    equi_conditions: range_join
                        .conditions
                        .iter()
                        .map(|expr| {
                            format!(
                                "{} {} {}",
                                expr.left_expr.as_expr(&BUILTIN_FUNCTIONS).sql_display(),
                                expr.operator,
                                expr.right_expr.as_expr(&BUILTIN_FUNCTIONS).sql_display(),
                            )
                        })
                        .join(" AND "),
                    non_equi_conditions: range_join
                        .other_conditions
                        .iter()
                        .map(|expr| expr.as_expr(&BUILTIN_FUNCTIONS).sql_display())
                        .join(" AND "),
                }),
            };
            plan_node_profs.push(prof);
        }
        PhysicalPlan::Exchange(exchange) => {
            flatten_plan_node_profile(metadata, &exchange.input, profs, plan_node_profs)?;
            let proc_prof = profs.get(&exchange.plan_id).copied().unwrap_or_default();
            let prof = OperatorProfile {
                id: exchange.plan_id,
                operator_type: OperatorType::Exchange,
                execution_info: proc_prof.into(),
                children: vec![exchange.input.get_id()],
                attribute: OperatorAttribute::Exchange(ExchangeAttribute {
                    exchange_mode: match exchange.kind {
                        FragmentKind::Init => "Init".to_string(),
                        FragmentKind::Normal => "Hash".to_string(),
                        FragmentKind::Expansive => "Broadcast".to_string(),
                        FragmentKind::Merge => "Merge".to_string(),
                    },
                }),
            };
            plan_node_profs.push(prof);
        }
        PhysicalPlan::UnionAll(union) => {
            flatten_plan_node_profile(metadata, &union.left, profs, plan_node_profs)?;
            flatten_plan_node_profile(metadata, &union.right, profs, plan_node_profs)?;
            let proc_prof = profs.get(&union.plan_id).copied().unwrap_or_default();
            let prof = OperatorProfile {
                id: union.plan_id,
                operator_type: OperatorType::UnionAll,
                execution_info: proc_prof.into(),
                children: vec![union.left.get_id(), union.right.get_id()],
                attribute: OperatorAttribute::Empty,
            };
            plan_node_profs.push(prof);
        }
        PhysicalPlan::RuntimeFilterSource(source) => {
            let proc_prof = profs.get(&source.plan_id).copied().unwrap_or_default();
            let prof = OperatorProfile {
                id: source.plan_id,
                operator_type: OperatorType::RuntimeFilter,
                execution_info: proc_prof.into(),
                children: vec![],
                attribute: OperatorAttribute::Empty,
            };
            plan_node_profs.push(prof);
        }
        PhysicalPlan::DistributedInsertSelect(select) => {
            flatten_plan_node_profile(metadata, &select.input, profs, plan_node_profs)?;
            let proc_prof = profs.get(&select.plan_id).copied().unwrap_or_default();
            let prof = OperatorProfile {
                id: select.plan_id,
                operator_type: OperatorType::Insert,
                execution_info: proc_prof.into(),
                children: vec![],
                attribute: OperatorAttribute::Empty,
            };
            plan_node_profs.push(prof);
        }
        PhysicalPlan::ExchangeSource(source) => {
            let proc_prof = profs.get(&source.plan_id).copied().unwrap_or_default();
            let prof = OperatorProfile {
                id: source.plan_id,
                operator_type: OperatorType::Exchange,
                execution_info: proc_prof.into(),
                children: vec![],
                attribute: OperatorAttribute::Empty,
            };
            plan_node_profs.push(prof);
        }
        PhysicalPlan::ExchangeSink(sink) => {
            flatten_plan_node_profile(metadata, &sink.input, profs, plan_node_profs)?;
            let proc_prof = profs.get(&sink.plan_id).copied().unwrap_or_default();
            let prof = OperatorProfile {
                id: sink.plan_id,
                operator_type: OperatorType::Exchange,
                execution_info: proc_prof.into(),
                children: vec![],
                attribute: OperatorAttribute::Empty,
            };
            plan_node_profs.push(prof);
        }
        PhysicalPlan::MaterializedCte(_) => todo!(),
        PhysicalPlan::DeleteSource(_)
        | PhysicalPlan::CommitSink(_)
        | PhysicalPlan::CopyIntoTable(_)
        | PhysicalPlan::AsyncSourcer(_)
        | PhysicalPlan::MergeInto(_)
        | PhysicalPlan::MergeIntoSource(_)
        | PhysicalPlan::Deduplicate(_)
        | PhysicalPlan::ReplaceInto(_)
        | PhysicalPlan::CompactSource(_) => unreachable!(),
    }

    Ok(())
}
