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

use common_exception::ErrorCode;
use common_exception::Result;
use common_profile::PlanNodeProfile;
use common_profile::ProcessorProfiles;
use common_profile::QueryProfile;

use crate::executor::PhysicalPlan;

pub struct ProfileHelper;

impl ProfileHelper {
    pub fn build_query_profile(
        query_id: &str,
        plan: &PhysicalPlan,
        profs: &ProcessorProfiles,
    ) -> Result<QueryProfile> {
        let mut plan_node_profs = vec![];
        flatten_plan_node_profile(plan, profs, &mut plan_node_profs)?;

        Ok(QueryProfile::new(query_id.to_string(), plan_node_profs))
    }
}

fn flatten_plan_node_profile(
    plan: &PhysicalPlan,
    profs: &ProcessorProfiles,
    plan_node_profs: &mut Vec<PlanNodeProfile>,
) -> Result<()> {
    match plan {
        PhysicalPlan::TableScan(scan) => {
            let prof = PlanNodeProfile {
                id: scan.plan_id,
                plan_node_name: "TableScan".to_string(),
                description: "".to_string(),
                // We don't record the time spent on table scan for now
                cpu_time: Default::default(),
            };
            plan_node_profs.push(prof);
        }
        PhysicalPlan::Filter(filter) => {
            flatten_plan_node_profile(&filter.input, profs, plan_node_profs)?;
            let proc_prof = profs
                .get(&filter.plan_id)
                .ok_or_else(|| ErrorCode::Internal("Plan node profile not found"))?;
            let prof = PlanNodeProfile {
                id: filter.plan_id,
                plan_node_name: "Filter".to_string(),
                description: "".to_string(),
                cpu_time: proc_prof.cpu_time,
            };
            plan_node_profs.push(prof);
        }
        PhysicalPlan::Project(project) => {
            flatten_plan_node_profile(&project.input, profs, plan_node_profs)?;
            let proc_prof = profs
                .get(&project.plan_id)
                .ok_or_else(|| ErrorCode::Internal("Plan node profile not found"))?;
            let prof = PlanNodeProfile {
                id: project.plan_id,
                plan_node_name: "Project".to_string(),
                description: "".to_string(),
                cpu_time: proc_prof.cpu_time,
            };
            plan_node_profs.push(prof);
        }
        PhysicalPlan::EvalScalar(eval) => {
            flatten_plan_node_profile(&eval.input, profs, plan_node_profs)?;
            let proc_prof = profs
                .get(&eval.plan_id)
                .ok_or_else(|| ErrorCode::Internal("Plan node profile not found"))?;
            let prof = PlanNodeProfile {
                id: eval.plan_id,
                plan_node_name: "EvalScalar".to_string(),
                description: "".to_string(),
                cpu_time: proc_prof.cpu_time,
            };
            plan_node_profs.push(prof);
        }
        PhysicalPlan::ProjectSet(project_set) => {
            flatten_plan_node_profile(&project_set.input, profs, plan_node_profs)?;
            let proc_prof = profs
                .get(&project_set.plan_id)
                .ok_or_else(|| ErrorCode::Internal("Plan node profile not found"))?;
            let prof = PlanNodeProfile {
                id: project_set.plan_id,
                plan_node_name: "ProjectSet".to_string(),
                description: "".to_string(),
                cpu_time: proc_prof.cpu_time,
            };
            plan_node_profs.push(prof);
        }
        PhysicalPlan::AggregateExpand(expand) => {
            flatten_plan_node_profile(&expand.input, profs, plan_node_profs)?;
            let proc_prof = profs
                .get(&expand.plan_id)
                .ok_or_else(|| ErrorCode::Internal("Plan node profile not found"))?;
            let prof = PlanNodeProfile {
                id: expand.plan_id,
                plan_node_name: "AggregateExpand".to_string(),
                description: "".to_string(),
                cpu_time: proc_prof.cpu_time,
            };
            plan_node_profs.push(prof);
        }
        PhysicalPlan::AggregatePartial(agg_partial) => {
            flatten_plan_node_profile(&agg_partial.input, profs, plan_node_profs)?;
            let proc_prof = profs
                .get(&agg_partial.plan_id)
                .ok_or_else(|| ErrorCode::Internal("Plan node profile not found"))?;
            let prof = PlanNodeProfile {
                id: agg_partial.plan_id,
                plan_node_name: "AggregatePartial".to_string(),
                description: "".to_string(),
                cpu_time: proc_prof.cpu_time,
            };
            plan_node_profs.push(prof);
        }
        PhysicalPlan::AggregateFinal(agg_final) => {
            flatten_plan_node_profile(&agg_final.input, profs, plan_node_profs)?;
            let proc_prof = profs
                .get(&agg_final.plan_id)
                .ok_or_else(|| ErrorCode::Internal("Plan node profile not found"))?;
            let prof = PlanNodeProfile {
                id: agg_final.plan_id,
                plan_node_name: "AggregateFinal".to_string(),
                description: "".to_string(),
                cpu_time: proc_prof.cpu_time,
            };
            plan_node_profs.push(prof);
        }
        PhysicalPlan::Window(window) => {
            flatten_plan_node_profile(&window.input, profs, plan_node_profs)?;
            let proc_prof = profs
                .get(&window.plan_id)
                .ok_or_else(|| ErrorCode::Internal("Plan node profile not found"))?;
            let prof = PlanNodeProfile {
                id: window.plan_id,
                plan_node_name: "Window".to_string(),
                description: "".to_string(),
                cpu_time: proc_prof.cpu_time,
            };
            plan_node_profs.push(prof);
        }
        PhysicalPlan::Sort(sort) => {
            flatten_plan_node_profile(&sort.input, profs, plan_node_profs)?;
            let proc_prof = profs
                .get(&sort.plan_id)
                .ok_or_else(|| ErrorCode::Internal("Plan node profile not found"))?;
            let prof = PlanNodeProfile {
                id: sort.plan_id,
                plan_node_name: "Sort".to_string(),
                description: "".to_string(),
                cpu_time: proc_prof.cpu_time,
            };
            plan_node_profs.push(prof);
        }
        PhysicalPlan::Limit(limit) => {
            flatten_plan_node_profile(&limit.input, profs, plan_node_profs)?;
            let proc_prof = profs
                .get(&limit.plan_id)
                .ok_or_else(|| ErrorCode::Internal("Plan node profile not found"))?;
            let prof = PlanNodeProfile {
                id: limit.plan_id,
                plan_node_name: "Limit".to_string(),
                description: "".to_string(),
                cpu_time: proc_prof.cpu_time,
            };
            plan_node_profs.push(prof);
        }
        PhysicalPlan::RowFetch(fetch) => {
            flatten_plan_node_profile(&fetch.input, profs, plan_node_profs)?;
            let proc_prof = profs
                .get(&fetch.plan_id)
                .ok_or_else(|| ErrorCode::Internal("Plan node profile not found"))?;
            let prof = PlanNodeProfile {
                id: fetch.plan_id,
                plan_node_name: "RowFetch".to_string(),
                description: "".to_string(),
                cpu_time: proc_prof.cpu_time,
            };
            plan_node_profs.push(prof);
        }
        PhysicalPlan::HashJoin(hash_join) => {
            flatten_plan_node_profile(&hash_join.probe, profs, plan_node_profs)?;
            flatten_plan_node_profile(&hash_join.build, profs, plan_node_profs)?;
            let proc_prof = profs
                .get(&hash_join.plan_id)
                .ok_or_else(|| ErrorCode::Internal("Plan node profile not found"))?;
            let prof = PlanNodeProfile {
                id: hash_join.plan_id,
                plan_node_name: "HashJoin".to_string(),
                description: "".to_string(),
                cpu_time: proc_prof.cpu_time,
            };
            plan_node_profs.push(prof);
        }
        PhysicalPlan::RangeJoin(range_join) => {
            flatten_plan_node_profile(&range_join.left, profs, plan_node_profs)?;
            flatten_plan_node_profile(&range_join.right, profs, plan_node_profs)?;
            let proc_prof = profs
                .get(&range_join.plan_id)
                .ok_or_else(|| ErrorCode::Internal("Plan node profile not found"))?;
            let prof = PlanNodeProfile {
                id: range_join.plan_id,
                plan_node_name: "RangeJoin".to_string(),
                description: "".to_string(),
                cpu_time: proc_prof.cpu_time,
            };
            plan_node_profs.push(prof);
        }
        PhysicalPlan::Exchange(exchange) => {
            flatten_plan_node_profile(&exchange.input, profs, plan_node_profs)?;
            let proc_prof = profs
                .get(&exchange.plan_id)
                .ok_or_else(|| ErrorCode::Internal("Plan node profile not found"))?;
            let prof = PlanNodeProfile {
                id: exchange.plan_id,
                plan_node_name: "Exchange".to_string(),
                description: "".to_string(),
                cpu_time: proc_prof.cpu_time,
            };
            plan_node_profs.push(prof);
        }
        PhysicalPlan::UnionAll(union) => {
            flatten_plan_node_profile(&union.left, profs, plan_node_profs)?;
            flatten_plan_node_profile(&union.right, profs, plan_node_profs)?;
            let proc_prof = profs
                .get(&union.plan_id)
                .ok_or_else(|| ErrorCode::Internal("Plan node profile not found"))?;
            let prof = PlanNodeProfile {
                id: union.plan_id,
                plan_node_name: "UnionAll".to_string(),
                description: "".to_string(),
                cpu_time: proc_prof.cpu_time,
            };
            plan_node_profs.push(prof);
        }
        PhysicalPlan::RuntimeFilterSource(source) => {
            let proc_prof = profs
                .get(&source.plan_id)
                .ok_or_else(|| ErrorCode::Internal("Plan node profile not found"))?;
            let prof = PlanNodeProfile {
                id: source.plan_id,
                plan_node_name: "RuntimeFilterSource".to_string(),
                description: "".to_string(),
                cpu_time: proc_prof.cpu_time,
            };
            plan_node_profs.push(prof);
        }
        PhysicalPlan::DistributedInsertSelect(select) => {
            flatten_plan_node_profile(&select.input, profs, plan_node_profs)?;
            let proc_prof = profs
                .get(&select.plan_id)
                .ok_or_else(|| ErrorCode::Internal("Plan node profile not found"))?;
            let prof = PlanNodeProfile {
                id: select.plan_id,
                plan_node_name: "DistributedInsertSelect".to_string(),
                description: "".to_string(),
                cpu_time: proc_prof.cpu_time,
            };
            plan_node_profs.push(prof);
        }
        PhysicalPlan::ExchangeSource(source) => {
            let proc_prof = profs
                .get(&source.plan_id)
                .ok_or_else(|| ErrorCode::Internal("Plan node profile not found"))?;
            let prof = PlanNodeProfile {
                id: source.plan_id,
                plan_node_name: "ExchangeSource".to_string(),
                description: "".to_string(),
                cpu_time: proc_prof.cpu_time,
            };
            plan_node_profs.push(prof);
        }
        PhysicalPlan::ExchangeSink(sink) => {
            flatten_plan_node_profile(&sink.input, profs, plan_node_profs)?;
            let proc_prof = profs
                .get(&sink.plan_id)
                .ok_or_else(|| ErrorCode::Internal("Plan node profile not found"))?;
            let prof = PlanNodeProfile {
                id: sink.plan_id,
                plan_node_name: "ExchangeSink".to_string(),
                description: "".to_string(),
                cpu_time: proc_prof.cpu_time,
            };
            plan_node_profs.push(prof);
        }
        PhysicalPlan::DeletePartial(_) | PhysicalPlan::DeleteFinal(_) => unreachable!(),
    }

    Ok(())
}
