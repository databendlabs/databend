// Copyright 2021 Datafuse Labs.
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

use super::AggregateFinal;
use super::AggregatePartial;
use super::EvalScalar;
use super::Exchange;
use super::ExchangeSink;
use super::ExchangeSource;
use super::Filter;
use super::HashJoin;
use super::Limit;
use super::PhysicalPlan;
use super::Project;
use super::Sort;
use super::TableScan;
use crate::sql::executor::UnionAll;

pub trait PhysicalPlanReplacer {
    fn replace(&mut self, plan: &PhysicalPlan) -> Result<PhysicalPlan> {
        match plan {
            PhysicalPlan::TableScan(plan) => self.replace_table_scan(plan),
            PhysicalPlan::Filter(plan) => self.replace_filter(plan),
            PhysicalPlan::Project(plan) => self.replace_project(plan),
            PhysicalPlan::EvalScalar(plan) => self.replace_eval_scalar(plan),
            PhysicalPlan::AggregatePartial(plan) => self.replace_aggregate_partial(plan),
            PhysicalPlan::AggregateFinal(plan) => self.replace_aggregate_final(plan),
            PhysicalPlan::Sort(plan) => self.replace_sort(plan),
            PhysicalPlan::Limit(plan) => self.replace_limit(plan),
            PhysicalPlan::HashJoin(plan) => self.replace_hash_join(plan),
            PhysicalPlan::Exchange(plan) => self.replace_exchange(plan),
            PhysicalPlan::ExchangeSource(plan) => self.replace_exchange_source(plan),
            PhysicalPlan::ExchangeSink(plan) => self.replace_exchange_sink(plan),
            PhysicalPlan::UnionAll(plan) => self.replace_union(plan),
        }
    }

    fn replace_table_scan(&mut self, plan: &TableScan) -> Result<PhysicalPlan> {
        Ok(PhysicalPlan::TableScan(plan.clone()))
    }

    fn replace_filter(&mut self, plan: &Filter) -> Result<PhysicalPlan> {
        let input = self.replace(&plan.input)?;

        Ok(PhysicalPlan::Filter(Filter {
            input: Box::new(input),
            predicates: plan.predicates.clone(),
        }))
    }

    fn replace_project(&mut self, plan: &Project) -> Result<PhysicalPlan> {
        let input = self.replace(&plan.input)?;

        Ok(PhysicalPlan::Project(Project {
            input: Box::new(input),
            projections: plan.projections.clone(),
            columns: plan.columns.clone(),
        }))
    }

    fn replace_eval_scalar(&mut self, plan: &EvalScalar) -> Result<PhysicalPlan> {
        let input = self.replace(&plan.input)?;

        Ok(PhysicalPlan::EvalScalar(EvalScalar {
            input: Box::new(input),
            scalars: plan.scalars.clone(),
        }))
    }

    fn replace_aggregate_partial(&mut self, plan: &AggregatePartial) -> Result<PhysicalPlan> {
        let input = self.replace(&plan.input)?;

        Ok(PhysicalPlan::AggregatePartial(AggregatePartial {
            input: Box::new(input),
            group_by: plan.group_by.clone(),
            agg_funcs: plan.agg_funcs.clone(),
        }))
    }

    fn replace_aggregate_final(&mut self, plan: &AggregateFinal) -> Result<PhysicalPlan> {
        let input = self.replace(&plan.input)?;

        Ok(PhysicalPlan::AggregateFinal(AggregateFinal {
            input: Box::new(input),
            before_group_by_schema: plan.before_group_by_schema.clone(),
            group_by: plan.group_by.clone(),
            agg_funcs: plan.agg_funcs.clone(),
        }))
    }

    fn replace_hash_join(&mut self, plan: &HashJoin) -> Result<PhysicalPlan> {
        let build = self.replace(&plan.build)?;
        let probe = self.replace(&plan.probe)?;

        Ok(PhysicalPlan::HashJoin(HashJoin {
            build: Box::new(build),
            probe: Box::new(probe),
            build_keys: plan.build_keys.clone(),
            probe_keys: plan.probe_keys.clone(),
            other_conditions: plan.other_conditions.clone(),
            join_type: plan.join_type.clone(),
            marker_index: plan.marker_index,
            from_correlated_subquery: plan.from_correlated_subquery,
        }))
    }

    fn replace_sort(&mut self, plan: &Sort) -> Result<PhysicalPlan> {
        let input = self.replace(&plan.input)?;

        Ok(PhysicalPlan::Sort(Sort {
            input: Box::new(input),
            order_by: plan.order_by.clone(),
        }))
    }

    fn replace_limit(&mut self, plan: &Limit) -> Result<PhysicalPlan> {
        let input = self.replace(&plan.input)?;

        Ok(PhysicalPlan::Limit(Limit {
            input: Box::new(input),
            limit: plan.limit,
            offset: plan.offset,
        }))
    }

    fn replace_exchange(&mut self, plan: &Exchange) -> Result<PhysicalPlan> {
        let input = self.replace(&plan.input)?;

        Ok(PhysicalPlan::Exchange(Exchange {
            input: Box::new(input),
            kind: plan.kind.clone(),
            keys: plan.keys.clone(),
        }))
    }

    fn replace_exchange_source(&mut self, plan: &ExchangeSource) -> Result<PhysicalPlan> {
        Ok(PhysicalPlan::ExchangeSource(plan.clone()))
    }

    fn replace_exchange_sink(&mut self, plan: &ExchangeSink) -> Result<PhysicalPlan> {
        let input = self.replace(&plan.input)?;

        Ok(PhysicalPlan::ExchangeSink(ExchangeSink {
            input: Box::new(input),
            schema: plan.schema.clone(),
            kind: plan.kind.clone(),
            keys: plan.keys.clone(),
            destination_fragment_id: plan.destination_fragment_id,
            destinations: plan.destinations.clone(),
            query_id: plan.query_id.clone(),
        }))
    }

    fn replace_union(&mut self, plan: &UnionAll) -> Result<PhysicalPlan> {
        let left = self.replace(&plan.left)?;
        let right = self.replace(&plan.right)?;
        Ok(PhysicalPlan::UnionAll(UnionAll {
            left: Box::new(left),
            right: Box::new(right),
            schema: plan.schema.clone(),
        }))
    }
}

impl PhysicalPlan {
    pub fn traverse<'a, 'b>(
        plan: &'a PhysicalPlan,
        pre_visit: &'b mut dyn FnMut(&'a PhysicalPlan) -> bool,
        visit: &'b mut dyn FnMut(&'a PhysicalPlan),
        post_visit: &'b mut dyn FnMut(&'a PhysicalPlan),
    ) {
        if pre_visit(plan) {
            visit(plan);
            match plan {
                PhysicalPlan::TableScan(_) => {}
                PhysicalPlan::Filter(plan) => {
                    Self::traverse(&plan.input, pre_visit, visit, post_visit);
                }
                PhysicalPlan::Project(plan) => {
                    Self::traverse(&plan.input, pre_visit, visit, post_visit);
                }
                PhysicalPlan::EvalScalar(plan) => {
                    Self::traverse(&plan.input, pre_visit, visit, post_visit);
                }
                PhysicalPlan::AggregatePartial(plan) => {
                    Self::traverse(&plan.input, pre_visit, visit, post_visit);
                }
                PhysicalPlan::AggregateFinal(plan) => {
                    Self::traverse(&plan.input, pre_visit, visit, post_visit);
                }
                PhysicalPlan::Sort(plan) => {
                    Self::traverse(&plan.input, pre_visit, visit, post_visit);
                }
                PhysicalPlan::Limit(plan) => {
                    Self::traverse(&plan.input, pre_visit, visit, post_visit);
                }
                PhysicalPlan::HashJoin(plan) => {
                    Self::traverse(&plan.build, pre_visit, visit, post_visit);
                    Self::traverse(&plan.probe, pre_visit, visit, post_visit);
                }
                PhysicalPlan::Exchange(plan) => {
                    Self::traverse(&plan.input, pre_visit, visit, post_visit);
                }
                PhysicalPlan::ExchangeSource(_) => {}
                PhysicalPlan::ExchangeSink(plan) => {
                    Self::traverse(&plan.input, pre_visit, visit, post_visit);
                }
                PhysicalPlan::UnionAll(plan) => {
                    Self::traverse(&plan.left, pre_visit, visit, post_visit);
                    Self::traverse(&plan.right, pre_visit, visit, post_visit);
                }
            }
            post_visit(plan);
        }
    }
}
