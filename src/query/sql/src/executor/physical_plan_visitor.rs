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

use crate::executor::physical_plans::physical_aggregate_expand::AggregateExpand;
use crate::executor::physical_plans::physical_aggregate_final::AggregateFinal;
use crate::executor::physical_plans::physical_aggregate_partial::AggregatePartial;
use crate::executor::physical_plans::physical_async_source::AsyncSourcerPlan;
use crate::executor::physical_plans::physical_commit_sink::CommitSink;
use crate::executor::physical_plans::physical_compact_source::CompactSource;
use crate::executor::physical_plans::physical_constant_table_scan::ConstantTableScan;
use crate::executor::physical_plans::physical_copy_into::CopyIntoTablePhysicalPlan;
use crate::executor::physical_plans::physical_copy_into::CopyIntoTableSource;
use crate::executor::physical_plans::physical_copy_into::QuerySource;
use crate::executor::physical_plans::physical_cte_scan::CteScan;
use crate::executor::physical_plans::physical_deduplicate::Deduplicate;
use crate::executor::physical_plans::physical_delete_source::DeleteSource;
use crate::executor::physical_plans::physical_distributed_insert_select::DistributedInsertSelect;
use crate::executor::physical_plans::physical_eval_scalar::EvalScalar;
use crate::executor::physical_plans::physical_exchange::Exchange;
use crate::executor::physical_plans::physical_exchange_sink::ExchangeSink;
use crate::executor::physical_plans::physical_exchange_source::ExchangeSource;
use crate::executor::physical_plans::physical_filter::Filter;
use crate::executor::physical_plans::physical_hash_join::HashJoin;
use crate::executor::physical_plans::physical_lambda::Lambda;
use crate::executor::physical_plans::physical_limit::Limit;
use crate::executor::physical_plans::physical_materialized_cte::MaterializedCte;
use crate::executor::physical_plans::physical_merge_into::MergeInto;
use crate::executor::physical_plans::physical_merge_into::MergeIntoSource;
use crate::executor::physical_plans::physical_project::Project;
use crate::executor::physical_plans::physical_project_set::ProjectSet;
use crate::executor::physical_plans::physical_range_join::RangeJoin;
use crate::executor::physical_plans::physical_replace_into::ReplaceInto;
use crate::executor::physical_plans::physical_row_fetch::RowFetch;
use crate::executor::physical_plans::physical_runtime_filter_source::RuntimeFilterSource;
use crate::executor::physical_plans::physical_sort::Sort;
use crate::executor::physical_plans::physical_table_scan::TableScan;
use crate::executor::physical_plans::physical_union_all::UnionAll;
use crate::executor::physical_plans::physical_window::Window;
use crate::executor::PhysicalPlan;

pub trait PhysicalPlanReplacer {
    fn replace(&mut self, plan: &PhysicalPlan) -> Result<PhysicalPlan> {
        match plan {
            PhysicalPlan::TableScan(plan) => self.replace_table_scan(plan),
            PhysicalPlan::CteScan(plan) => self.replace_cte_scan(plan),
            PhysicalPlan::Filter(plan) => self.replace_filter(plan),
            PhysicalPlan::Project(plan) => self.replace_project(plan),
            PhysicalPlan::EvalScalar(plan) => self.replace_eval_scalar(plan),
            PhysicalPlan::AggregateExpand(plan) => self.replace_aggregate_expand(plan),
            PhysicalPlan::AggregatePartial(plan) => self.replace_aggregate_partial(plan),
            PhysicalPlan::AggregateFinal(plan) => self.replace_aggregate_final(plan),
            PhysicalPlan::Window(plan) => self.replace_window(plan),
            PhysicalPlan::Sort(plan) => self.replace_sort(plan),
            PhysicalPlan::Limit(plan) => self.replace_limit(plan),
            PhysicalPlan::RowFetch(plan) => self.replace_row_fetch(plan),
            PhysicalPlan::HashJoin(plan) => self.replace_hash_join(plan),
            PhysicalPlan::Exchange(plan) => self.replace_exchange(plan),
            PhysicalPlan::ExchangeSource(plan) => self.replace_exchange_source(plan),
            PhysicalPlan::ExchangeSink(plan) => self.replace_exchange_sink(plan),
            PhysicalPlan::UnionAll(plan) => self.replace_union(plan),
            PhysicalPlan::DistributedInsertSelect(plan) => self.replace_insert_select(plan),
            PhysicalPlan::ProjectSet(plan) => self.replace_project_set(plan),
            PhysicalPlan::Lambda(plan) => self.replace_lambda(plan),
            PhysicalPlan::RuntimeFilterSource(plan) => self.replace_runtime_filter_source(plan),
            PhysicalPlan::CompactSource(plan) => self.replace_compact_source(plan),
            PhysicalPlan::DeleteSource(plan) => self.replace_delete_source(plan),
            PhysicalPlan::CommitSink(plan) => self.replace_commit_sink(plan),
            PhysicalPlan::RangeJoin(plan) => self.replace_range_join(plan),
            PhysicalPlan::CopyIntoTable(plan) => self.replace_copy_into_table(plan),
            PhysicalPlan::AsyncSourcer(plan) => self.replace_async_sourcer(plan),
            PhysicalPlan::Deduplicate(plan) => self.replace_deduplicate(plan),
            PhysicalPlan::ReplaceInto(plan) => self.replace_replace_into(plan),
            PhysicalPlan::MergeInto(plan) => self.replace_merge_into(plan),
            PhysicalPlan::MergeIntoSource(plan) => self.replace_merge_into_source(plan),
            PhysicalPlan::MaterializedCte(plan) => self.replace_materialized_cte(plan),
            PhysicalPlan::ConstantTableScan(plan) => self.replace_constant_table_scan(plan),
        }
    }

    fn replace_table_scan(&mut self, plan: &TableScan) -> Result<PhysicalPlan> {
        Ok(PhysicalPlan::TableScan(plan.clone()))
    }

    fn replace_cte_scan(&mut self, plan: &CteScan) -> Result<PhysicalPlan> {
        Ok(PhysicalPlan::CteScan(plan.clone()))
    }

    fn replace_constant_table_scan(&mut self, plan: &ConstantTableScan) -> Result<PhysicalPlan> {
        Ok(PhysicalPlan::ConstantTableScan(plan.clone()))
    }

    fn replace_filter(&mut self, plan: &Filter) -> Result<PhysicalPlan> {
        let input = self.replace(&plan.input)?;

        Ok(PhysicalPlan::Filter(Filter {
            plan_id: plan.plan_id,
            projections: plan.projections.clone(),
            input: Box::new(input),
            predicates: plan.predicates.clone(),
            stat_info: plan.stat_info.clone(),
        }))
    }

    fn replace_project(&mut self, plan: &Project) -> Result<PhysicalPlan> {
        let input = self.replace(&plan.input)?;

        Ok(PhysicalPlan::Project(Project {
            plan_id: plan.plan_id,
            input: Box::new(input),
            projections: plan.projections.clone(),
            columns: plan.columns.clone(),
            stat_info: plan.stat_info.clone(),
        }))
    }

    fn replace_eval_scalar(&mut self, plan: &EvalScalar) -> Result<PhysicalPlan> {
        let input = self.replace(&plan.input)?;

        Ok(PhysicalPlan::EvalScalar(EvalScalar {
            plan_id: plan.plan_id,
            projections: plan.projections.clone(),
            input: Box::new(input),
            exprs: plan.exprs.clone(),
            stat_info: plan.stat_info.clone(),
        }))
    }

    fn replace_aggregate_expand(&mut self, plan: &AggregateExpand) -> Result<PhysicalPlan> {
        let input = self.replace(&plan.input)?;

        Ok(PhysicalPlan::AggregateExpand(AggregateExpand {
            plan_id: plan.plan_id,
            input: Box::new(input),
            group_bys: plan.group_bys.clone(),
            grouping_sets: plan.grouping_sets.clone(),
            stat_info: plan.stat_info.clone(),
        }))
    }

    fn replace_aggregate_partial(&mut self, plan: &AggregatePartial) -> Result<PhysicalPlan> {
        let input = self.replace(&plan.input)?;

        Ok(PhysicalPlan::AggregatePartial(AggregatePartial {
            plan_id: plan.plan_id,
            input: Box::new(input),
            group_by: plan.group_by.clone(),
            agg_funcs: plan.agg_funcs.clone(),
            stat_info: plan.stat_info.clone(),
        }))
    }

    fn replace_aggregate_final(&mut self, plan: &AggregateFinal) -> Result<PhysicalPlan> {
        let input = self.replace(&plan.input)?;

        Ok(PhysicalPlan::AggregateFinal(AggregateFinal {
            plan_id: plan.plan_id,
            input: Box::new(input),
            before_group_by_schema: plan.before_group_by_schema.clone(),
            group_by: plan.group_by.clone(),
            agg_funcs: plan.agg_funcs.clone(),
            stat_info: plan.stat_info.clone(),
            limit: plan.limit,
        }))
    }

    fn replace_window(&mut self, plan: &Window) -> Result<PhysicalPlan> {
        let input = self.replace(&plan.input)?;

        Ok(PhysicalPlan::Window(Window {
            plan_id: plan.plan_id,
            index: plan.index,
            input: Box::new(input),
            func: plan.func.clone(),
            partition_by: plan.partition_by.clone(),
            order_by: plan.order_by.clone(),
            window_frame: plan.window_frame.clone(),
        }))
    }

    fn replace_hash_join(&mut self, plan: &HashJoin) -> Result<PhysicalPlan> {
        let build = self.replace(&plan.build)?;
        let probe = self.replace(&plan.probe)?;

        Ok(PhysicalPlan::HashJoin(HashJoin {
            plan_id: plan.plan_id,
            projections: plan.projections.clone(),
            probe_projections: plan.probe_projections.clone(),
            build_projections: plan.build_projections.clone(),
            build: Box::new(build),
            probe: Box::new(probe),
            build_keys: plan.build_keys.clone(),
            probe_keys: plan.probe_keys.clone(),
            non_equi_conditions: plan.non_equi_conditions.clone(),
            join_type: plan.join_type.clone(),
            marker_index: plan.marker_index,
            from_correlated_subquery: plan.from_correlated_subquery,
            probe_to_build: plan.probe_to_build.clone(),
            output_schema: plan.output_schema.clone(),
            contain_runtime_filter: plan.contain_runtime_filter,
            stat_info: plan.stat_info.clone(),
        }))
    }

    fn replace_materialized_cte(&mut self, plan: &MaterializedCte) -> Result<PhysicalPlan> {
        let left = self.replace(&plan.left)?;
        let right = self.replace(&plan.right)?;

        Ok(PhysicalPlan::MaterializedCte(MaterializedCte {
            plan_id: plan.plan_id,
            left: Box::new(left),
            right: Box::new(right),
            cte_idx: plan.cte_idx,
            left_output_columns: plan.left_output_columns.clone(),
        }))
    }

    fn replace_range_join(&mut self, plan: &RangeJoin) -> Result<PhysicalPlan> {
        let left = self.replace(&plan.left)?;
        let right = self.replace(&plan.right)?;

        Ok(PhysicalPlan::RangeJoin(RangeJoin {
            plan_id: plan.plan_id,
            left: Box::new(left),
            right: Box::new(right),
            conditions: plan.conditions.clone(),
            other_conditions: plan.other_conditions.clone(),
            join_type: plan.join_type.clone(),
            range_join_type: plan.range_join_type.clone(),
            stat_info: plan.stat_info.clone(),
        }))
    }

    fn replace_sort(&mut self, plan: &Sort) -> Result<PhysicalPlan> {
        let input = self.replace(&plan.input)?;

        Ok(PhysicalPlan::Sort(Sort {
            plan_id: plan.plan_id,
            input: Box::new(input),
            order_by: plan.order_by.clone(),
            limit: plan.limit,
            after_exchange: plan.after_exchange,
            pre_projection: plan.pre_projection.clone(),
            stat_info: plan.stat_info.clone(),
        }))
    }

    fn replace_limit(&mut self, plan: &Limit) -> Result<PhysicalPlan> {
        let input = self.replace(&plan.input)?;

        Ok(PhysicalPlan::Limit(Limit {
            plan_id: plan.plan_id,
            input: Box::new(input),
            limit: plan.limit,
            offset: plan.offset,
            stat_info: plan.stat_info.clone(),
        }))
    }

    fn replace_row_fetch(&mut self, plan: &RowFetch) -> Result<PhysicalPlan> {
        let input = self.replace(&plan.input)?;

        Ok(PhysicalPlan::RowFetch(RowFetch {
            plan_id: plan.plan_id,
            input: Box::new(input),
            source: plan.source.clone(),
            row_id_col_offset: plan.row_id_col_offset,
            cols_to_fetch: plan.cols_to_fetch.clone(),
            fetched_fields: plan.fetched_fields.clone(),
            stat_info: plan.stat_info.clone(),
        }))
    }

    fn replace_exchange(&mut self, plan: &Exchange) -> Result<PhysicalPlan> {
        let input = self.replace(&plan.input)?;

        Ok(PhysicalPlan::Exchange(Exchange {
            plan_id: plan.plan_id,
            input: Box::new(input),
            kind: plan.kind.clone(),
            keys: plan.keys.clone(),
            ignore_exchange: plan.ignore_exchange,
        }))
    }

    fn replace_exchange_source(&mut self, plan: &ExchangeSource) -> Result<PhysicalPlan> {
        Ok(PhysicalPlan::ExchangeSource(plan.clone()))
    }

    fn replace_exchange_sink(&mut self, plan: &ExchangeSink) -> Result<PhysicalPlan> {
        let input = self.replace(&plan.input)?;

        Ok(PhysicalPlan::ExchangeSink(ExchangeSink {
            // TODO(leiysky): we reuse the plan id of the Exchange node here,
            // should generate a new one.
            plan_id: plan.plan_id,

            input: Box::new(input),
            schema: plan.schema.clone(),
            kind: plan.kind.clone(),
            keys: plan.keys.clone(),
            destination_fragment_id: plan.destination_fragment_id,
            query_id: plan.query_id.clone(),
            ignore_exchange: plan.ignore_exchange,
        }))
    }

    fn replace_union(&mut self, plan: &UnionAll) -> Result<PhysicalPlan> {
        let left = self.replace(&plan.left)?;
        let right = self.replace(&plan.right)?;
        Ok(PhysicalPlan::UnionAll(UnionAll {
            plan_id: plan.plan_id,
            left: Box::new(left),
            right: Box::new(right),
            schema: plan.schema.clone(),
            pairs: plan.pairs.clone(),
            stat_info: plan.stat_info.clone(),
        }))
    }

    fn replace_copy_into_table(
        &mut self,
        plan: &CopyIntoTablePhysicalPlan,
    ) -> Result<PhysicalPlan> {
        match &plan.source {
            CopyIntoTableSource::Stage(_) => {
                Ok(PhysicalPlan::CopyIntoTable(Box::new(plan.clone())))
            }
            CopyIntoTableSource::Query(query_ctx) => {
                let input = self.replace(&query_ctx.plan)?;
                Ok(PhysicalPlan::CopyIntoTable(Box::new(
                    CopyIntoTablePhysicalPlan {
                        source: CopyIntoTableSource::Query(Box::new(QuerySource {
                            plan: input,
                            ..*query_ctx.clone()
                        })),
                        ..plan.clone()
                    },
                )))
            }
        }
    }

    fn replace_insert_select(&mut self, plan: &DistributedInsertSelect) -> Result<PhysicalPlan> {
        let input = self.replace(&plan.input)?;

        Ok(PhysicalPlan::DistributedInsertSelect(Box::new(
            DistributedInsertSelect {
                plan_id: plan.plan_id,
                input: Box::new(input),
                catalog_info: plan.catalog_info.clone(),
                table_info: plan.table_info.clone(),
                select_schema: plan.select_schema.clone(),
                insert_schema: plan.insert_schema.clone(),
                select_column_bindings: plan.select_column_bindings.clone(),
                cast_needed: plan.cast_needed,
            },
        )))
    }

    fn replace_compact_source(&mut self, plan: &CompactSource) -> Result<PhysicalPlan> {
        Ok(PhysicalPlan::CompactSource(Box::new(plan.clone())))
    }

    fn replace_delete_source(&mut self, plan: &DeleteSource) -> Result<PhysicalPlan> {
        Ok(PhysicalPlan::DeleteSource(Box::new(plan.clone())))
    }

    fn replace_commit_sink(&mut self, plan: &CommitSink) -> Result<PhysicalPlan> {
        let input = self.replace(&plan.input)?;
        Ok(PhysicalPlan::CommitSink(Box::new(CommitSink {
            input: Box::new(input),
            ..plan.clone()
        })))
    }

    fn replace_async_sourcer(&mut self, plan: &AsyncSourcerPlan) -> Result<PhysicalPlan> {
        Ok(PhysicalPlan::AsyncSourcer(plan.clone()))
    }

    fn replace_deduplicate(&mut self, plan: &Deduplicate) -> Result<PhysicalPlan> {
        let input = self.replace(&plan.input)?;
        Ok(PhysicalPlan::Deduplicate(Box::new(Deduplicate {
            input: Box::new(input),
            ..plan.clone()
        })))
    }

    fn replace_replace_into(&mut self, plan: &ReplaceInto) -> Result<PhysicalPlan> {
        let input = self.replace(&plan.input)?;
        Ok(PhysicalPlan::ReplaceInto(Box::new(ReplaceInto {
            input: Box::new(input),
            ..plan.clone()
        })))
    }

    fn replace_merge_into(&mut self, plan: &MergeInto) -> Result<PhysicalPlan> {
        let input = self.replace(&plan.input)?;
        Ok(PhysicalPlan::MergeInto(Box::new(MergeInto {
            input: Box::new(input),
            ..plan.clone()
        })))
    }

    fn replace_merge_into_source(&mut self, plan: &MergeIntoSource) -> Result<PhysicalPlan> {
        let input = self.replace(&plan.input)?;
        Ok(PhysicalPlan::MergeIntoSource(MergeIntoSource {
            input: Box::new(input),
            ..plan.clone()
        }))
    }

    fn replace_project_set(&mut self, plan: &ProjectSet) -> Result<PhysicalPlan> {
        let input = self.replace(&plan.input)?;
        Ok(PhysicalPlan::ProjectSet(ProjectSet {
            plan_id: plan.plan_id,
            input: Box::new(input),
            srf_exprs: plan.srf_exprs.clone(),
            projections: plan.projections.clone(),
            stat_info: plan.stat_info.clone(),
        }))
    }

    fn replace_lambda(&mut self, plan: &Lambda) -> Result<PhysicalPlan> {
        let input = self.replace(&plan.input)?;
        Ok(PhysicalPlan::Lambda(Lambda {
            plan_id: plan.plan_id,
            input: Box::new(input),
            lambda_funcs: plan.lambda_funcs.clone(),
            stat_info: plan.stat_info.clone(),
        }))
    }

    fn replace_runtime_filter_source(
        &mut self,
        plan: &RuntimeFilterSource,
    ) -> Result<PhysicalPlan> {
        let left_side = self.replace(&plan.left_side)?;
        let right_side = self.replace(&plan.right_side)?;
        Ok(PhysicalPlan::RuntimeFilterSource(RuntimeFilterSource {
            plan_id: plan.plan_id,
            left_side: Box::new(left_side),
            right_side: Box::new(right_side),
            left_runtime_filters: plan.left_runtime_filters.clone(),
            right_runtime_filters: plan.right_runtime_filters.clone(),
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
                PhysicalPlan::TableScan(_)
                | PhysicalPlan::AsyncSourcer(_)
                | PhysicalPlan::CteScan(_)
                | PhysicalPlan::ConstantTableScan(_) => {}
                PhysicalPlan::Filter(plan) => {
                    Self::traverse(&plan.input, pre_visit, visit, post_visit);
                }
                PhysicalPlan::Project(plan) => {
                    Self::traverse(&plan.input, pre_visit, visit, post_visit);
                }
                PhysicalPlan::EvalScalar(plan) => {
                    Self::traverse(&plan.input, pre_visit, visit, post_visit);
                }
                PhysicalPlan::AggregateExpand(plan) => {
                    Self::traverse(&plan.input, pre_visit, visit, post_visit);
                }
                PhysicalPlan::AggregatePartial(plan) => {
                    Self::traverse(&plan.input, pre_visit, visit, post_visit);
                }
                PhysicalPlan::AggregateFinal(plan) => {
                    Self::traverse(&plan.input, pre_visit, visit, post_visit);
                }
                PhysicalPlan::Window(plan) => {
                    Self::traverse(&plan.input, pre_visit, visit, post_visit);
                }
                PhysicalPlan::Sort(plan) => {
                    Self::traverse(&plan.input, pre_visit, visit, post_visit);
                }
                PhysicalPlan::Limit(plan) => {
                    Self::traverse(&plan.input, pre_visit, visit, post_visit);
                }
                PhysicalPlan::RowFetch(plan) => {
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
                PhysicalPlan::DistributedInsertSelect(plan) => {
                    Self::traverse(&plan.input, pre_visit, visit, post_visit);
                }
                PhysicalPlan::ProjectSet(plan) => {
                    Self::traverse(&plan.input, pre_visit, visit, post_visit)
                }
                PhysicalPlan::Lambda(plan) => {
                    Self::traverse(&plan.input, pre_visit, visit, post_visit)
                }
                PhysicalPlan::CopyIntoTable(plan) => match &plan.source {
                    CopyIntoTableSource::Query(input) => {
                        Self::traverse(&input.plan, pre_visit, visit, post_visit);
                    }
                    CopyIntoTableSource::Stage(_) => {}
                },
                PhysicalPlan::RuntimeFilterSource(plan) => {
                    Self::traverse(&plan.left_side, pre_visit, visit, post_visit);
                    Self::traverse(&plan.right_side, pre_visit, visit, post_visit);
                }
                PhysicalPlan::RangeJoin(plan) => {
                    Self::traverse(&plan.left, pre_visit, visit, post_visit);
                    Self::traverse(&plan.right, pre_visit, visit, post_visit);
                }
                PhysicalPlan::CompactSource(_) => {}
                PhysicalPlan::DeleteSource(_) => {}
                PhysicalPlan::CommitSink(plan) => {
                    Self::traverse(&plan.input, pre_visit, visit, post_visit);
                }
                PhysicalPlan::Deduplicate(plan) => {
                    Self::traverse(&plan.input, pre_visit, visit, post_visit);
                }
                PhysicalPlan::ReplaceInto(plan) => {
                    Self::traverse(&plan.input, pre_visit, visit, post_visit);
                }
                PhysicalPlan::MergeIntoSource(plan) => {
                    Self::traverse(&plan.input, pre_visit, visit, post_visit);
                }
                PhysicalPlan::MergeInto(plan) => {
                    Self::traverse(&plan.input, pre_visit, visit, post_visit);
                }
                PhysicalPlan::MaterializedCte(plan) => {
                    Self::traverse(&plan.left, pre_visit, visit, post_visit);
                    Self::traverse(&plan.right, pre_visit, visit, post_visit);
                }
            }
            post_visit(plan);
        }
    }
}
