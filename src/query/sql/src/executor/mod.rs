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

mod explain;
mod format;
mod physical_plan;
mod physical_plan_builder;
mod physical_plan_display;
mod physical_plan_visitor;
mod physical_plans;
mod profile;
pub mod table_read_plan;
mod util;

pub use physical_plan::PhysicalPlan;
pub use physical_plan_builder::PhysicalPlanBuilder;
pub use physical_plan_visitor::PhysicalPlanReplacer;
pub use physical_plans::common::AggregateFunctionDesc;
pub use physical_plans::common::FragmentKind;
pub use physical_plans::common::MutationKind;
pub use physical_plans::common::OnConflictField;
pub use physical_plans::physical_aggregate_expand::AggregateExpand;
pub use physical_plans::physical_aggregate_final::AggregateFinal;
pub use physical_plans::physical_aggregate_partial::AggregatePartial;
pub use physical_plans::physical_async_source::AsyncSourcerPlan;
pub use physical_plans::physical_commit_sink::CommitSink;
pub use physical_plans::physical_compact_source::CompactSource;
pub use physical_plans::physical_constant_table_scan::ConstantTableScan;
pub use physical_plans::physical_copy_into::CopyIntoTablePhysicalPlan;
pub use physical_plans::physical_copy_into::CopyIntoTableSource;
pub use physical_plans::physical_copy_into::QuerySource;
pub use physical_plans::physical_cte_scan::CteScan;
pub use physical_plans::physical_deduplicate::Deduplicate;
pub use physical_plans::physical_deduplicate::SelectCtx;
pub use physical_plans::physical_delete_source::DeleteSource;
pub use physical_plans::physical_distributed_insert_select::DistributedInsertSelect;
pub use physical_plans::physical_eval_scalar::EvalScalar;
pub use physical_plans::physical_exchange::Exchange;
pub use physical_plans::physical_exchange_sink::ExchangeSink;
pub use physical_plans::physical_exchange_source::ExchangeSource;
pub use physical_plans::physical_filter::Filter;
pub use physical_plans::physical_hash_join::HashJoin;
pub use physical_plans::physical_lambda::Lambda;
pub use physical_plans::physical_lambda::LambdaFunctionDesc;
pub use physical_plans::physical_limit::Limit;
pub use physical_plans::physical_materialized_cte::MaterializedCte;
pub use physical_plans::physical_merge_into::MatchExpr;
pub use physical_plans::physical_merge_into::MergeInto;
pub use physical_plans::physical_merge_into::MergeIntoSource;
pub use physical_plans::physical_project::Project;
pub use physical_plans::physical_project_set::ProjectSet;
pub use physical_plans::physical_range_join::RangeJoin;
pub use physical_plans::physical_range_join::RangeJoinCondition;
pub use physical_plans::physical_range_join::RangeJoinType;
pub use physical_plans::physical_replace_into::ReplaceInto;
pub use physical_plans::physical_row_fetch::RowFetch;
pub use physical_plans::physical_runtime_filter_source::RuntimeFilterSource;
pub use physical_plans::physical_sort::Sort;
pub use physical_plans::physical_table_scan::TableScan;
pub use physical_plans::physical_union_all::UnionAll;
pub use physical_plans::physical_window::LagLeadDefault;
pub use physical_plans::physical_window::Window;
pub use physical_plans::physical_window::WindowFunction;
pub use profile::*;
pub use util::*;
