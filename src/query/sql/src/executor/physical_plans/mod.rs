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

pub mod common;
pub mod physical_aggregate_expand;
pub mod physical_aggregate_final;
pub mod physical_aggregate_partial;
pub mod physical_async_source;
pub mod physical_commit_sink;
pub mod physical_compact_source;
pub mod physical_constant_table_scan;
pub mod physical_copy_into;
pub mod physical_cte_scan;
pub mod physical_deduplicate;
pub mod physical_delete_source;
pub mod physical_distributed_insert_select;
pub mod physical_eval_scalar;
pub mod physical_exchange;
pub mod physical_exchange_sink;
pub mod physical_exchange_source;
pub mod physical_filter;
pub mod physical_hash_join;
pub mod physical_join;
pub mod physical_lambda;
pub mod physical_limit;
pub mod physical_materialized_cte;
pub mod physical_merge_into;
pub mod physical_project;
pub mod physical_project_set;
pub mod physical_range_join;
pub mod physical_refresh_index;
pub mod physical_replace_into;
pub mod physical_row_fetch;
pub mod physical_runtime_filter_source;
pub mod physical_sort;
pub mod physical_table_scan;
pub mod physical_union_all;
pub mod physical_window;
