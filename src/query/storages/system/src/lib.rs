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

#![allow(clippy::uninlined_format_args)]
#![feature(type_alias_impl_trait)]

extern crate core;

mod build_options_table;
mod caches_table;
mod catalogs_table;
mod clustering_history_table;
mod clusters_table;
mod columns_table;
mod configs_table;
mod contributors_table;
mod credits_table;
mod databases_table;
mod engines_table;
mod functions_table;
mod log_queue;
mod malloc_stats_table;
mod malloc_stats_totals_table;
mod metrics_table;
mod one_table;
mod processes_table;
mod query_cache_table;
mod query_log_table;
mod roles_table;
mod settings_table;
mod stages_table;
mod table;
mod table_functions_table;
mod tables_table;
mod tracing_table;
mod users_table;

pub use build_options_table::BuildOptionsTable;
pub use caches_table::CachesTable;
pub use catalogs_table::CatalogsTable;
pub use clustering_history_table::ClusteringHistoryLogElement;
pub use clustering_history_table::ClusteringHistoryQueue;
pub use clustering_history_table::ClusteringHistoryTable;
pub use clusters_table::ClustersTable;
pub use columns_table::ColumnsTable;
pub use configs_table::ConfigsTable;
pub use contributors_table::ContributorsTable;
pub use credits_table::CreditsTable;
pub use databases_table::DatabasesTable;
pub use engines_table::EnginesTable;
pub use functions_table::FunctionsTable;
pub use log_queue::SystemLogElement;
pub use log_queue::SystemLogQueue;
pub use log_queue::SystemLogTable;
pub use malloc_stats_table::MallocStatsTable;
pub use malloc_stats_totals_table::MallocStatsTotalsTable;
pub use metrics_table::MetricsTable;
pub use one_table::OneTable;
pub use processes_table::ProcessesTable;
pub use query_cache_table::QueryCacheTable;
pub use query_log_table::LogType;
pub use query_log_table::QueryLogElement;
pub use query_log_table::QueryLogQueue;
pub use query_log_table::QueryLogTable;
pub use roles_table::RolesTable;
pub use settings_table::SettingsTable;
pub use stages_table::StagesTable;
pub use table::SyncOneBlockSystemTable;
pub use table::SyncSystemTable;
pub use table_functions_table::TableFunctionsTable;
pub use tables_table::TablesTable;
pub use tables_table::TablesTableWithHistory;
pub use tables_table::TablesTableWithoutHistory;
pub use tracing_table::TracingTable;
pub use users_table::UsersTable;
