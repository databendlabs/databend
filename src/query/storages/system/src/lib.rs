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
#![allow(clippy::useless_asref)]
#![feature(type_alias_impl_trait)]
#![feature(impl_trait_in_assoc_type)]
#![feature(variant_count)]

extern crate core;

mod background_jobs_table;
mod background_tasks_table;
mod backtrace_table;
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
mod indexes_table;
mod locks_table;
mod log_queue;
mod malloc_stats_table;
mod malloc_stats_totals_table;
mod metrics_table;
mod notification_history_table;
mod notifications_table;
mod one_table;
mod password_policies_table;
mod processes_table;
mod processor_profile_table;
mod query_cache_table;
mod query_log_table;
mod roles_table;
mod settings_table;
mod stages_table;
mod streams_table;
mod table;
mod table_functions_table;
mod tables_table;
mod task_history_table;
mod tasks_table;
mod temp_files_table;
mod user_functions_table;
mod users_table;
mod util;
mod virtual_columns_table;

pub use background_jobs_table::BackgroundJobTable;
pub use background_tasks_table::BackgroundTaskTable;
pub use backtrace_table::BacktraceTable;
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
pub use indexes_table::IndexesTable;
pub use locks_table::LocksTable;
pub use log_queue::SystemLogElement;
pub use log_queue::SystemLogQueue;
pub use log_queue::SystemLogTable;
pub use malloc_stats_table::MallocStatsTable;
pub use malloc_stats_totals_table::MallocStatsTotalsTable;
pub use metrics_table::MetricsTable;
pub use notification_history_table::NotificationHistoryTable;
pub use notifications_table::parse_notifications_to_datablock;
pub use notifications_table::NotificationsTable;
pub use one_table::OneTable;
pub use password_policies_table::PasswordPoliciesTable;
pub use processes_table::ProcessesTable;
pub use processor_profile_table::ProcessorProfileTable;
pub use query_cache_table::QueryCacheTable;
pub use query_log_table::LogType;
pub use query_log_table::QueryLogElement;
pub use query_log_table::QueryLogQueue;
pub use query_log_table::QueryLogTable;
pub use roles_table::RolesTable;
pub use settings_table::SettingsTable;
pub use stages_table::StagesTable;
pub use streams_table::StreamsTable;
pub use table::SyncOneBlockSystemTable;
pub use table::SyncSystemTable;
pub use table_functions_table::TableFunctionsTable;
pub use tables_table::TablesTable;
pub use tables_table::TablesTableWithHistory;
pub use tables_table::TablesTableWithoutHistory;
pub use tables_table::ViewsTableWithHistory;
pub use tables_table::ViewsTableWithoutHistory;
pub use task_history_table::parse_task_runs_to_datablock;
pub use task_history_table::TaskHistoryTable;
pub use tasks_table::parse_tasks_to_datablock;
pub use tasks_table::TasksTable;
pub use temp_files_table::TempFilesTable;
pub use user_functions_table::UserFunctionsTable;
pub use users_table::UsersTable;
pub use virtual_columns_table::VirtualColumnsTable;
