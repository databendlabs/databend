// Copyright 2020 Datafuse Labs.
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

pub use clusters_table::ClustersTable;
pub use columns_table::ColumnsTable;
pub use configs_table::ConfigsTable;
pub use contributors_table::ContributorsTable;
pub use credits_table::CreditsTable;
pub use databases_table::DatabasesTable;
pub use functions_table::FunctionsTable;
pub use metrics_table::MetricsTable;
pub use one_table::OneTable;
pub use processes_table::ProcessesTable;
pub use settings_table::SettingsTable;
pub use system_database::SystemDatabase;
pub use tables_table::TablesTable;
pub use tracing_table::TracingTable;
pub use tracing_table_stream::TracingTableStream;

#[cfg(test)]
mod clusters_table_test;
#[cfg(test)]
mod columns_table_test;
#[cfg(test)]
mod configs_table_test;
#[cfg(test)]
mod contributors_table_test;
#[cfg(test)]
mod credits_table_test;
#[cfg(test)]
mod databases_table_test;
#[cfg(test)]
mod functions_table_test;
#[cfg(test)]
mod metrics_table_test;
#[cfg(test)]
mod settings_table_test;
#[cfg(test)]
mod tables_table_test;
#[cfg(test)]
mod tracing_table_test;

mod clusters_table;
mod columns_table;
mod configs_table;
mod contributors_table;
mod credits_table;
mod databases_table;
mod functions_table;
mod metrics_table;
mod one_table;
mod processes_table;
mod settings_table;
mod system_database;
mod tables_table;
mod tracing_table;
mod tracing_table_stream;

// TODO introduce A "base" type VTable, to de-duplicate codes of system tables
