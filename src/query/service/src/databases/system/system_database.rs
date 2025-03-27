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

use std::collections::HashMap;
use std::sync::Arc;

use databend_common_config::InnerConfig;
use databend_common_meta_app::schema::database_name_ident::DatabaseNameIdent;
use databend_common_meta_app::schema::DatabaseId;
use databend_common_meta_app::schema::DatabaseInfo;
use databend_common_meta_app::schema::DatabaseMeta;
use databend_common_meta_app::tenant::Tenant;
use databend_common_meta_types::seq_value::SeqV;
use databend_common_storages_system::BackgroundJobTable;
use databend_common_storages_system::BackgroundTaskTable;
use databend_common_storages_system::BacktraceTable;
use databend_common_storages_system::BuildOptionsTable;
use databend_common_storages_system::CachesTable;
use databend_common_storages_system::CatalogsTable;
use databend_common_storages_system::ClusteringHistoryTable;
use databend_common_storages_system::ClustersTable;
use databend_common_storages_system::ColumnsTable;
use databend_common_storages_system::ConfigsTable;
use databend_common_storages_system::ContributorsTable;
use databend_common_storages_system::CreditsTable;
use databend_common_storages_system::DatabasesTableWithHistory;
use databend_common_storages_system::DatabasesTableWithoutHistory;
use databend_common_storages_system::DictionariesTable;
use databend_common_storages_system::EnginesTable;
use databend_common_storages_system::FullStreamsTable;
use databend_common_storages_system::FunctionsTable;
use databend_common_storages_system::IndexesTable;
use databend_common_storages_system::LocksTable;
#[cfg(feature = "jemalloc")]
use databend_common_storages_system::MallocStatsTable;
#[cfg(feature = "jemalloc")]
use databend_common_storages_system::MallocStatsTotalsTable;
use databend_common_storages_system::MetricsTable;
use databend_common_storages_system::NotificationHistoryTable;
use databend_common_storages_system::NotificationsTable;
use databend_common_storages_system::OneTable;
use databend_common_storages_system::PasswordPoliciesTable;
use databend_common_storages_system::ProceduresTable;
use databend_common_storages_system::ProcessesTable;
use databend_common_storages_system::QueriesProfilingTable;
use databend_common_storages_system::QueryCacheTable;
use databend_common_storages_system::QueryLogTable;
use databend_common_storages_system::RolesTable;
use databend_common_storages_system::SettingsTable;
use databend_common_storages_system::StagesTable;
use databend_common_storages_system::TableFunctionsTable;
use databend_common_storages_system::TablesTableWithHistory;
use databend_common_storages_system::TablesTableWithoutHistory;
use databend_common_storages_system::TaskHistoryTable;
use databend_common_storages_system::TasksTable;
use databend_common_storages_system::TempFilesTable;
use databend_common_storages_system::TemporaryTablesTable;
use databend_common_storages_system::TerseStreamsTable;
use databend_common_storages_system::UserFunctionsTable;
use databend_common_storages_system::UsersTable;
use databend_common_storages_system::ViewsTableWithHistory;
use databend_common_storages_system::ViewsTableWithoutHistory;
use databend_common_storages_system::VirtualColumnsTable;

use crate::catalogs::InMemoryMetas;
use crate::databases::Database;
use crate::storages::Table;

#[derive(Clone)]
pub struct SystemDatabase {
    db_info: DatabaseInfo,
}

impl SystemDatabase {
    /// These tables may disabled to the sql users.
    fn disable_system_tables() -> HashMap<String, bool> {
        let mut map = HashMap::new();
        map.insert("configs".to_string(), true);
        map.insert("tracing".to_string(), true);
        map.insert("clusters".to_string(), true);
        map.insert("malloc_stats".to_string(), true);
        map.insert("build_options".to_string(), true);
        map
    }

    pub fn create(sys_db_meta: &mut InMemoryMetas, config: &InnerConfig) -> Self {
        let table_list: Vec<Arc<dyn Table>> = vec![
            OneTable::create(sys_db_meta.next_table_id()),
            FunctionsTable::create(sys_db_meta.next_table_id()),
            ContributorsTable::create(sys_db_meta.next_table_id()),
            CreditsTable::create(sys_db_meta.next_table_id()),
            SettingsTable::create(sys_db_meta.next_table_id()),
            TablesTableWithoutHistory::create(sys_db_meta.next_table_id()),
            TablesTableWithHistory::create(sys_db_meta.next_table_id()),
            ClustersTable::create(sys_db_meta.next_table_id()),
            DatabasesTableWithHistory::create(sys_db_meta.next_table_id()),
            DatabasesTableWithoutHistory::create(sys_db_meta.next_table_id()),
            FullStreamsTable::create(sys_db_meta.next_table_id()),
            TerseStreamsTable::create(sys_db_meta.next_table_id()),
            ProcessesTable::create(sys_db_meta.next_table_id()),
            ConfigsTable::create(sys_db_meta.next_table_id()),
            MetricsTable::create(sys_db_meta.next_table_id()),
            #[cfg(feature = "jemalloc")]
            MallocStatsTable::create(sys_db_meta.next_table_id()),
            #[cfg(feature = "jemalloc")]
            MallocStatsTotalsTable::create(sys_db_meta.next_table_id()),
            ColumnsTable::create(sys_db_meta.next_table_id()),
            UsersTable::create(sys_db_meta.next_table_id()),
            Arc::new(QueryLogTable::create(
                sys_db_meta.next_table_id(),
                config.query.max_query_log_size,
            )),
            Arc::new(ClusteringHistoryTable::create(
                sys_db_meta.next_table_id(),
                config.query.max_query_log_size,
            )),
            EnginesTable::create(sys_db_meta.next_table_id()),
            RolesTable::create(sys_db_meta.next_table_id()),
            StagesTable::create(sys_db_meta.next_table_id()),
            BuildOptionsTable::create(sys_db_meta.next_table_id()),
            CatalogsTable::create(sys_db_meta.next_table_id()),
            QueryCacheTable::create(sys_db_meta.next_table_id()),
            TableFunctionsTable::create(sys_db_meta.next_table_id()),
            CachesTable::create(sys_db_meta.next_table_id()),
            IndexesTable::create(sys_db_meta.next_table_id()),
            BackgroundTaskTable::create(sys_db_meta.next_table_id()),
            BackgroundJobTable::create(sys_db_meta.next_table_id()),
            BacktraceTable::create(sys_db_meta.next_table_id()),
            TempFilesTable::create(sys_db_meta.next_table_id()),
            TasksTable::create(sys_db_meta.next_table_id()),
            TaskHistoryTable::create(sys_db_meta.next_table_id()),
            QueriesProfilingTable::create(sys_db_meta.next_table_id()),
            LocksTable::create(sys_db_meta.next_table_id()),
            VirtualColumnsTable::create(sys_db_meta.next_table_id()),
            PasswordPoliciesTable::create(sys_db_meta.next_table_id()),
            UserFunctionsTable::create(sys_db_meta.next_table_id()),
            NotificationsTable::create(sys_db_meta.next_table_id()),
            NotificationHistoryTable::create(sys_db_meta.next_table_id()),
            ViewsTableWithHistory::create(sys_db_meta.next_table_id()),
            ViewsTableWithoutHistory::create(sys_db_meta.next_table_id()),
            TemporaryTablesTable::create(sys_db_meta.next_table_id()),
            ProceduresTable::create(sys_db_meta.next_table_id()),
            DictionariesTable::create(sys_db_meta.next_table_id()),
        ];

        let disable_tables = Self::disable_system_tables();
        for tbl in table_list.into_iter() {
            // Not load the disable system tables.
            if config.query.disable_system_table_load {
                let name = tbl.name();
                if !disable_tables.contains_key(name) {
                    sys_db_meta.insert("system", tbl);
                }
            } else {
                sys_db_meta.insert("system", tbl);
            }
        }

        let db_info = DatabaseInfo {
            database_id: DatabaseId::new(sys_db_meta.next_db_id()),
            name_ident: DatabaseNameIdent::new(Tenant::new_literal("dummy"), "system"),
            meta: SeqV::new(0, DatabaseMeta {
                engine: "SYSTEM".to_string(),
                ..Default::default()
            }),
        };

        Self { db_info }
    }
}

#[async_trait::async_trait]
impl Database for SystemDatabase {
    fn name(&self) -> &str {
        "system"
    }

    fn get_db_info(&self) -> &DatabaseInfo {
        &self.db_info
    }
}
