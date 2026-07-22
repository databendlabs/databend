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

use std::collections::BTreeMap;
use std::sync::Arc;

use databend_common_catalog::catalog::CATALOG_DEFAULT;
use databend_common_catalog::table::Table;
use databend_common_meta_app::schema::CatalogInfo;
use databend_common_meta_app::schema::CatalogNameIdent;
use databend_common_meta_app::schema::TableIdent;
use databend_common_meta_app::schema::TableInfo;
use databend_common_meta_app::schema::TableMeta;
use databend_common_meta_app::tenant::Tenant;
use databend_common_storages_basic::view_table::QUERY;
use databend_common_storages_basic::view_table::ViewTable;
use databend_common_storages_system::generate_default_catalog_meta;

pub struct PrivateTaskHistoryTable;

impl PrivateTaskHistoryTable {
    pub fn create(table_id: u64) -> Arc<dyn Table> {
        // Keep the private-task system table compatible with the cloud task-history
        // contract. The underlying task_run table is an implementation detail and
        // may grow independently without changing this public schema.
        let query = "SELECT
        COALESCE(task_name, '') AS name,
        COALESCE(task_id, 0) AS id,
        COALESCE(owner, '') AS owner,
        comment,
        CASE
            WHEN schedule_type = 0 AND interval_milliseconds IS NOT NULL THEN CONCAT('INTERVAL ', COALESCE(CAST(interval AS STRING), '0'), ' SECOND ', CAST(interval_milliseconds AS STRING), ' MILLISECOND')
            WHEN schedule_type = 0 THEN CONCAT('INTERVAL ', COALESCE(CAST(interval AS STRING), '0'), ' SECOND')
            WHEN schedule_type = 1 AND time_zone IS NOT NULL THEN CONCAT('CRON ', cron, ' TIMEZONE ', time_zone)
            WHEN schedule_type = 1 THEN CONCAT('CRON ', cron)
            ELSE NULL
        END AS schedule,
        warehouse_name AS warehouse,
        COALESCE(state, '') AS state,
        COALESCE(query_text, '') AS definition,
        COALESCE(when_condition, '') AS condition_text,
        CAST(COALESCE(run_id, 0) AS STRING) AS run_id,
        '' AS query_id,
        COALESCE(error_code, 0) AS exception_code,
        error_message AS exception_text,
        COALESCE(attempt_number, 0) AS attempt_number,
        completed_at AS completed_time,
        COALESCE(scheduled_at, TO_TIMESTAMP(0)) AS scheduled_time,
        CAST(COALESCE(root_task_id, 0) AS STRING) AS root_task_id,
        session_params AS session_parameters
        FROM system_task.task_run ORDER BY run_id DESC;";

        let mut options = BTreeMap::new();
        options.insert(QUERY.to_string(), query.to_string());
        let table_info = TableInfo {
            desc: "'system'.'task_history'".to_string(),
            name: "task_history".to_string(),
            ident: TableIdent::new(table_id, 0),
            meta: TableMeta {
                options,
                engine: "VIEW".to_string(),
                ..Default::default()
            },
            catalog_info: Arc::new(CatalogInfo {
                name_ident: CatalogNameIdent::new(Tenant::new_literal("dummy"), CATALOG_DEFAULT)
                    .into(),
                meta: generate_default_catalog_meta(),
                ..Default::default()
            }),
            ..Default::default()
        };

        ViewTable::create(table_info)
    }
}
