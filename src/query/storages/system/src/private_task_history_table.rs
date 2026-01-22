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

use crate::generate_default_catalog_meta;

pub struct PrivateTaskHistoryTable {}

impl PrivateTaskHistoryTable {
    // desc  system.task_history;
    // +------------------------------+---------------------+------+-----+---------------------------+--------+
    // | Field                        | Type                | Null | Key | Default                   | Extra  |
    // +------------------------------+---------------------+------+-----+---------------------------+--------+
    // | task_id                      | bigint unsigned     | YES  |     | NULL                      |        |
    // | task_name                    | text                | NO   |     | NULL                      |        |
    // | query_text                   | text                | NO   |     | NULL                      |        |
    // | when_condition               | text                | YES  |     | NULL                      |        |
    // | after                        | text                | YES  |     | NULL                      |        |
    // | comment                      | text                | YES  |     | NULL                      |        |
    // | owner                        | text                | YES  |     | NULL                      |        |
    // | owner_user                   | text                | YES  |     | NULL                      |        |
    // | warehouse_name               | text                | YES  |     | NULL                      |        |
    // | using_warehouse_size         | text                | YES  |     | NULL                      |        |
    // | schedule_type                | integer             | YES  |     | NULL                      |        |
    // | interval                     | integer             | YES  |     | NULL                      |        |
    // | interval_secs                | integer             | YES  |     | NULL                      |        |
    // | interval_milliseconds        | bigint unsigned     | YES  |     | NULL                      |        |
    // | cron                         | text                | YES  |     | NULL                      |        |
    // | time_zone                    | text                | YES  |     | 'UTC'                     |        |
    // | run_id                       | bigint unsigned     | YES  |     | NULL                      |        |
    // | attempt_number               | integer             | YES  |     | NULL                      |        |
    // | state                        | text                | NO   |     | 'SCHEDULED'               |        |
    // | error_code                   | bigint              | YES  |     | NULL                      |        |
    // | error_message                | text                | YES  |     | NULL                      |        |
    // | root_task_id                 | bigint unsigned     | YES  |     | NULL                      |        |
    // | scheduled_at                 | timestamp           | YES  |     | CURRENT_TIMESTAMP         |        |
    // | completed_at                 | timestamp           | YES  |     | NULL                      |        |
    // | next_scheduled_at            | timestamp           | YES  |     | CURRENT_TIMESTAMP         |        |
    // | error_integration            | text                | YES  |     | NULL                      |        |
    // | status                       | text                | YES  |     | NULL                      |        |
    // | created_at                   | timestamp           | YES  |     | NULL                      |        |
    // | updated_at                   | timestamp           | YES  |     | NULL                      |        |
    // | session_params               | variant             | YES  |     | NULL                      |        |
    // | last_suspended_at            | timestamp           | YES  |     | NULL                      |        |
    // | suspend_task_after_num_failures | integer         | YES  |     | NULL                      |        |
    // +------------------------------+---------------------+------+-----+---------------------------+--------+
    pub fn create(table_id: u64) -> Arc<dyn Table> {
        let query = "SELECT
        task_id,
        task_name,
        query_text,
        when_condition,
        after,
        comment,
        owner,
        owner_user,
        warehouse_name,
        using_warehouse_size,
        schedule_type,
        interval,
        interval_milliseconds,
        cron,
        time_zone,
        run_id,
        attempt_number,
        state,
        error_code,
        error_message,
        root_task_id,
        scheduled_at,
        completed_at,
        next_scheduled_at,
        error_integration,
        status,
        created_at,
        updated_at,
        session_params,
        last_suspended_at,
        suspend_task_after_num_failures
        FROM system_task.task_run ORDER BY run_id DESC;";

        let mut options = BTreeMap::new();
        options.insert(QUERY.to_string(), query.to_string());
        let table_info = TableInfo {
            desc: "'information_schema'.'task_history'".to_string(),
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
