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

use std::sync::Arc;

use databend_common_catalog::plan::PushDownInfo;
use databend_common_catalog::table::Table;
use databend_common_catalog::table_context::TableContext;
use databend_common_cloud_control::pb::ScheduleOptions as CloudScheduleOptions;
use databend_common_cloud_control::pb::WarehouseOptions;
use databend_common_cloud_control::task_utils::Status as CloudTaskStatus;
use databend_common_cloud_control::task_utils::Task as CloudTask;
use databend_common_cloud_control::task_utils::format_schedule_options;
use databend_common_exception::Result;
use databend_common_expression::DataBlock;
use databend_common_expression::infer_table_schema;
use databend_common_meta_app::principal::Status;
use databend_common_meta_app::principal::Task;
use databend_common_meta_app::schema::TableIdent;
use databend_common_meta_app::schema::TableInfo;
use databend_common_meta_app::schema::TableMeta;
use databend_common_sql::plans::task_schema;
use databend_common_storages_system::AsyncOneBlockSystemTable;
use databend_common_storages_system::AsyncSystemTable;
use databend_common_users::UserApiProvider;
use itertools::Itertools;

use crate::system_tables::parse_tasks_to_datablock;

pub struct PrivateTasksTable {
    table_info: TableInfo,
}

#[async_trait::async_trait]
impl AsyncSystemTable for PrivateTasksTable {
    const NAME: &'static str = "system.tasks";

    fn get_table_info(&self) -> &TableInfo {
        &self.table_info
    }

    async fn get_full_data(
        &self,
        ctx: Arc<dyn TableContext>,
        push_downs: Option<PushDownInfo>,
    ) -> Result<DataBlock> {
        let tenant = ctx.get_tenant();
        let tasks = UserApiProvider::instance()
            .task_api(&tenant)
            .list_task()
            .await
            .map_err(|e| databend_common_exception::ErrorCode::MetaServiceError(e.to_string()))?;
        let tasks_len = tasks.len();

        let trans_tasks = tasks
            .into_iter()
            .take(
                push_downs
                    .as_ref()
                    .and_then(|v| v.limit)
                    .unwrap_or(tasks_len),
            )
            .map(private_task_to_cloud_task)
            .try_collect()?;

        parse_tasks_to_datablock(trans_tasks)
    }
}

impl PrivateTasksTable {
    pub fn create(table_id: u64) -> Arc<dyn Table> {
        let schema = infer_table_schema(&task_schema()).expect("failed to parse task table schema");

        let table_info = TableInfo {
            desc: "'system'.'tasks'".to_string(),
            name: "tasks".to_string(),
            ident: TableIdent::new(table_id, 0),
            meta: TableMeta {
                schema,
                engine: "SystemTasks".to_string(),
                ..Default::default()
            },
            ..Default::default()
        };

        AsyncOneBlockSystemTable::create(Self { table_info })
    }
}

pub fn private_task_to_cloud_task(task: Task) -> Result<CloudTask> {
    Ok(CloudTask {
        task_id: task.task_id,
        task_name: task.task_name,
        query_text: task.query_text,
        condition_text: task.when_condition.unwrap_or_default(),
        after: task.after,
        comment: task.comment,
        owner: task.owner,
        schedule_options: task
            .schedule_options
            .map(|schedule_options| {
                format_schedule_options(&CloudScheduleOptions {
                    interval: schedule_options.interval,
                    milliseconds_interval: schedule_options.milliseconds_interval,
                    cron: schedule_options.cron,
                    time_zone: schedule_options.time_zone,
                    schedule_type: schedule_options.schedule_type as i32,
                })
            })
            .transpose()?,
        warehouse_options: task
            .warehouse_options
            .map(|warehouse_options| WarehouseOptions {
                warehouse: warehouse_options.warehouse,
                using_warehouse_size: warehouse_options.using_warehouse_size,
            }),
        next_scheduled_at: task.next_scheduled_at,
        suspend_task_after_num_failures: task.suspend_task_after_num_failures.map(|i| i as i32),
        error_integration: task.error_integration,
        status: match task.status {
            Status::Suspended => CloudTaskStatus::Suspended,
            Status::Started => CloudTaskStatus::Started,
        },
        created_at: task.created_at,
        updated_at: task.updated_at,
        last_suspended_at: task.last_suspended_at,
        session_params: task.session_params,
    })
}
