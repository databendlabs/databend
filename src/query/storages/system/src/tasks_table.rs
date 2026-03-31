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
use databend_common_cloud_control::client_config::build_client_config;
use databend_common_cloud_control::client_config::make_request;
use databend_common_cloud_control::cloud_api::CloudControlApiProvider;
use databend_common_cloud_control::pb::ShowTasksRequest;
use databend_common_cloud_control::task_utils::Task;
use databend_common_config::GlobalConfig;
use databend_common_exception::ErrorCode;
use databend_common_exception::Result;
use databend_common_expression::DataBlock;
use databend_common_expression::FromData;
use databend_common_expression::infer_table_schema;
use databend_common_expression::types::StringType;
use databend_common_expression::types::TimestampType;
use databend_common_expression::types::UInt64Type;
use databend_common_expression::types::VariantType;
use databend_common_meta_app::schema::TableIdent;
use databend_common_meta_app::schema::TableInfo;
use databend_common_meta_app::schema::TableMeta;
use databend_common_sql::plans::task_schema;
use itertools::Itertools;

use crate::table::AsyncOneBlockSystemTable;
use crate::table::AsyncSystemTable;

pub fn parse_tasks_to_datablock(tasks: Vec<Task>) -> Result<DataBlock> {
    let mut created_on: Vec<i64> = Vec::with_capacity(tasks.len());
    let mut name: Vec<String> = Vec::with_capacity(tasks.len());
    let mut id: Vec<u64> = Vec::with_capacity(tasks.len());
    let mut owner: Vec<String> = Vec::with_capacity(tasks.len());
    let mut comment: Vec<Option<String>> = Vec::with_capacity(tasks.len());
    let mut warehouse: Vec<Option<String>> = Vec::with_capacity(tasks.len());
    let mut schedule: Vec<Option<String>> = Vec::with_capacity(tasks.len());
    let mut status: Vec<String> = Vec::with_capacity(tasks.len());
    let mut definition: Vec<String> = Vec::with_capacity(tasks.len());
    let mut condition_text: Vec<String> = Vec::with_capacity(tasks.len());
    let mut after: Vec<String> = Vec::with_capacity(tasks.len());
    let mut suspend_after_num_failures: Vec<Option<u64>> = Vec::with_capacity(tasks.len());
    let mut error_integration: Vec<Option<String>> = Vec::with_capacity(tasks.len());
    let mut last_committed_on: Vec<i64> = Vec::with_capacity(tasks.len());
    let mut next_schedule_time: Vec<Option<i64>> = Vec::with_capacity(tasks.len());
    let mut last_suspended_on: Vec<Option<i64>> = Vec::with_capacity(tasks.len());
    let mut session_params: Vec<Option<Vec<u8>>> = Vec::with_capacity(tasks.len());
    for task in tasks {
        created_on.push(task.created_at.timestamp_micros());
        name.push(task.task_name);
        id.push(task.task_id);
        owner.push(task.owner);
        comment.push(task.comment);
        warehouse.push(task.warehouse_options.and_then(|s| s.warehouse));
        schedule.push(task.schedule_options);
        status.push(task.status.to_string());
        definition.push(task.query_text);
        condition_text.push(task.condition_text);
        // join by comma
        after.push(task.after.into_iter().collect::<Vec<_>>().join(","));
        suspend_after_num_failures.push(task.suspend_task_after_num_failures.map(|v| v as u64));
        error_integration.push(task.error_integration);
        next_schedule_time.push(task.next_scheduled_at.map(|t| t.timestamp_micros()));
        last_committed_on.push(task.updated_at.timestamp_micros());
        last_suspended_on.push(task.last_suspended_at.map(|t| t.timestamp_micros()));
        let serialized_params = serde_json::to_vec(&task.session_params).unwrap();
        session_params.push(Some(serialized_params));
    }

    Ok(DataBlock::new_from_columns(vec![
        TimestampType::from_data(created_on),
        StringType::from_data(name),
        UInt64Type::from_data(id),
        StringType::from_data(owner),
        StringType::from_opt_data(comment),
        StringType::from_opt_data(warehouse),
        StringType::from_opt_data(schedule),
        StringType::from_data(status),
        StringType::from_data(definition),
        StringType::from_data(condition_text),
        StringType::from_data(after),
        UInt64Type::from_opt_data(suspend_after_num_failures),
        StringType::from_opt_data(error_integration),
        TimestampType::from_opt_data(next_schedule_time),
        TimestampType::from_data(last_committed_on),
        TimestampType::from_opt_data(last_suspended_on),
        VariantType::from_opt_data(session_params),
    ]))
}

pub struct TasksTable {
    table_info: TableInfo,
}

#[async_trait::async_trait]
impl AsyncSystemTable for TasksTable {
    const NAME: &'static str = "system.tasks";

    fn get_table_info(&self) -> &TableInfo {
        &self.table_info
    }

    #[async_backtrace::framed]
    async fn get_full_data(
        &self,
        ctx: Arc<dyn TableContext>,
        _push_downs: Option<PushDownInfo>,
    ) -> Result<DataBlock> {
        let config = GlobalConfig::instance();
        if config
            .query
            .common
            .cloud_control_grpc_server_address
            .is_none()
        {
            return Err(ErrorCode::CloudControlNotEnabled(
                "cannot view system.tasks table without cloud control enabled, please set cloud_control_grpc_server_address in config",
            ));
        }

        let tenant = ctx.get_tenant();
        let query_id = ctx.get_id();
        let user = ctx.get_current_user()?.identity().display().to_string();
        let available_roles = ctx.get_all_available_roles().await?;
        let req = ShowTasksRequest {
            tenant_id: tenant.tenant_name().to_string(),
            name_like: "".to_string(),
            result_limit: 10000, // TODO: use plan.limit pushdown
            owners: available_roles
                .into_iter()
                .map(|x| x.identity().to_string())
                .collect(),
            task_ids: vec![],
        };

        let cloud_api = CloudControlApiProvider::instance();
        let task_client = cloud_api.get_task_client();
        let cfg = build_client_config(
            tenant.tenant_name().to_string(),
            user,
            query_id,
            cloud_api.get_timeout(),
        );
        let req = make_request(req, cfg);

        let resp = task_client.show_tasks(req).await?;
        let tasks = resp.tasks;

        parse_tasks_to_datablock(tasks.into_iter().map(Task::try_from).try_collect()?)
    }
}

impl TasksTable {
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
