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

use common_ast::ast::ScheduleOptions;
use common_ast::ast::WarehouseOptions;
use common_catalog::table_context::TableContext;
use common_cloud_control::client_config::ClientConfig;
use common_cloud_control::cloud_api::QUERY_ID;
use common_cloud_control::cloud_api::REQUESTER;
use common_cloud_control::cloud_api::TENANT_ID;
use common_cloud_control::pb::schedule_options::ScheduleType;
use common_cloud_control::pb::Task;
use common_exception::Result;
use common_expression::types::StringType;
use common_expression::types::TimestampType;
use common_expression::types::UInt64Type;
use common_expression::DataBlock;
use common_expression::FromData;
use common_expression::FromOptData;

use crate::sessions::QueryContext;

pub fn make_schedule_options(opt: ScheduleOptions) -> common_cloud_control::pb::ScheduleOptions {
    match opt {
        ScheduleOptions::IntervalMinutes(minute) => common_cloud_control::pb::ScheduleOptions {
            interval: Some(minute as i32),
            cron: None,
            time_zone: None,
            schedule_type: i32::from(ScheduleType::IntervalType),
        },
        ScheduleOptions::CronExpression(expr, timezone) => {
            common_cloud_control::pb::ScheduleOptions {
                interval: None,
                cron: Some(expr),
                time_zone: timezone,
                schedule_type: i32::from(ScheduleType::CronType),
            }
        }
    }
}

pub fn make_warehouse_options(opt: WarehouseOptions) -> common_cloud_control::pb::WarehouseOptions {
    let mut ret = common_cloud_control::pb::WarehouseOptions {
        warehouse: None,
        using_warehouse_size: None,
    };
    if let Some(warehouse) = opt.warehouse {
        ret.warehouse = Some(warehouse);
    }
    ret
}

pub fn get_client_config(ctx: Arc<QueryContext>) -> Result<ClientConfig> {
    let user = ctx.get_current_user()?;
    let mut config = ClientConfig::new();
    config.add_metadata(TENANT_ID, ctx.get_tenant());
    config.add_metadata(REQUESTER, user.identity().to_string());
    config.add_metadata(QUERY_ID, ctx.get_id());
    Ok(config)
}

pub fn parse_tasks_to_datablock(tasks: Vec<Task>) -> Result<DataBlock> {
    let mut created_on: Vec<i64> = Vec::with_capacity(tasks.len());
    let mut name: Vec<Vec<u8>> = Vec::with_capacity(tasks.len());
    let mut id: Vec<u64> = Vec::with_capacity(tasks.len());
    let mut owner: Vec<Vec<u8>> = Vec::with_capacity(tasks.len());
    let mut comment: Vec<Option<Vec<u8>>> = Vec::with_capacity(tasks.len());
    let mut warehouse: Vec<Option<Vec<u8>>> = Vec::with_capacity(tasks.len());
    let mut schedule: Vec<Option<Vec<u8>>> = Vec::with_capacity(tasks.len());
    let mut state: Vec<Vec<u8>> = Vec::with_capacity(tasks.len());
    let mut definition: Vec<Vec<u8>> = Vec::with_capacity(tasks.len());
    let mut last_committed_on: Vec<i64> = Vec::with_capacity(tasks.len());
    let mut next_schedule_time: Vec<Option<i64>> = Vec::with_capacity(tasks.len());

    for task in tasks {
        let tsk: common_cloud_control::task_utils::Task = task.try_into()?;
        created_on.push(tsk.created_at.timestamp_micros());
        name.push(tsk.task_name.into_bytes());
        id.push(tsk.task_id);
        owner.push(tsk.owner.into_bytes());
        comment.push(tsk.comment.map(|s| s.into_bytes()));
        warehouse.push(
            tsk.warehouse_options
                .and_then(|s| s.warehouse.map(|v| v.into_bytes())),
        );
        schedule.push(tsk.schedule_options.map(|s| s.into_bytes()));
        state.push(tsk.status.to_string().into_bytes());
        definition.push(tsk.query_text.into_bytes());
        next_schedule_time.push(Some(tsk.next_scheduled_at.timestamp_micros()));
        last_committed_on.push(tsk.updated_at.timestamp_micros());
    }
    Ok(DataBlock::new_from_columns(vec![
        TimestampType::from_data(created_on),
        StringType::from_data(name),
        UInt64Type::from_data(id),
        StringType::from_data(owner),
        StringType::from_opt_data(comment),
        StringType::from_opt_data(warehouse),
        StringType::from_opt_data(schedule),
        StringType::from_data(state),
        StringType::from_data(definition),
        TimestampType::from_opt_data(next_schedule_time),
        TimestampType::from_data(last_committed_on),
    ]))
}
