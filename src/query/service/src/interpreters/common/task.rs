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
use std::time::Duration;

use databend_common_ast::ast::ScheduleOptions;
use databend_common_catalog::table_context::TableContext;
use databend_common_cloud_control::client_config::build_client_config;
use databend_common_cloud_control::client_config::ClientConfig;
use databend_common_cloud_control::pb::schedule_options::ScheduleType;
use databend_common_exception::Result;

use crate::sessions::QueryContext;

pub fn make_schedule_options(
    opt: ScheduleOptions,
) -> databend_common_cloud_control::pb::ScheduleOptions {
    match opt {
        ScheduleOptions::IntervalSecs(secs, ms) => {
            databend_common_cloud_control::pb::ScheduleOptions {
                interval: Some(secs as i32),
                // none if ms is 0, else some ms
                milliseconds_interval: if ms == 0 { None } else { Some(ms) },
                cron: None,
                time_zone: None,
                schedule_type: i32::from(ScheduleType::IntervalType),
            }
        }

        ScheduleOptions::CronExpression(expr, timezone) => {
            databend_common_cloud_control::pb::ScheduleOptions {
                interval: None,
                milliseconds_interval: None,
                cron: Some(expr),
                time_zone: timezone,
                schedule_type: i32::from(ScheduleType::CronType),
            }
        }
    }
}

pub fn make_warehouse_options(
    opt: Option<String>,
) -> databend_common_cloud_control::pb::WarehouseOptions {
    databend_common_cloud_control::pb::WarehouseOptions {
        warehouse: opt,
        using_warehouse_size: None,
    }
}

pub fn get_task_client_config(ctx: Arc<QueryContext>, timeout: Duration) -> Result<ClientConfig> {
    let tenant = ctx.get_tenant();
    let user = ctx.get_current_user()?.identity().display().to_string();
    let query_id = ctx.get_id();
    let mut cfg = build_client_config(tenant.tenant_name().to_string(), user, query_id, timeout);
    cfg.add_task_version_info();
    Ok(cfg)
}
