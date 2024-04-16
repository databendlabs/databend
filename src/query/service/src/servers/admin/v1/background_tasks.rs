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

use chrono::DateTime;
use chrono::Utc;
use databend_common_exception::Result;
use databend_common_meta_api::BackgroundApi;
use databend_common_meta_app::background::BackgroundTaskInfo;
use databend_common_meta_app::background::BackgroundTaskState;
use databend_common_meta_app::background::BackgroundTaskType;
use databend_common_meta_app::background::ListBackgroundTasksReq;
use databend_common_meta_app::tenant::Tenant;
use databend_common_users::UserApiProvider;
use log::debug;
use minitrace::func_name;
use poem::web::Json;
use poem::web::Path;
use poem::web::Query;
use poem::IntoResponse;
use serde::Deserialize;
use serde::Serialize;

#[derive(Debug, Serialize, Deserialize)]
pub struct BackgroundTaskQuery {
    timestamp: Option<DateTime<Utc>>,
    table_id: Option<u64>,
    task_state: Option<BackgroundTaskState>,
    task_type: Option<BackgroundTaskType>,
}

/// Required to be json serde by: [`list_background_tasks`]
#[derive(Debug, Serialize, Deserialize)]
pub struct ListBackgroundTasksResponse {
    task_infos: Vec<(String, BackgroundTaskInfo)>,
    params: BackgroundTaskQuery,
}

#[async_backtrace::framed]
async fn load_background_tasks(
    tenant: &Tenant,
    params: Query<BackgroundTaskQuery>,
) -> Result<ListBackgroundTasksResponse> {
    let meta_api = UserApiProvider::instance().get_meta_store_client();
    let tasks = meta_api
        .list_background_tasks(ListBackgroundTasksReq::new(tenant))
        .await?;
    debug!(
        "list_background_tasks: tenant: {}, candidate tasks: {}",
        tenant.tenant_name(),
        tasks.len()
    );
    let mut task_infos = Vec::with_capacity(tasks.len());
    for (_, name, task) in tasks {
        if params.task_state.is_some() && task.task_state != *params.task_state.as_ref().unwrap() {
            continue;
        }
        if params.task_type.is_some() && task.task_type != *params.task_type.as_ref().unwrap() {
            continue;
        }
        if task.task_type == BackgroundTaskType::COMPACTION {
            if task.compaction_task_stats.is_none() {
                continue;
            }
            if params.table_id.is_some()
                && task.compaction_task_stats.as_ref().unwrap().table_id != params.table_id.unwrap()
            {
                continue;
            }
        }
        if params.timestamp.as_ref().is_some()
            && task.last_updated.is_some()
            && task.last_updated.unwrap() < params.timestamp.unwrap()
        {
            continue;
        }
        task_infos.push((name, task));
    }
    Ok(ListBackgroundTasksResponse {
        task_infos,
        params: params.0,
    })
}

#[poem::handler]
#[async_backtrace::framed]
pub async fn list_background_tasks(
    Path(tenant): Path<String>,
    params: Query<BackgroundTaskQuery>,
) -> poem::Result<impl IntoResponse> {
    debug!(
        "list_background_tasks: tenant: {}, params: {:?}",
        tenant, params
    );
    if tenant.is_empty() {
        return Ok(Json(ListBackgroundTasksResponse {
            task_infos: vec![],
            params: params.0,
        }));
    }

    // Safe unwrap: tenant is not empty
    let tenant = Tenant::new_or_err(tenant, func_name!()).unwrap();

    let resp = load_background_tasks(&tenant, params)
        .await
        .map_err(poem::error::InternalServerError)?;
    Ok(Json(resp))
}
