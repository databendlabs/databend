use chrono::DateTime;
use chrono::Utc;
use common_config::GlobalConfig;
use common_exception::Result;
use common_meta_api::BackgroundApi;
use common_meta_app::background::BackgroundTaskInfo;
use common_meta_app::background::BackgroundTaskState;
use common_meta_app::background::BackgroundTaskType;
use common_meta_app::background::ListBackgroundTasksReq;
use common_users::UserApiProvider;
use log::debug;
use poem::web::Json;
use poem::web::Query;
use poem::IntoResponse;
use serde::Deserialize;
use serde::Serialize;
#[derive(Debug, Serialize, Deserialize)]
pub struct BackgroundTaskQuery {
    timestamp: DateTime<Utc>,
    table_id: u64,
    task_state: BackgroundTaskState,
    task_type: BackgroundTaskType,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct ListBackgroundTasksResponse {
    task_infos: Vec<(String, BackgroundTaskInfo)>,
    params: BackgroundTaskQuery,
}

#[async_backtrace::framed]
async fn load_background_tasks(
    tenant: &str,
    params: Query<BackgroundTaskQuery>,
) -> Result<ListBackgroundTasksResponse> {
    let meta_api = UserApiProvider::instance().get_meta_store_client();
    let tasks = meta_api
        .list_background_tasks(ListBackgroundTasksReq {
            tenant: tenant.to_string(),
        })
        .await?;
    let mut task_infos = Vec::with_capacity(tasks.len());
    for (_, name, task) in tasks {
        if task.task_state != params.task_state {
            continue;
        }
        if task.task_type != params.task_type {
            continue;
        }
        if task.task_type == BackgroundTaskType::COMPACTION {
            if task.compaction_task_stats.is_none() {
                continue;
            }
            if task.compaction_task_stats.as_ref().unwrap().table_id != params.table_id {
                continue;
            }
        }
        if task.last_updated.is_some() && task.last_updated.unwrap() < params.timestamp {
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
    params: Query<BackgroundTaskQuery>,
) -> poem::Result<impl IntoResponse> {
    let tenant = &GlobalConfig::instance().query.tenant_id;
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

    let resp = load_background_tasks(tenant, params)
        .await
        .map_err(poem::error::InternalServerError)?;
    Ok(Json(resp))
}
