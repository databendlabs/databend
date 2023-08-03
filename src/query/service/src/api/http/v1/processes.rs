use std::time::SystemTime;

use poem::web::Json;
use poem::IntoResponse;
use serde::Deserialize;
use serde::Serialize;

use crate::sessions::SessionManager;

#[derive(Serialize, Deserialize, Eq, PartialEq, Debug)]
pub struct ProcessInfo {
    pub id: String,
    pub typ: String,
    pub state: String,
    pub database: String,
    pub user: String,
    pub client_address: String,
    pub session_extra_info: Option<String>,
    pub memory_usage: i64,
    pub mysql_connection_id: Option<u32>,
    pub created_time: SystemTime,
    pub status_info: Option<String>,
}

#[poem::handler]
#[async_backtrace::framed]
pub async fn processlist_handler() -> poem::Result<impl IntoResponse> {
    let session_manager = SessionManager::instance();
    let processes = session_manager
        .processes_info()
        .iter()
        .map(|process| ProcessInfo {
            id: process.id.clone(),
            typ: process.typ.clone(),
            state: process.state.clone(),
            database: process.database.clone(),
            user: process
                .user
                .as_ref()
                .map(|u| u.identity().to_string())
                .unwrap_or("".to_string()),
            client_address: process
                .client_address
                .as_ref()
                .map(|addr| addr.to_string())
                .unwrap_or("".to_string()),
            session_extra_info: process.session_extra_info.clone(),
            memory_usage: process.memory_usage,
            mysql_connection_id: process.mysql_connection_id,
            created_time: process.created_time,
            status_info: process.status_info.clone(),
        })
        .collect::<Vec<_>>();
    Ok(Json(processes))
}
