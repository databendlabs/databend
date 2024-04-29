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
            state: process.state.to_string(),
            database: process.database.clone(),
            user: process
                .user
                .as_ref()
                .map(|u| u.identity().display().to_string())
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
