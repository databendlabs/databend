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

use std::collections::HashSet;
use std::time::Duration;

use databend_common_meta_app::principal::UserInfo;
use poem::web::Json;
use poem::IntoResponse;
use serde::Deserialize;
use serde::Serialize;

use crate::sessions::QueriesQueueManager;
use crate::sessions::SessionManager;

#[derive(Serialize, Deserialize, Eq, PartialEq, Debug)]
pub struct QueuedQuery {
    pub id: String,
    pub typ: String,
    pub user: String,
    pub client_address: String,
    pub wait_time: Duration,
    pub mysql_connection_id: Option<u32>,
}

#[poem::handler]
#[async_backtrace::framed]
pub async fn queries_queue_handler() -> poem::Result<impl IntoResponse> {
    let session_manager = SessionManager::instance();
    let queries_queue_manager = QueriesQueueManager::instance();

    let queries = queries_queue_manager
        .list()
        .iter()
        .map(|x| x.query_id.clone())
        .collect::<HashSet<_>>();

    let processes = session_manager
        .processes_info()
        .into_iter()
        .filter(|x| match &x.current_query_id {
            None => false,
            Some(query_id) => queries.contains(query_id),
        })
        .map(|process| {
            let wait_time = process
                .created_time
                .elapsed()
                .unwrap_or(Duration::from_secs(0));
            QueuedQuery {
                wait_time,
                id: process.id.clone(),
                typ: process.typ.clone(),
                user: user_identity(&process.user),
                client_address: client_address(&process.client_address),
                mysql_connection_id: process.mysql_connection_id,
            }
        })
        .collect::<Vec<_>>();
    Ok(Json(processes))
}

fn user_identity(user: &Option<UserInfo>) -> String {
    user.as_ref()
        .map(|u| u.identity().display().to_string())
        .unwrap_or("".to_string())
}

fn client_address(address: &Option<String>) -> String {
    address
        .as_ref()
        .map(|addr| addr.to_string())
        .unwrap_or("".to_string())
}
