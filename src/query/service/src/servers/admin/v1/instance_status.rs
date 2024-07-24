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

use crate::sessions::QueriesQueueManager;
use crate::sessions::SessionManager;

#[derive(Serialize, Deserialize, Eq, PartialEq, Debug)]
pub struct InstanceStatus {
    // the active sessions count with running queries
    pub running_queries_count: u64,
    // the length of query queue
    pub queuing_queries_count: u64,
    // the active sessions count, have active connections, but may not have any query running
    pub active_sessions_count: u64,
    // the timestamp on last query started
    pub last_query_started_at: Option<u64>,
    // the timestamp on last query finished
    pub last_query_finished_at: Option<u64>,
    // the timestamp when the instance started up
    pub instance_started_at: u64,
    // the local timestamp, may be useful to avoid the clock drift issues
    pub instance_timestamp: u64,
}

// lightweight way to get status
// return Status in json
#[poem::handler]
#[async_backtrace::framed]
pub async fn instance_status_handler() -> poem::Result<impl IntoResponse> {
    let session_manager = SessionManager::instance();
    let queue_manager = QueriesQueueManager::instance();
    let status = session_manager.get_current_session_status();
    let status = InstanceStatus {
        running_queries_count: status.running_queries_count,
        active_sessions_count: status.active_sessions_count,
        queuing_queries_count: queue_manager.length() as u64,
        last_query_started_at: status.last_query_started_at.map(unix_timestamp_secs),
        last_query_finished_at: status.last_query_finished_at.map(unix_timestamp_secs),
        instance_started_at: unix_timestamp_secs(status.instance_started_at),
        instance_timestamp: unix_timestamp_secs(SystemTime::now()),
    };
    Ok(Json(status))
}

// Convert SystemTime to an u64 timestamp. The clock is almost impossible to drift before
// 1970, so we can safely use the expect() here.
fn unix_timestamp_secs(t: SystemTime) -> u64 {
    t.duration_since(SystemTime::UNIX_EPOCH)
        .expect("Time went backwards")
        .as_secs()
}
