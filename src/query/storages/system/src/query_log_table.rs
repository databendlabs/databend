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

use std::collections::HashMap;

use chrono::DateTime;
use serde::Serialize;
use serde::Serializer;
use serde_repr::Serialize_repr;

#[derive(Clone, Copy, Serialize_repr)]
#[repr(u8)]
pub enum LogType {
    Start = 1,
    Finish = 2,
    Error = 3,

    /// canceled by another thread:
    /// 1. `kill <query_id>` statement
    /// 2. client_driver/ctx.cancel() -> /kill
    Aborted = 4,

    /// close early because client does not need more data:
    /// 1. explicit or implicit result_set.close() -> /final
    Closed = 5,
}

impl LogType {
    pub fn as_string(&self) -> String {
        match self {
            LogType::Start => "Start".to_string(),
            LogType::Finish => "Finish".to_string(),
            LogType::Error => "Error".to_string(),
            LogType::Aborted => "Aborted".to_string(),
            LogType::Closed => "Closed".to_string(),
        }
    }
}

impl std::fmt::Debug for LogType {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        write!(f, "{}", self.as_string())
    }
}

fn date_str<S>(dt: &i32, s: S) -> std::result::Result<S::Ok, S::Error>
where S: Serializer {
    let t = DateTime::from_timestamp(i64::from(*dt) * 24 * 3600, 0)
        .unwrap()
        .with_timezone(&chrono::Local);
    s.serialize_str(t.format("%Y-%m-%d").to_string().as_str())
}

fn datetime_str<S>(dt: &i64, s: S) -> std::result::Result<S::Ok, S::Error>
where S: Serializer {
    let t = DateTime::from_timestamp(
        dt / 1_000_000,
        TryFrom::try_from((dt % 1_000_000) * 1000).unwrap_or(0),
    )
    .unwrap()
    .with_timezone(&chrono::Local);
    s.serialize_str(t.to_rfc3339().to_string().as_str())
}

#[derive(Clone, Serialize)]
pub struct QueryLogElement {
    // Type.
    pub log_type: LogType,
    pub log_type_name: String,
    pub handler_type: String,

    // User.
    pub tenant_id: String,
    pub cluster_id: String,
    pub node_id: String,
    pub sql_user: String,

    #[serde(skip_serializing_if = "String::is_empty")]
    pub sql_user_quota: String,
    #[serde(skip_serializing_if = "String::is_empty")]
    pub sql_user_privileges: String,

    // Query.
    pub query_id: String,
    pub query_kind: String,
    pub query_text: String,
    pub query_hash: String,
    pub query_parameterized_hash: String,

    #[serde(serialize_with = "date_str")]
    pub event_date: i32,
    #[serde(serialize_with = "datetime_str")]
    pub event_time: i64,
    #[serde(serialize_with = "datetime_str")]
    pub query_start_time: i64,
    pub query_duration_ms: i64,
    pub query_queued_duration_ms: i64,

    // Schema.
    pub current_database: String,
    pub databases: String,
    pub tables: String,
    pub columns: String,
    pub projections: String,

    // Stats.
    pub written_rows: u64,
    pub written_bytes: u64,
    pub written_io_bytes: u64,
    pub written_io_bytes_cost_ms: u64,
    pub scan_rows: u64,
    pub scan_bytes: u64,
    pub scan_io_bytes: u64,
    pub scan_io_bytes_cost_ms: u64,
    pub scan_partitions: u64,
    pub total_partitions: u64,
    pub result_rows: u64,
    pub result_bytes: u64,
    pub cpu_usage: u32,
    pub memory_usage: u64,
    pub join_spilled_bytes: u64,
    pub join_spilled_rows: u64,
    pub agg_spilled_bytes: u64,
    pub agg_spilled_rows: u64,
    pub group_by_spilled_bytes: u64,
    pub group_by_spilled_rows: u64,
    pub bytes_from_remote_disk: u64,
    pub bytes_from_local_disk: u64,
    pub bytes_from_memory: u64,

    // Client.
    pub client_info: String,
    pub client_address: String,
    pub user_agent: String,

    // Exception.
    pub exception_code: i32,
    pub exception_text: String,
    pub stack_trace: String,

    // Server.
    pub server_version: String,

    // Session
    pub query_tag: String,
    #[serde(skip_serializing_if = "String::is_empty")]
    pub session_settings: String,

    // Extra.
    pub extra: String,

    pub has_profiles: bool,

    // Transaction
    pub txn_state: String,
    pub txn_id: String,
    pub peek_memory_usage: HashMap<String, usize>,

    pub session_id: String,
}
