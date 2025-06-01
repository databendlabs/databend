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

use chrono::DateTime;
use log::info;
use serde::Serialize;
use serde::Serializer;

use crate::auth::CredentialType;
use crate::sessions::convert_query_log_timestamp;

#[derive(Serialize)]
pub enum LoginEventType {
    LoginSuccess,
    LoginFailed,
}

#[derive(Serialize)]
pub enum LoginHandler {
    #[allow(clippy::upper_case_acronyms)]
    HTTP,
    MySQL,
}

#[derive(Serialize)]
pub struct LoginHistory {
    #[serde(serialize_with = "datetime_str")]
    pub event_time: i64,
    pub handler: LoginHandler,
    pub event_type: LoginEventType,
    pub connection_uri: String,
    pub auth_type: CredentialType,
    pub user_name: String,
    pub client_ip: String,
    pub user_agent: String,
    pub session_id: String,
    pub node_id: String,
    pub error_message: String,
}

impl LoginHistory {
    pub fn new() -> Self {
        LoginHistory::default()
    }

    pub fn write_to_log(&self) {
        let event_str = serde_json::to_string(&self).unwrap();
        info!(target: "databend::log::login", "{}", event_str);
    }
}
impl Default for LoginHistory {
    fn default() -> Self {
        Self {
            event_type: LoginEventType::LoginSuccess,
            auth_type: CredentialType::NoNeed,
            event_time: convert_query_log_timestamp(SystemTime::now()),
            handler: LoginHandler::HTTP,
            connection_uri: "".to_string(),
            user_name: "".to_string(),
            client_ip: "".to_string(),
            user_agent: "".to_string(),
            session_id: "".to_string(),
            node_id: "".to_string(),
            error_message: "".to_string(),
        }
    }
}

fn datetime_str<S>(dt: &i64, s: S) -> std::result::Result<S::Ok, S::Error>
where S: Serializer {
    let t = DateTime::from_timestamp(
        dt / 1_000_000,
        TryFrom::try_from((dt % 1_000_000) * 1000).unwrap_or(0),
    )
    .unwrap()
    .naive_utc();
    s.serialize_str(t.format("%Y-%m-%d %H:%M:%S%.6f").to_string().as_str())
}
