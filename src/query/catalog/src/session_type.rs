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

use std::fmt;

#[derive(Clone, PartialEq)]
pub enum SessionType {
    Clickhouse,
    MySQL,
    HTTPQuery,
    HTTPStreamingLoad,
    ClickHouseHttpHandler,
    FlightRPC,
    FlightSQL,
    HTTPAPI(String),
    Dummy,
    Fuzz,
    Local,
}

impl SessionType {
    pub fn is_user_session(&self) -> bool {
        !matches!(
            self,
            SessionType::HTTPAPI(_) | SessionType::Dummy | SessionType::Fuzz
        )
    }
}

impl fmt::Display for SessionType {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        let name = match self {
            SessionType::ClickHouseHttpHandler => "ClickhouseHTTPHandler".to_string(),
            SessionType::Clickhouse => "Clickhouse".to_string(),
            SessionType::MySQL => "MySQL".to_string(),
            SessionType::HTTPQuery => "HTTPQuery".to_string(),
            SessionType::HTTPStreamingLoad => "HTTPStreamingLoad".to_string(),
            SessionType::Dummy => "Dummy".to_string(),
            SessionType::FlightSQL => "FlightSQL".to_string(),
            SessionType::FlightRPC => "FlightRPC".to_string(),
            SessionType::HTTPAPI(usage) => format!("HTTPAPI({})", usage),
            SessionType::Fuzz => "Fuzz".to_string(),
            SessionType::Local => "Local".to_string(),
        };
        write!(f, "{}", name)
    }
}
