// Copyright 2023 Datafuse Labs.
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

use serde::Deserialize;

use crate::request::SessionConfig;

#[derive(Deserialize, Debug)]
pub struct QueryError {
    pub code: u16,
    pub message: String,
}

#[derive(Deserialize, Debug)]
pub struct QueryStats {
    #[serde(flatten)]
    pub progresses: Progresses,
    pub running_time_ms: f64,
}

#[derive(Deserialize, Debug)]
pub struct Progresses {
    pub scan_progress: ProgressValues,
    pub write_progress: ProgressValues,
    pub result_progress: ProgressValues,
}

#[derive(Debug, Deserialize)]
pub struct ProgressValues {
    pub rows: usize,
    pub bytes: usize,
}

#[derive(Deserialize, Debug)]
pub struct SchemaField {
    pub name: String,
    pub r#type: String,
}

#[derive(Deserialize, Debug)]
pub struct QueryResponse {
    pub id: String,
    pub session_id: Option<String>,
    pub session: Option<SessionConfig>,
    pub schema: Vec<SchemaField>,
    pub data: Vec<Vec<String>>,
    pub state: String,
    pub error: Option<QueryError>,
    pub stats: QueryStats,
    // pub affect: Option<QueryAffect>,
    pub stats_uri: Option<String>,
    pub final_uri: Option<String>,
    pub next_uri: Option<String>,
    pub kill_uri: Option<String>,
}
