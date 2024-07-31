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

use serde::Deserialize;

use crate::request::SessionState;

#[derive(Deserialize, Debug)]
pub struct QueryError {
    pub code: u16,
    pub message: String,
    pub detail: Option<String>,
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
    // make it optional for backward compatibility
    pub total_scan: Option<ProgressValues>,
}

#[derive(Debug, Deserialize)]
pub struct ProgressValues {
    pub rows: usize,
    pub bytes: usize,
}

#[derive(Deserialize, Debug, Clone)]
pub struct SchemaField {
    pub name: String,
    #[serde(rename = "type")]
    pub data_type: String,
}

#[derive(Deserialize, Debug)]
pub struct QueryResponse {
    pub id: String,
    pub session_id: Option<String>,
    pub session: Option<SessionState>,
    pub schema: Vec<SchemaField>,
    pub data: Vec<Vec<Option<String>>>,
    pub state: String,
    pub error: Option<QueryError>,
    // make it optional for backward compatibility
    pub warnings: Option<Vec<String>>,
    pub stats: QueryStats,
    // pub affect: Option<QueryAffect>,
    pub stats_uri: Option<String>,
    pub final_uri: Option<String>,
    pub next_uri: Option<String>,
    pub kill_uri: Option<String>,
}

#[cfg(test)]
mod test {
    use std::collections::BTreeMap;

    use super::*;

    #[test]
    fn deserialize_session_config() {
        let session_json = r#"{"database":"default","settings":{}}"#;
        let session_config: SessionState = serde_json::from_str(session_json).unwrap();
        assert_eq!(session_config.database, Some("default".to_string()));
        assert_eq!(session_config.settings, Some(BTreeMap::default()));
        assert_eq!(session_config.role, None);
        assert_eq!(session_config.secondary_roles, None);

        let session_json = r#"{"database":"default","settings":{},"role": "role1", "secondary_roles": [], "unknown_field": 1}"#;
        let session_config: SessionState = serde_json::from_str(session_json).unwrap();
        assert_eq!(session_config.database, Some("default".to_string()));
        assert_eq!(session_config.settings, Some(BTreeMap::default()));
        assert_eq!(session_config.role, Some("role1".to_string()));
        assert_eq!(session_config.secondary_roles, Some(vec![]));
    }
}
