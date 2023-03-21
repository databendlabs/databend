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

use std::collections::BTreeMap;

use serde::{Deserialize, Serialize};

#[derive(Deserialize, Serialize, Debug)]
pub struct SessionConfig {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub database: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub settings: Option<BTreeMap<String, String>>,
}

#[derive(Serialize, Debug)]
pub struct QueryRequest {
    #[serde(skip_serializing_if = "Option::is_none")]
    session: Option<SessionConfig>,
    sql: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pagination: Option<PaginationConfig>,
    #[serde(skip_serializing_if = "Option::is_none")]
    stage_attachment: Option<StageAttachmentConfig>,
}

#[derive(Serialize, Debug)]
pub struct PaginationConfig {
    #[serde(skip_serializing_if = "Option::is_none")]
    wait_time_secs: Option<i64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    max_rows_in_buffer: Option<i64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    max_rows_per_page: Option<i64>,
}

#[derive(Serialize, Debug)]
pub struct StageAttachmentConfig {
    #[serde(skip_serializing_if = "Option::is_none")]
    stage: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    stage_result_uri: Option<String>,
}

impl QueryRequest {
    pub fn new(sql: String) -> Self {
        QueryRequest {
            session: None,
            sql,
            pagination: None,
            stage_attachment: None,
        }
    }

    pub fn with_session(mut self, session: SessionConfig) -> Self {
        self.session = Some(session);
        self
    }

    pub fn with_pagination(mut self, pagination: PaginationConfig) -> Self {
        self.pagination = Some(pagination);
        self
    }

    pub fn with_stage_attachment(mut self, stage_attachment: StageAttachmentConfig) -> Self {
        self.stage_attachment = Some(stage_attachment);
        self
    }
}

#[cfg(test)]
mod test {
    use super::*;
    use anyhow::Result;

    #[test]
    fn test_build_request() -> Result<()> {
        let req = QueryRequest::new("select 1".to_string())
            .with_session(SessionConfig {
                database: Some("default".to_string()),
                settings: Some(BTreeMap::new()),
            })
            .with_pagination(PaginationConfig {
                wait_time_secs: Some(1),
                max_rows_in_buffer: Some(1),
                max_rows_per_page: Some(1),
            })
            .with_stage_attachment(StageAttachmentConfig {
                stage: Some("stage".to_string()),
                stage_result_uri: Some("stage_result_uri".to_string()),
            });
        assert_eq!(
            serde_json::to_string(&req)?,
            r#"{"session":{"database":"default","settings":{}},"sql":"select 1","pagination":{"wait_time_secs":1,"max_rows_in_buffer":1,"max_rows_per_page":1},"stage_attachment":{"stage":"stage","stage_result_uri":"stage_result_uri"}}"#
        );
        Ok(())
    }
}
