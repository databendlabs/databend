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
use databend_common_meta_app::principal::StageType;
use serde::Serialize;
use serde::Serializer;
use serde_json::Value;

#[derive(Debug, Serialize)]
pub struct AccessLogEntry {
    pub query_id: String,
    #[serde(serialize_with = "datetime_str")]
    pub query_start: i64,
    pub user_name: String,
    pub base_objects_accessed: Vec<AccessObject>,
    pub direct_objects_accessed: Vec<AccessObject>,
    pub objects_modified: Vec<AccessObject>,
    pub object_modified_by_ddl: Vec<ModifyByDDLObject>,
}

impl AccessLogEntry {
    pub fn is_empty(&self) -> bool {
        self.base_objects_accessed.is_empty()
            && self.direct_objects_accessed.is_empty()
            && self.objects_modified.is_empty()
            && self.object_modified_by_ddl.is_empty()
    }
}

#[derive(Debug, Serialize, Default)]
pub enum ObjectDomain {
    #[default]
    Table,
    Stage,
    Database,
}

#[derive(Debug, Serialize)]
pub struct AccessObjectColumn {
    pub column_name: String,
}

#[derive(Debug, Serialize, Default)]
pub struct AccessObject {
    pub object_domain: ObjectDomain,
    pub object_name: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub columns: Option<Vec<AccessObjectColumn>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub stage_type: Option<StageType>,
}

#[derive(Debug, Serialize, Default)]
pub enum DDLOperationType {
    #[default]
    Create,
    Drop,
    Alter,
    Undrop,
}

#[derive(Debug, Serialize, Default)]
pub struct ModifyByDDLObject {
    pub object_domain: ObjectDomain,
    pub object_name: String,
    pub operation_type: DDLOperationType,
    pub properties: HashMap<String, Value>,
}

impl AccessLogEntry {
    pub fn create(query_id: String, query_start: i64, user_name: String) -> Self {
        Self {
            query_id,
            query_start,
            user_name,
            base_objects_accessed: vec![],
            direct_objects_accessed: vec![],
            objects_modified: vec![],
            object_modified_by_ddl: vec![],
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
