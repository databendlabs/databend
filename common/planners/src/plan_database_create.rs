// Copyright 2021 Datafuse Labs.
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
use std::fmt;
use std::sync::Arc;

use common_datavalues::chrono::DateTime;
use common_datavalues::chrono::Utc;
use common_datavalues::DataSchema;
use common_datavalues::DataSchemaRef;

#[derive(serde::Serialize, serde::Deserialize, Clone, Debug, Eq, PartialEq)]
pub struct DatabaseMeta {
    pub engine: String,
    pub engine_options: BTreeMap<String, String>,
    pub options: BTreeMap<String, String>,
    pub created_on: DateTime<Utc>,
    pub updated_on: DateTime<Utc>,
    pub comment: String,
}

impl Default for DatabaseMeta {
    fn default() -> Self {
        DatabaseMeta {
            engine: "".to_string(),
            engine_options: BTreeMap::new(),
            options: BTreeMap::new(),
            created_on: Utc::now(),
            updated_on: Utc::now(),
            comment: "".to_string(),
        }
    }
}

impl fmt::Display for DatabaseMeta {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "Engine: {}={:?}, Options: {:?}, CreatedOn: {:?}",
            self.engine, self.engine_options, self.options, self.created_on
        )
    }
}

#[derive(serde::Serialize, serde::Deserialize, Clone, Debug, PartialEq)]
pub struct CreateDatabasePlan {
    pub if_not_exists: bool,
    pub tenant: String,
    pub db_name: String,
    pub meta: DatabaseMeta,
}

impl CreateDatabasePlan {
    pub fn schema(&self) -> DataSchemaRef {
        Arc::new(DataSchema::empty())
    }
}
