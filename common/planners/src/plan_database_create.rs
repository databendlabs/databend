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

use std::collections::HashMap;
use std::sync::Arc;

use common_datavalues::DataSchema;
use common_datavalues::DataSchemaRef;
use common_meta_types::CreateDatabaseReq;

pub type DatabaseOptions = HashMap<String, String>;

#[derive(serde::Serialize, serde::Deserialize, Clone, Debug, PartialEq)]
pub struct CreateDatabasePlan {
    pub if_not_exists: bool,
    pub db: String,
    pub engine: String,
    pub options: DatabaseOptions,
}

impl From<CreateDatabasePlan> for CreateDatabaseReq {
    fn from(p: CreateDatabasePlan) -> Self {
        CreateDatabaseReq {
            if_not_exists: p.if_not_exists,
            db: p.db.clone(),
            engine: p.engine.to_string(),
            options: p.options,
        }
    }
}

impl From<&CreateDatabasePlan> for CreateDatabaseReq {
    fn from(p: &CreateDatabasePlan) -> Self {
        CreateDatabaseReq {
            if_not_exists: p.if_not_exists,
            db: p.db.clone(),
            engine: p.engine.clone(),
            options: p.options.clone(),
        }
    }
}

impl CreateDatabasePlan {
    pub fn schema(&self) -> DataSchemaRef {
        Arc::new(DataSchema::empty())
    }
}
