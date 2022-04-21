//  Copyright 2022 Datafuse Labs.
//
//  Licensed under the Apache License, Version 2.0 (the "License");
//  you may not use this file except in compliance with the License.
//  You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
//  Unless required by applicable law or agreed to in writing, software
//  distributed under the License is distributed on an "AS IS" BASIS,
//  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
//  See the License for the specific language governing permissions and
//  limitations under the License.
//

use std::collections::BTreeMap;

use common_datavalues::chrono::DateTime;
use common_datavalues::chrono::Utc;
use common_exception::Result;
use serde::Deserialize;
use serde::Serialize;

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(tag = "version")]
pub enum VersionedCreateDatabase {
    #[serde(rename = "v1")]
    V1(CreateDatabaseV1),
}

// V1(please don't modify this struct).
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct CreateDatabaseV1 {
    pub tenant: String,
    pub database_name: String,
    pub engine_name: String,
    pub engine_options: BTreeMap<String, String>,
    pub comment: String,
    pub options: BTreeMap<String, String>,
    pub created_on: DateTime<Utc>,
}

// From Vec<u8> to V1.
impl TryFrom<Vec<u8>> for CreateDatabaseV1 {
    type Error = common_exception::ErrorCode;

    fn try_from(value: Vec<u8>) -> Result<Self> {
        let version = serde_json::from_slice::<VersionedCreateDatabase>(&value)?;
        match version {
            VersionedCreateDatabase::V1(v1) => Ok(v1),
        }
    }
}

// From V1 to Vec<u8>.
impl TryFrom<CreateDatabaseV1> for Vec<u8> {
    type Error = common_exception::ErrorCode;

    fn try_from(value: CreateDatabaseV1) -> Result<Vec<u8>> {
        Ok(serde_json::to_vec(&VersionedCreateDatabase::V1(value))?)
    }
}
