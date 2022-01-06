//  Copyright 2021 Datafuse Labs.
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

use std::collections::HashMap;
use std::fmt::Display;
use std::fmt::Formatter;
use std::ops::Deref;

use uuid::Uuid;

#[derive(serde::Serialize, serde::Deserialize, Clone, Debug, Default, Eq, PartialEq)]
pub struct DatabaseNameIdent {
    pub tenant_id: Uuid,
    pub db_name: String,
}

#[derive(serde::Serialize, serde::Deserialize, Clone, Debug, Default, Eq, PartialEq)]
pub struct DatabaseInfo {
    // TODO(xp): store the seq AKA version for CAS update
    pub database_id: u64,
    pub db: String,
    pub meta: DatabaseMeta,
}

#[derive(serde::Serialize, serde::Deserialize, Clone, Debug, Default, Eq, PartialEq)]
pub struct DatabaseMeta {
    pub engine: String,
    pub engine_options: HashMap<String, String>,
    pub options: HashMap<String, String>,
}

impl Display for DatabaseMeta {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "Engine: {}={:?}, Options: {:?}",
            self.engine, self.engine_options, self.options
        )
    }
}

impl DatabaseInfo {
    pub fn engine(&self) -> &str {
        &self.meta.engine
    }
}

#[derive(serde::Serialize, serde::Deserialize, Clone, Debug, PartialEq)]
pub struct CreateDatabaseReq {
    pub if_not_exists: bool,
    pub tenant_id: Uuid,
    pub db: String,
    pub meta: DatabaseMeta,
}

#[derive(serde::Serialize, serde::Deserialize, Clone, Debug, Eq, PartialEq)]
pub struct CreateDatabaseReply {
    pub database_id: u64,
}

#[derive(serde::Serialize, serde::Deserialize, Clone, Debug, PartialEq)]
pub struct DropDatabaseReq {
    pub if_exists: bool,
    pub tenant_id: Uuid,
    pub db: String,
}

#[derive(serde::Serialize, serde::Deserialize, Clone, Debug, PartialEq)]
pub struct DropDatabaseReply {}

#[derive(serde::Serialize, serde::Deserialize, Clone, Debug, PartialEq)]
pub struct GetDatabaseReq {
    pub inner: DatabaseNameIdent,
}

impl Deref for GetDatabaseReq {
    type Target = DatabaseNameIdent;

    fn deref(&self) -> &Self::Target {
        &self.inner
    }
}

impl GetDatabaseReq {
    pub fn new(tenant_id: Uuid, db_name: impl Into<String>) -> GetDatabaseReq {
        GetDatabaseReq {
            inner: DatabaseNameIdent {
                tenant_id,
                db_name: db_name.into(),
            },
        }
    }
}

#[derive(serde::Serialize, serde::Deserialize, Clone, Debug, PartialEq)]
pub struct ListDatabaseReq {
    pub tenant_id: Uuid,
}
